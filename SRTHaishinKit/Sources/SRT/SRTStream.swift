@preconcurrency import AVFoundation
import Combine
import Foundation
import HaishinKit
import libsrt

/// An actor that provides the interface to control a one-way channel over a SRTConnection.
public actor SRTStream {
    static let supportedAudioCodecs: [AudioCodecSettings.Format] = [.aac]
    static let supportedVideoCodecs: [VideoCodecSettings.Format] = VideoCodecSettings.Format.allCases

    /// The expected medias for transport stream.
    public var expectedMedias: Set<AVMediaType> {
        writer.expectedMedias
    }

    @Published public private(set) var readyState: StreamReadyState = .idle
    public private(set) var videoTrackId: UInt8? = UInt8.max
    public private(set) var audioTrackId: UInt8? = UInt8.max
    package var outputs: [any StreamOutput] = []
    package var bitRateStrategy: (any StreamBitRateStrategy)?
    private lazy var writer = TSWriter()
    // nonisolated(unsafe): reader is accessed from nonisolated doInput()
    // (called serially from SRTConnection.recv) and from actor-isolated
    // play()/close(). Serial access is guaranteed by readerEnabled flag.
    nonisolated(unsafe) private lazy var reader = TSReader()
    nonisolated(unsafe) private var readerEnabled = true
    package lazy var incoming = IncomingStream(self)
    package lazy var outgoing = OutgoingStream()
    private weak var connection: SRTConnection?
    nonisolated(unsafe) private var mixerAudioContinuation: AsyncStream<(AVAudioPCMBuffer, AVAudioTime)>.Continuation?
    nonisolated(unsafe) private var mixerVideoContinuation: AsyncStream<CMSampleBuffer>.Continuation?

    /// The error domain codes.
    public enum Error: Swift.Error {
        // An unsupported codec.
        case unsupportedCodec
    }

    /// Creates a new stream object.
    public init(connection: SRTConnection) {
        self.connection = connection
        Task {
            await self.startMixerInputConsumers()
            await connection.addStream(self)
        }
    }

    deinit {
        mixerAudioContinuation?.finish()
        mixerVideoContinuation?.finish()
        outputs.removeAll()
    }

    /// Sends streaming audio and video from client.
    ///
    /// - Warning: As a prerequisite, SRTConnection must be connected. In the future, an exception will be thrown.
    public func publish(_ name: String? = "") async {
        guard let connection, await connection.connected else {
            return
        }
        guard name != nil else {
            switch readyState {
            case .publishing:
                await close()
            default:
                break
            }
            return
        }
        readyState = .publishing
        stopMixerInputConsumers()
        startMixerInputConsumers()
        outgoing.startRunning()
        if outgoing.videoInputFormat != nil {
            writer.expectedMedias.insert(.video)
        }
        if outgoing.audioInputFormat != nil {
            writer.expectedMedias.insert(.audio)
        }
        if writer.expectedMedias.isEmpty {
            logger.error("Please set expected media.")
        }
        Task {
            for await buffer in outgoing.videoOutputStream {
                append(buffer)
            }
        }
        Task {
            for await buffer in outgoing.audioOutputStream {
                append(buffer.0, when: buffer.1)
            }
        }
        Task {
            for await buffer in outgoing.videoInputStream {
                outgoing.append(video: buffer)
            }
        }
        Task {
            for await data in writer.output {
                await connection.send(data)
            }
        }
    }

    /// Playback streaming audio and video from server.
    ///
    /// - Warning: As a prerequisite, SRTConnection must be connected. In the future, an exception will be thrown.
    public func play(_ name: String? = "") async {
        guard let connection, await connection.connected else {
            return
        }
        guard name != nil else {
            switch readyState {
            case .playing:
                await close()
            default:
                break
            }
            return
        }
        // Re-enable reader for this connection (may have been disabled by close()).
        readerEnabled = true
        // Capture reader output BEFORE starting recv to ensure TSReader
        // continuation is set when doInput calls arrive.
        let readerOutput = reader.output
        let incomingRef = self.incoming
        await connection.recv()
        // Consumer runs on a detached Task (OFF the SRTStream actor)
        // so it doesn't compete with doInput for actor scheduling time.
        // Backlog is bounded by bufferingNewest on TSReader output (30),
        // VideoCodec output (2), and AudioCodec output (8) — no PTS-based
        // dropping needed here. Maximum display delay ≈ 2 decoded frames.
        Task.detached {
            await incomingRef.startRunning()
            for await buffer in readerOutput {
                await incomingRef.append(buffer.1)
            }
        }
        readyState = .playing
    }

    /// Stops playing or publishing and makes available other uses.
    public func close() async {
        guard readyState != .idle else {
            return
        }
        // Disable nonisolated doInput before clearing reader state.
        readerEnabled = false
        stopMixerInputConsumers()
        startMixerInputConsumers()
        writer.clear()
        reader.clear()
        outgoing.stopRunning()
        await incoming.stopRunning()
        readyState = .idle
    }

    /// Sets the expected media.
    ///
    /// This sets whether the stream contains audio only, video only, or both. Normally, this is automatically set through the append method.
    /// If you cannot call the append method before publishing, please use this method to explicitly specify the contents of the stream.
    public func setExpectedMedias(_ expectedMedias: Set<AVMediaType>) {
        writer.expectedMedias = expectedMedias
    }

    // nonisolated: runs on SRTConnection's context without hopping to
    // SRTStream actor. Eliminates ~500 actor hops/sec per source.
    nonisolated func doInput(_ data: Data) {
        guard readerEnabled else { return }
        _ = reader.read(data)
    }

    private func startMixerInputConsumers() {
        let (audioStream, audioContinuation) = AsyncStream.makeStream(of: (AVAudioPCMBuffer, AVAudioTime).self)
        let (videoStream, videoContinuation) = AsyncStream.makeStream(of: CMSampleBuffer.self)
        mixerAudioContinuation = audioContinuation
        mixerVideoContinuation = videoContinuation
        Task {
            for await (buffer, when) in audioStream {
                append(buffer, when: when)
            }
        }
        Task {
            for await sampleBuffer in videoStream {
                append(sampleBuffer)
            }
        }
    }

    private func stopMixerInputConsumers() {
        mixerAudioContinuation?.finish()
        mixerAudioContinuation = nil
        mixerVideoContinuation?.finish()
        mixerVideoContinuation = nil
    }
}

extension SRTStream: _Stream {
    public func setAudioSettings(_ audioSettings: AudioCodecSettings) throws {
        guard Self.supportedAudioCodecs.contains(audioSettings.format) else {
            throw Error.unsupportedCodec
        }
        outgoing.audioSettings = audioSettings
    }

    public func setVideoSettings(_ videoSettings: VideoCodecSettings) throws {
        guard Self.supportedVideoCodecs.contains(videoSettings.format) else {
            throw Error.unsupportedCodec
        }
        outgoing.videoSettings = videoSettings
    }

    public func append(_ sampleBuffer: CMSampleBuffer) {
        switch sampleBuffer.formatDescription?.mediaType {
        case .video:
            if sampleBuffer.formatDescription?.isCompressed == true {
                writer.videoFormat = sampleBuffer.formatDescription
                writer.append(sampleBuffer)
            } else {
                outgoing.append(sampleBuffer)
                outputs.forEach { $0.stream(self, didOutput: sampleBuffer) }
            }
        default:
            break
        }
    }

    public func append(_ audioBuffer: AVAudioBuffer, when: AVAudioTime) {
        switch audioBuffer {
        case let audioBuffer as AVAudioPCMBuffer:
            outgoing.append(audioBuffer, when: when)
            outputs.forEach { $0.stream(self, didOutput: audioBuffer, when: when) }
        case let audioBuffer as AVAudioCompressedBuffer:
            writer.audioFormat = audioBuffer.format
            writer.append(audioBuffer, when: when)
        default:
            break
        }
    }

    public func dispatch(_ event: NetworkMonitorEvent) async {
        await bitRateStrategy?.adjustBitrate(event, stream: self)
    }
}

extension SRTStream: MediaMixerOutput {
    // MARK: MediaMixerOutput
    public func selectTrack(_ id: UInt8?, mediaType: CMFormatDescription.MediaType) {
        switch mediaType {
        case .audio:
            audioTrackId = id
        case .video:
            videoTrackId = id
        default:
            break
        }
    }

    nonisolated public func mixer(_ mixer: MediaMixer, didOutput sampleBuffer: CMSampleBuffer) {
        mixerVideoContinuation?.yield(sampleBuffer)
    }

    nonisolated public func mixer(_ mixer: MediaMixer, didOutput buffer: AVAudioPCMBuffer, when: AVAudioTime) {
        mixerAudioContinuation?.yield((buffer, when))
    }
}
