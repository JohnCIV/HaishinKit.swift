@preconcurrency import AVFoundation
import Foundation

/// An actor that provides a stream playback feature.
package final actor IncomingStream {
    public private(set) var isRunning = false
    /// The sound transform value control.
    public var soundTransfrom: SoundTransform? {
        get async {
            return await audioPlayerNode?.soundTransfrom
        }
    }
    private lazy var mediaLink = MediaLink()
    private lazy var audioCodec = AudioCodec()
    private lazy var videoCodec = VideoCodec()
    private weak var stream: (any StreamConvertible)?
    private var audioPlayerNode: AudioPlayerNode?

    /// Nonisolated delivery handlers — bypass actor re-entry for decoded frame delivery.
    /// Set by the owning stream (e.g. RTMPStream) before startRunning() to deliver
    /// decoded frames directly to StreamOutput objects without hopping back to the
    /// stream actor. When nil, falls back to `await stream?.append()`.
    nonisolated(unsafe) package var videoDeliveryHandler: (@Sendable (CMSampleBuffer) -> Void)?
    nonisolated(unsafe) package var audioDeliveryHandler: (@Sendable (AVAudioBuffer, AVAudioTime) -> Void)?

    /// Creates a new instance.
    public init(_ stream: some StreamConvertible) {
        self.stream = stream
    }

    /// Sets the sound transform value control.
    public func setSoundTransform(_ soundTransfrom: SoundTransform) async {
        await audioPlayerNode?.setSoundTransfrom(soundTransfrom)
    }

    /// Appends a sample buffer for playback.
    public func append(_ buffer: CMSampleBuffer) {
        switch buffer.formatDescription?.mediaType {
        case .audio:
            audioCodec.append(buffer)
        case .video:
            videoCodec.append(buffer)
        default:
            break
        }
    }

    /// Appends an audio buffer for playback.
    public func append(_ buffer: AVAudioBuffer, when: AVAudioTime) {
        audioCodec.append(buffer, when: when)
    }

    /// Attaches an audio player.
    public func attachAudioPlayer(_ audioPlayer: AudioPlayer?) async {
        await audioPlayerNode?.detach()
        audioPlayerNode = await audioPlayer?.makePlayerNode()
        await mediaLink.setAudioPlayer(audioPlayerNode)
    }
}

extension IncomingStream: AsyncRunner {
    // MARK: AsyncRunner
    public func startRunning() {
        guard !isRunning else {
            return
        }
        audioCodec.settings.format = .pcm
        videoCodec.startRunning()
        audioCodec.startRunning()
        isRunning = true
        // Capture codec output streams and references while on the actor,
        // then run consumers in detached Tasks so decoded frames don't
        // compete with append() for IncomingStream actor scheduling time.
        let videoOutput = videoCodec.outputStream
        let audioOutput = audioCodec.outputStream
        let streamRef = self.stream
        let audioPlayerRef = self.audioPlayerNode
        // Capture delivery handlers — when set, decoded frames are delivered
        // directly to StreamOutput objects without re-entering the stream actor.
        // This eliminates the primary bottleneck where ~24 decoded frames/sec
        // competed with ~50 RTMP message dispatches/sec for actor scheduling.
        let videoHandler = self.videoDeliveryHandler
        let audioHandler = self.audioDeliveryHandler
        Task.detached {
            for await video in videoOutput {
                if let videoHandler {
                    videoHandler(video)
                } else {
                    await streamRef?.append(video)
                }
            }
        }
        Task.detached {
            await audioPlayerRef?.startRunning()
            for await audio in audioOutput {
                await audioPlayerRef?.enqueue(audio.0, when: audio.1)
                if let audioHandler {
                    audioHandler(audio.0, audio.1)
                } else {
                    await streamRef?.append(audio.0, when: audio.1)
                }
            }
        }
    }

    public func stopRunning() {
        guard isRunning else {
            return
        }
        videoCodec.stopRunning()
        audioCodec.stopRunning()
        Task { await mediaLink.stopRunning() }
        Task { await audioPlayerNode?.stopRunning() }
        isRunning = false
    }
}
