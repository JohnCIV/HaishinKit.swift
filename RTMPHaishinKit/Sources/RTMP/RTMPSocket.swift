import Foundation
import HaishinKit
import Network
import QuartzCore

/// Per-host TCP send-completion timing snapshot used by the FLS adaptive-bitrate
/// controller. Populated on every socket send by RTMPSocket; read out-of-band via
/// `RTMPSocket.wireStats(forHost:)`. The numbers are derived from the same kernel
/// `connection.send(.contentProcessed)` callback that the egress CSV uses.
public struct RTMPWireStats: Sendable {
    public let lastUpdate: TimeInterval
    public let rollingMeanSendDurMs: Double
    public let rollingMaxSendDurMs: Double
    public let queueBytesOut: Int
    public let samples: Int
    /// Age of the oldest currently-in-flight (un-completed) send, in ms; 0 when
    /// no send is in flight. S35: under a hard uplink collapse the serial send
    /// loop blocks inside one send and records NOTHING — the rolling window
    /// keeps its pre-collapse healthy samples, so completion-derived stats lie.
    /// Worse, the post-collapse flush burst completes dozens of queued messages
    /// fast, evicting the one giant-duration sample from the count-based window
    /// before the ABR tick can read it. A send that has been in flight for
    /// 800 ms IS a stall — no completion needed to know it. Consumers should
    /// treat max(rollingMaxSendDurMs, inFlightAgeMs) as the effective max.
    public let inFlightAgeMs: Double
    /// S45: cumulative count of stale queued video frames dropped forward to
    /// the next keyframe by the send-backlog freshness gate (connection-side),
    /// and the media time they covered. 0 on a healthy link.
    public let backlogDroppedFrames: Int
    public let backlogDroppedMs: Double
}

final class RTMPWireStatsRegistry: @unchecked Sendable {
    static let shared = RTMPWireStatsRegistry()
    private let lock = NSLock()
    private struct Window {
        var samples: [Double] = []
        var queue: Int = 0
        var lastUpdate: TimeInterval = 0
        var inFlightSince: Double = 0   // CACurrentMediaTime of current send's start; 0 = idle
        var backlogDroppedFrames: Int = 0
        var backlogDroppedMs: Double = 0
    }
    private var perHost: [String: Window] = [:]
    private let maxSamples = 60   // ~0.75 s of video+audio messages on a 30 fps stream

    /// Called immediately before the socket awaits a send. The send loop is
    /// serial, so a single timestamp per host suffices; `record` clears it.
    func markSendStart(host: String, at time: Double) {
        guard !host.isEmpty else { return }
        lock.lock()
        var w = perHost[host] ?? Window()
        w.inFlightSince = time
        perHost[host] = w
        lock.unlock()
    }

    func record(host: String, sendDurMs: Double, queueBytesOut: Int) {
        guard !host.isEmpty else { return }
        lock.lock()
        var w = perHost[host] ?? Window()
        w.samples.append(sendDurMs)
        if w.samples.count > maxSamples {
            w.samples.removeFirst(w.samples.count - maxSamples)
        }
        w.queue = queueBytesOut
        w.lastUpdate = CACurrentMediaTime()
        w.inFlightSince = 0
        perHost[host] = w
        lock.unlock()
    }

    /// S45: called by the connection's freshness gate once per drop episode.
    func recordBacklogDrop(host: String, frames: Int, droppedMs: Double) {
        guard !host.isEmpty else { return }
        lock.lock()
        var w = perHost[host] ?? Window()
        w.backlogDroppedFrames += frames
        w.backlogDroppedMs += droppedMs
        perHost[host] = w
        lock.unlock()
    }

    func snapshot(host: String) -> RTMPWireStats? {
        lock.lock()
        defer { lock.unlock() }
        guard let w = perHost[host], !w.samples.isEmpty else { return nil }
        let mean = w.samples.reduce(0.0, +) / Double(w.samples.count)
        let mx = w.samples.max() ?? 0
        let inFlightAge = w.inFlightSince > 0 ? (CACurrentMediaTime() - w.inFlightSince) * 1000 : 0
        return RTMPWireStats(lastUpdate: w.lastUpdate,
                             rollingMeanSendDurMs: mean,
                             rollingMaxSendDurMs: mx,
                             queueBytesOut: w.queue,
                             samples: w.samples.count,
                             inFlightAgeMs: inFlightAge,
                             backlogDroppedFrames: w.backlogDroppedFrames,
                             backlogDroppedMs: w.backlogDroppedMs)
    }

    func reset(host: String) {
        lock.lock()
        perHost.removeValue(forKey: host)
        lock.unlock()
    }
}

final actor RTMPSocket {
    static let defaultWindowSizeC = Int(UInt8.max)

    enum Error: Swift.Error {
        case invalidState
        case endOfStream
        case connectionTimedOut
        case connectionNotEstablished(_ error: NWError?)
    }

    private var timeout: UInt64 = 15
    private var connected = false
    private var windowSizeC = RTMPSocket.defaultWindowSizeC
    private var securityLevel: StreamSocketSecurityLevel = .none
    private var totalBytesIn = 0
    private var queueBytesOut = 0
    private var totalBytesOut = 0
    private var parameters: NWParameters = RTMPSocket.makeTCPParameters()
    private var connection: NWConnection? {
        didSet {
            oldValue?.viabilityUpdateHandler = nil
            oldValue?.stateUpdateHandler = nil
            oldValue?.forceCancel()
        }
    }
    private var qualityOfService: DispatchQoS = .userInitiated
    private var continuation: CheckedContinuation<Void, any Swift.Error>?
    private lazy var networkQueue = DispatchQueue(label: "com.haishinkit.HaishinKit.RTMPSocket.network", qos: qualityOfService)

    // Wire-egress instrumentation (paired with RTMPSender's wirelog). Captures pre/post
    // wall time around each `connection.send(.contentProcessed)` so we can distinguish
    // "encoder bursts into the send path" from "TCP send buffer back-pressures us".
    // Gated by the FLS app's `fls.streaming.wirelog.enabled` UserDefault — cached at
    // connect time so the send path skips even the timestamp captures when disabled.
    private var egressEnabled = false
    private var egressHost: String = ""
    private var egressLastPostWall: Double = 0
    private var egressFirstWall: Double = 0
    private var egressLogHandle: FileHandle?
    private var egressLogPath: String = ""
    // ~74 rows/s on a 30fps A+V stream; 90k rows ≈ 20 min of coverage. The 3600
    // cap only covered the first ~49s and missed late-session pacing regressions.
    private var egressLogRemaining: Int = 90_000
    private var egressSeq: Int64 = 0

    init() {
    }

    init(qualityOfService: DispatchQoS, securityLevel: StreamSocketSecurityLevel) {
        self.qualityOfService = qualityOfService
        switch securityLevel {
        case .ssLv2, .ssLv3, .tlSv1, .negotiatedSSL:
            parameters = RTMPSocket.makeTLSParameters()
        default:
            parameters = RTMPSocket.makeTCPParameters()
        }
    }

    // Disable Nagle's algorithm. Without TCP_NODELAY the kernel coalesces small
    // audio writes that arrive after a large video write, producing ~120ms gaps
    // between AAC packets on the wire.
    private static func makeTCPParameters() -> NWParameters {
        let p = NWParameters.tcp
        if let tcp = p.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
            tcp.noDelay = true
        }
        return p
    }

    private static func makeTLSParameters() -> NWParameters {
        let p = NWParameters.tls
        if let tcp = p.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
            tcp.noDelay = true
        }
        return p
    }

    func connect(_ name: String, port: Int) async throws {
        guard !connected else {
            throw Error.invalidState
        }
        totalBytesIn = 0
        totalBytesOut = 0
        queueBytesOut = 0
        egressEnabled = UserDefaults.standard.bool(forKey: "fls.streaming.wirelog.enabled")
        egressHost = name
        egressFirstWall = 0
        egressLastPostWall = 0
        egressLogRemaining = 90_000
        egressSeq = 0
        do {
            let connection = NWConnection(to: NWEndpoint.hostPort(host: .init(name), port: .init(integerLiteral: NWEndpoint.Port.IntegerLiteralType(port))), using: parameters)
            self.connection = connection
            try await withCheckedThrowingContinuation { (checkedContinuation: CheckedContinuation<Void, Swift.Error>) in
                self.continuation = checkedContinuation
                Task {
                    try? await Task.sleep(nanoseconds: timeout * 1_000_000_000)
                    guard let continuation else {
                        return
                    }
                    continuation.resume(throwing: Error.connectionTimedOut)
                    self.continuation = nil
                    close()
                }
                connection.stateUpdateHandler = { state in
                    Task { await self.stateDidChange(to: state) }
                }
                connection.viabilityUpdateHandler = { viability in
                    Task { await self.viabilityDidChange(to: viability) }
                }
                connection.start(queue: networkQueue)
            }
        } catch {
            throw error
        }
    }

    // The send methods complete only after the kernel has processed the bytes
    // (`connection.send(.contentProcessed)`), so a blocked TCP socket
    // back-pressures the caller instead of piling messages into an invisible
    // unbounded buffer here (S45: an 80 s uplink block queued ~2,400 frames
    // that flushed at 13x real-time on release). The backlog now accumulates
    // at the connection's outbound queue, where the freshness gate can drop
    // stale video forward to a keyframe. Callers must be serial per socket —
    // in practice: the handshake path, then RTMPConnection's single
    // chunkOutTask — so wire order is preserved.
    //
    // Returns false when nothing reached the kernel (socket closed or the
    // send errored). The consumer uses this to skip drain pacing and drop
    // accounting while finish-draining a dead connection's queue — the
    // 2026-08-14 rig session showed a recycle's teardown drain pacing for
    // seconds and polluting the wire-stats registry with phantom drops.
    @discardableResult
    func send(_ data: Data) async -> Bool {
        guard connected else {
            return false
        }
        queueBytesOut += data.count
        return await performSend(data, tag: 0)
    }

    // Concatenate all chunks from one RTMP message into a single Data and send once.
    // With 8KB chunkSize a single 33KB video message would otherwise produce 5
    // sequential kernel round-trips. One large Data = one round-trip, eliminates
    // the ~120ms audio bunching that head-of-line-blocks behind video messages.
    @discardableResult
    func send(_ chunks: [Data], tag: UInt16 = 0) async -> Bool {
        guard connected else {
            return false
        }
        guard !chunks.isEmpty else { return false }
        if chunks.count == 1 {
            queueBytesOut += chunks[0].count
            return await performSend(chunks[0], tag: tag)
        }
        var combined = Data()
        for data in chunks {
            combined.append(data)
        }
        queueBytesOut += combined.count
        return await performSend(combined, tag: tag)
    }

    // The tag is the chunkStreamId (0x04 audio, 0x05 video, 0 for
    // handshake/untagged); it flows into the egress CSV so wire interleave can
    // be measured per media type.
    private func performSend(_ data: Data, tag: UInt16) async -> Bool {
        // Always-on: capture send-completion timing for the ABR controller.
        // Two CACurrentMediaTime() calls + a registry update per RTMP message
        // (~78/s for a 30 fps stream) — negligible cost, drives bitrate control.
        // S35: mark the send start BEFORE awaiting so a blocked send is
        // visible to snapshot() as a growing inFlightAgeMs while it hangs.
        let preSend = CACurrentMediaTime()
        RTMPWireStatsRegistry.shared.markSendStart(host: egressHost, at: preSend)
        do {
            try await transmit(data)
        } catch {
            // The NWConnection state/viability handlers own closing the socket;
            // leave the accounting to the reconnect reset.
            return false
        }
        let postSend = CACurrentMediaTime()
        let sendDurMs = (postSend - preSend) * 1000
        let queueSnapshot = queueBytesOut
        if egressEnabled {
            recordEgress(preSend: preSend, postSend: postSend, byteCount: data.count, csid: tag)
        }
        RTMPWireStatsRegistry.shared.record(host: egressHost,
                                            sendDurMs: sendDurMs,
                                            queueBytesOut: queueSnapshot)
        totalBytesOut += data.count
        queueBytesOut -= data.count
        return true
    }

    func recv() -> AsyncStream<Data> {
        AsyncStream<Data> { continuation in
            Task {
                do {
                    while connected {
                        let data = try await recv()
                        continuation.yield(data)
                        totalBytesIn += data.count
                    }
                } catch {
                    continuation.finish()
                }
            }
        }
    }

    func close(_ error: NWError? = nil) {
        guard connection != nil else {
            return
        }
        if let continuation {
            continuation.resume(throwing: Error.connectionNotEstablished(error))
            self.continuation = nil
        }
        connected = false
        connection = nil
        continuation = nil
        try? egressLogHandle?.close()
        egressLogHandle = nil
    }

    private func recordEgress(preSend: Double, postSend: Double, byteCount: Int, csid: UInt16) {
        if egressFirstWall == 0 { egressFirstWall = preSend }
        let sendDurMs = (postSend - preSend) * 1000
        let intervalMs = egressLastPostWall > 0 ? (postSend - egressLastPostWall) * 1000 : 0
        egressLastPostWall = postSend
        guard egressLogRemaining > 0 else { return }
        if egressLogHandle == nil {
            let safeHost = egressHost.replacingOccurrences(of: "/", with: "_")
                                     .replacingOccurrences(of: ":", with: "_")
                                     .replacingOccurrences(of: " ", with: "_")
            let stamp = ISO8601DateFormatter().string(from: Date())
                                              .replacingOccurrences(of: ":", with: "-")
            let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
            let dir = docs.appendingPathComponent("Telemetry", isDirectory: true)
            try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
            let url = dir.appendingPathComponent("egress_\(safeHost)_\(stamp).csv")
            FileManager.default.createFile(atPath: url.path, contents: nil)
            egressLogHandle = try? FileHandle(forWritingTo: url)
            egressLogPath = url.path
            let header = "seq,wallElapsedMs,preSendMs,postSendMs,sendDurMs,intervalSincePrevPostMs,byteCount,csid\n"
            egressLogHandle?.write(Data(header.utf8))
        }
        egressSeq += 1
        let wallElapsedMs = (postSend - egressFirstWall) * 1000
        let preSendMs = (preSend - egressFirstWall) * 1000
        let postSendMs = (postSend - egressFirstWall) * 1000
        let row = "\(egressSeq),\(String(format: "%.3f", wallElapsedMs)),\(String(format: "%.3f", preSendMs)),\(String(format: "%.3f", postSendMs)),\(String(format: "%.3f", sendDurMs)),\(String(format: "%.3f", intervalMs)),\(byteCount),\(csid)\n"
        egressLogHandle?.write(Data(row.utf8))
        egressLogRemaining -= 1
        if egressLogRemaining == 0 {
            try? egressLogHandle?.close()
            egressLogHandle = nil
        }
    }

    private func stateDidChange(to state: NWConnection.State) {
        switch state {
        case .ready:
            logger.info("Connection is ready.")
            connected = true
            self.continuation?.resume()
            self.continuation = nil
        case .waiting(let error):
            logger.warn("Connection waiting:", error)
            close(error)
        case .setup:
            logger.debug("Connection is setting up.")
        case .preparing:
            logger.debug("Connection is preparing.")
        case .failed(let error):
            logger.warn("Connection failed:", error)
            close(error)
        case .cancelled:
            logger.info("Connection cancelled.")
            close()
        @unknown default:
            logger.error("Unknown connection state.")
        }
    }

    private func viabilityDidChange(to viability: Bool) {
        logger.info("Connection viability changed to ", viability)
        if viability == false {
            close()
        }
    }

    private func transmit(_ data: Data) async throws {
        return try await withCheckedThrowingContinuation { continuation in
            guard let connection else {
                continuation.resume(throwing: Error.invalidState)
                return
            }
            connection.send(content: data, completion: .contentProcessed { error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                continuation.resume()
            })
        }
    }

    private func recv() async throws -> Data {
        return try await withCheckedThrowingContinuation { continuation in
            guard let connection else {
                continuation.resume(throwing: Error.invalidState)
                return
            }
            connection.receive(minimumIncompleteLength: 0, maximumLength: windowSizeC) { content, _, _, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                if let content {
                    continuation.resume(returning: content)
                } else {
                    continuation.resume(throwing: Error.endOfStream)
                }
            }
        }
    }
}

/// Public namespace exposing per-host TCP send-completion timing collected by
/// RTMPSocket. Used by the FLS adaptive-bitrate controller. Nonisolated and
/// lock-protected. `RTMPSocket` itself is internal to the package.
public enum RTMPWireStatsAPI {
    public static func snapshot(forHost host: String) -> RTMPWireStats? {
        return RTMPWireStatsRegistry.shared.snapshot(host: host)
    }

    public static func reset(forHost host: String) {
        RTMPWireStatsRegistry.shared.reset(host: host)
    }
}

extension RTMPSocket: NetworkTransportReporter {
    // MARK: NetworkTransportReporter
    func makeNetworkMonitor() async -> NetworkMonitor {
        return .init(self)
    }

    func makeNetworkTransportReport() -> NetworkTransportReport {
        return .init(queueBytesOut: queueBytesOut, totalBytesIn: totalBytesIn, totalBytesOut: totalBytesOut)
    }
}
