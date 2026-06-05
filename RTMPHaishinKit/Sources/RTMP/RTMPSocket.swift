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
}

final class RTMPWireStatsRegistry: @unchecked Sendable {
    static let shared = RTMPWireStatsRegistry()
    private let lock = NSLock()
    private struct Window {
        var samples: [Double] = []
        var queue: Int = 0
        var lastUpdate: TimeInterval = 0
    }
    private var perHost: [String: Window] = [:]
    private let maxSamples = 60   // ~0.75 s of video+audio messages on a 30 fps stream

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
        perHost[host] = w
        lock.unlock()
    }

    func snapshot(host: String) -> RTMPWireStats? {
        lock.lock()
        defer { lock.unlock() }
        guard let w = perHost[host], !w.samples.isEmpty else { return nil }
        let mean = w.samples.reduce(0.0, +) / Double(w.samples.count)
        let mx = w.samples.max() ?? 0
        return RTMPWireStats(lastUpdate: w.lastUpdate,
                             rollingMeanSendDurMs: mean,
                             rollingMaxSendDurMs: mx,
                             queueBytesOut: w.queue,
                             samples: w.samples.count)
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
    private var outputs: AsyncStream<Data>.Continuation? {
        didSet {
            oldValue?.finish()
        }
    }
    private var qualityOfService: DispatchQoS = .userInitiated
    private var continuation: CheckedContinuation<Void, any Swift.Error>?
    private lazy var networkQueue = DispatchQueue(label: "com.haishinkit.HaishinKit.RTMPSocket.network", qos: qualityOfService)

    // Wire-egress instrumentation (paired with RTMPSender's wirelog). Captures pre/post
    // wall time around each `connection.send(.contentProcessed)` so we can distinguish
    // "encoder bursts into outputs stream" from "TCP send buffer back-pressures us".
    // Gated by the FLS app's `fls.streaming.wirelog.enabled` UserDefault — cached at
    // connect time so the drain loop skips even the timestamp captures when disabled.
    private var egressEnabled = false
    private var egressHost: String = ""
    private var egressLastPostWall: Double = 0
    private var egressFirstWall: Double = 0
    private var egressLogHandle: FileHandle?
    private var egressLogPath: String = ""
    private var egressLogRemaining: Int = 3600
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
        egressLogRemaining = 3600
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

    func send(_ data: Data) {
        guard connected else {
            return
        }
        queueBytesOut += data.count
        outputs?.yield(data)
    }

    // Concatenate all chunks from one RTMP message into a single Data and yield once.
    // The downstream consumer awaits `connection.send(.contentProcessed)` per yielded
    // Data; with 8KB chunkSize a single 33KB video message would otherwise produce 5
    // sequential continuation round-trips. One large Data = one round-trip, eliminates
    // the ~120ms audio bunching that head-of-line-blocks behind video messages.
    func send(_ iterator: AnyIterator<Data>) {
        guard connected else {
            return
        }
        var combined = Data()
        for data in iterator {
            combined.append(data)
        }
        guard !combined.isEmpty else { return }
        queueBytesOut += combined.count
        outputs?.yield(combined)
    }

    func send(_ chunks: [Data]) {
        guard connected else {
            return
        }
        guard !chunks.isEmpty else { return }
        if chunks.count == 1 {
            queueBytesOut += chunks[0].count
            outputs?.yield(chunks[0])
            return
        }
        var combined = Data()
        for data in chunks {
            combined.append(data)
        }
        queueBytesOut += combined.count
        outputs?.yield(combined)
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
        outputs = nil
        connection = nil
        continuation = nil
        try? egressLogHandle?.close()
        egressLogHandle = nil
    }

    private func recordEgress(preSend: Double, postSend: Double, byteCount: Int) {
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
            let header = "seq,wallElapsedMs,preSendMs,postSendMs,sendDurMs,intervalSincePrevPostMs,byteCount\n"
            egressLogHandle?.write(Data(header.utf8))
        }
        egressSeq += 1
        let wallElapsedMs = (postSend - egressFirstWall) * 1000
        let preSendMs = (preSend - egressFirstWall) * 1000
        let postSendMs = (postSend - egressFirstWall) * 1000
        let row = "\(egressSeq),\(String(format: "%.3f", wallElapsedMs)),\(String(format: "%.3f", preSendMs)),\(String(format: "%.3f", postSendMs)),\(String(format: "%.3f", sendDurMs)),\(String(format: "%.3f", intervalMs)),\(byteCount)\n"
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
            let (stream, continuation) = AsyncStream<Data>.makeStream()
            Task {
                for await data in stream where connected {
                    // Always-on: capture send-completion timing for the ABR controller.
                    // Two CACurrentMediaTime() calls + a registry update per RTMP message
                    // (~78/s for a 30 fps stream) — negligible cost, drives bitrate control.
                    let preSend = CACurrentMediaTime()
                    try await send(data)
                    let postSend = CACurrentMediaTime()
                    let sendDurMs = (postSend - preSend) * 1000
                    let queueSnapshot = queueBytesOut
                    if egressEnabled {
                        await recordEgress(preSend: preSend, postSend: postSend, byteCount: data.count)
                    }
                    RTMPWireStatsRegistry.shared.record(host: egressHost,
                                                       sendDurMs: sendDurMs,
                                                       queueBytesOut: queueSnapshot)
                    totalBytesOut += data.count
                    queueBytesOut -= data.count
                }
            }
            self.outputs = continuation
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

    private func send(_ data: Data) async throws {
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
