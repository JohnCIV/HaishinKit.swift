import Foundation

/// Classification of an outbound RTMP message for the freshness gate, decided
/// at doOutput time where the concrete message type is still known.
enum RTMPOutboundKind: Sendable {
    /// A coded video frame (AVC NAL / HEVC codedFrames). The only kind the
    /// freshness gate may drop.
    case videoCoded(isKeyFrame: Bool)
    /// Video decoder configuration (AVC seq / HEVC sequenceStart). Required
    /// receiver state — never dropped.
    case videoConfig
    /// Audio. Never dropped: continuity is the product requirement.
    case audio
    /// Commands, control, data. Never dropped.
    case other
}

/// S45 (FLS): freshness-over-completeness policy for the RTMP send backlog.
///
/// During an uplink micro-outage the TCP socket blocks and outbound messages
/// queue app-side (TCP is lossless — nothing drops on its own). On link
/// recovery the whole backlog would flush at once, up to 13x real-time; the
/// remote live packager keeps what fits the live edge and discards the stale
/// tail, so the viewer gets a fast-forward seam and the VOD loses real time.
/// Worse, a discard anywhere mid-GOP breaks the HEVC reference chain (S43
/// green garble).
///
/// This gate runs where messages are dequeued for serialization. When a video
/// frame has sat in the queue longer than `staleThresholdMs`, it and every
/// subsequent video frame are dropped until the first KEYFRAME that is itself
/// fresh — whole GOPs only, so the reference chain is never broken. Audio,
/// video decoder config, and command/control messages always pass. The
/// timestamp deltas of dropped frames accumulate in `carryMs` and are added to
/// the resume keyframe's delta, keeping the receiver's video clock
/// wall-accurate: the picture jumps to live instead of falling behind audio.
///
/// The threshold sits far above normal Starlink jitter (100–400 ms send
/// spikes, ~15 s handover cadence) so a healthy link never drops anything —
/// queue age only approaches it when the socket is genuinely blocked.
struct RTMPFreshnessGate {
    struct DropEpisode {
        let droppedFrames: Int
        let droppedMs: UInt64
    }

    enum Verdict {
        /// Serialize and send. `timestampShiftMs` is non-zero only on the
        /// keyframe that ends a drop episode: add it to the message's delta
        /// timestamp before serialization. `endedEpisode` reports a just-closed
        /// drop episode for logging/telemetry.
        case send(timestampShiftMs: UInt32, endedEpisode: DropEpisode?)
        case drop
    }

    static let defaultStaleThresholdMs: Double = 2500

    let staleThresholdMs: Double
    private var droppingToKeyFrame = false
    private var carryMs: UInt64 = 0
    private var episodeDroppedFrames = 0
    private var episodeDroppedMs: UInt64 = 0

    init(staleThresholdMs: Double = RTMPFreshnessGate.defaultStaleThresholdMs) {
        self.staleThresholdMs = staleThresholdMs
    }

    /// - Parameters:
    ///   - kind: message classification from doOutput.
    ///   - chunkType: the RTMP chunk type the message will serialize with
    ///     (`.one` = delta timestamp, `.zero` = absolute).
    ///   - timestampMs: the message's timestamp field (a delta for `.one`).
    ///   - ageMs: how long the message sat in the outbound queue.
    mutating func verdict(kind: RTMPOutboundKind, chunkType: RTMPChunkType, timestampMs: UInt32, ageMs: Double) -> Verdict {
        switch kind {
        case .audio, .other, .videoConfig:
            return .send(timestampShiftMs: 0, endedEpisode: nil)
        case .videoCoded(let isKeyFrame):
            // An absolute timestamp rebases the receiver's video clock by
            // itself; discard any pending carry and pass it through.
            if chunkType == .zero {
                let episode = endEpisodeIfAny()
                carryMs = 0
                return .send(timestampShiftMs: 0, endedEpisode: episode)
            }
            if droppingToKeyFrame {
                if isKeyFrame && ageMs <= staleThresholdMs {
                    let shift = UInt32(clamping: carryMs)
                    carryMs = 0
                    let episode = endEpisodeIfAny()
                    return .send(timestampShiftMs: shift, endedEpisode: episode)
                }
                recordDrop(timestampMs)
                return .drop
            }
            if ageMs > staleThresholdMs {
                droppingToKeyFrame = true
                recordDrop(timestampMs)
                return .drop
            }
            return .send(timestampShiftMs: 0, endedEpisode: nil)
        }
    }

    private mutating func recordDrop(_ timestampMs: UInt32) {
        carryMs += UInt64(timestampMs)
        episodeDroppedFrames += 1
        episodeDroppedMs += UInt64(timestampMs)
    }

    private mutating func endEpisodeIfAny() -> DropEpisode? {
        guard droppingToKeyFrame else { return nil }
        droppingToKeyFrame = false
        let episode = DropEpisode(droppedFrames: episodeDroppedFrames, droppedMs: episodeDroppedMs)
        episodeDroppedFrames = 0
        episodeDroppedMs = 0
        return episode
    }
}

/// Wraps a message so the freshness gate can add the accumulated dropped-frame
/// deltas onto the keyframe that resumes video after a drop episode. Payload
/// and identity pass through untouched.
struct RTMPTimestampShiftedMessage: RTMPMessage {
    let base: any RTMPMessage
    let timestamp: UInt32

    var type: RTMPMessageType { base.type }
    var streamId: UInt32 { base.streamId }
    var payload: Data { base.payload }
}
