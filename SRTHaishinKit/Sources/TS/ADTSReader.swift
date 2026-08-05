import Foundation
import HaishinKit

class ADTSReader: Sequence {
    private var data: Data = .init()

    func read(_ data: Data) {
        self.data = data
    }

    func makeIterator() -> ADTSReaderIterator {
        return ADTSReaderIterator(data: data)
    }
}

struct ADTSReaderIterator: IteratorProtocol {
    private let data: Data
    private var cursor: Int = 0
    private var header: ADTSHeader = .init()

    init(data: Data) {
        self.data = data
    }

    mutating func next() -> Int? {
        // (S37) A payload tail shorter than a full header, a missing sync byte, or a
        // parsed frame length below the header size all mean corrupt/truncated ADTS
        // (e.g. an SRT reconnect cutting the PES mid-frame). aacFrameLength == 0 would
        // otherwise never advance the cursor: infinite loop → unbounded sampleSizes →
        // OOM abort. Drop the malformed tail; the demux re-syncs on the next PES.
        guard cursor + ADTSHeader.size <= data.count else {
            return nil
        }
        header.data = data.advanced(by: cursor)
        guard header.sync == ADTSHeader.sync, ADTSHeader.size <= Int(header.aacFrameLength) else {
            return nil
        }
        defer {
            cursor += Int(header.aacFrameLength)
        }
        return Int(header.aacFrameLength)
    }
}
