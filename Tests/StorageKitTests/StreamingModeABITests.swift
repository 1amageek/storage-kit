import Testing
@testable import StorageKit

@Suite("Streaming Mode ABI Tests")
struct StreamingModeABITests {
    @Test func rawValuesMatchFoundationDBContract() {
        #expect(StreamingMode.wantAll.rawValue == -2)
        #expect(StreamingMode.iterator.rawValue == -1)
        #expect(StreamingMode.exact.rawValue == 0)
        #expect(StreamingMode.small.rawValue == 1)
        #expect(StreamingMode.medium.rawValue == 2)
        #expect(StreamingMode.large.rawValue == 3)
        #expect(StreamingMode.serial.rawValue == 4)
    }

    @Test func rawValuesRoundTripAndRejectUnknownValues() {
        #expect(StreamingMode(rawValue: -2) == .wantAll)
        #expect(StreamingMode(rawValue: -1) == .iterator)
        #expect(StreamingMode(rawValue: 0) == .exact)
        #expect(StreamingMode(rawValue: 1) == .small)
        #expect(StreamingMode(rawValue: 2) == .medium)
        #expect(StreamingMode(rawValue: 3) == .large)
        #expect(StreamingMode(rawValue: 4) == .serial)
        #expect(StreamingMode(rawValue: 99) == nil)
    }
}
