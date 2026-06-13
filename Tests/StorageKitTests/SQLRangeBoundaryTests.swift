import Testing
@testable import StorageKit

@Suite("SQLRangeBoundary Tests")
struct SQLRangeBoundaryTests {

    @Test func beginBoundaryMappings() throws {
        let key: Bytes = [0x01, 0x02, 0x03]

        #expect(try SQLRangeBoundary.begin(.firstGreaterOrEqual(key)) == .direct(op: ">=", key: key))
        #expect(try SQLRangeBoundary.begin(.firstGreaterThan(key)) == .direct(op: ">", key: key))
        #expect(
            try SQLRangeBoundary.begin(.lastLessOrEqual(key))
                == .resolvedSubquery(op: ">=", subqueryOp: "<=", key: key)
        )
        #expect(
            try SQLRangeBoundary.begin(.lastLessThan(key))
                == .resolvedSubquery(op: ">=", subqueryOp: "<", key: key)
        )
    }

    @Test func endBoundaryMappings() throws {
        let key: Bytes = [0x01, 0x02, 0x03]

        #expect(try SQLRangeBoundary.end(.firstGreaterOrEqual(key)) == .direct(op: "<", key: key))
        #expect(try SQLRangeBoundary.end(.firstGreaterThan(key)) == .direct(op: "<=", key: key))
        #expect(
            try SQLRangeBoundary.end(.lastLessOrEqual(key))
                == .resolvedSubquery(op: "<", subqueryOp: "<=", key: key)
        )
        #expect(
            try SQLRangeBoundary.end(.lastLessThan(key))
                == .resolvedSubquery(op: "<", subqueryOp: "<", key: key)
        )
    }

    @Test func unsupportedBeginOffsetThrowsInvalidOperation() throws {
        let selector = KeySelector(key: [0x01], orEqual: false, offset: 2)

        do {
            _ = try SQLRangeBoundary.begin(selector)
            Issue.record("Expected unsupported begin selector to throw")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .rangeRead)
        }
    }

    @Test func unsupportedEndOffsetThrowsInvalidOperation() throws {
        let selector = KeySelector(key: [0x01], orEqual: true, offset: -1)

        do {
            _ = try SQLRangeBoundary.end(selector)
            Issue.record("Expected unsupported end selector to throw")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .rangeRead)
        }
    }
}
