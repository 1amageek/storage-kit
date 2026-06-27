/// Applies transaction-local writes to committed rows using StorageKit ordering.
public enum EmbeddedRangeOverlay {
    public static func value(
        for key: [UInt8],
        committed: [UInt8]?,
        applying writes: [EmbeddedWriteOperation]
    ) throws(EmbeddedRangeOverlayError) -> [UInt8]? {
        var value = committed
        for write in writes {
            switch write {
            case .set(let writeKey, let writeValue) where writeKey == key:
                value = writeValue
            case .clear(let writeKey) where writeKey == key:
                value = nil
            case .clearRange(let begin, let end)
                where EmbeddedByteOrdering.compare(key, begin) >= 0
                    && EmbeddedByteOrdering.compare(key, end) < 0:
                value = nil
            case .atomic(let writeKey, let param, let mutationType) where writeKey == key:
                let result: EmbeddedAtomicMutationResult
                do {
                    result = try mutationType.apply(to: value, param: param)
                } catch {
                    throw .mutation(error)
                }
                switch result {
                case .set(let bytes):
                    value = bytes
                case .clear:
                    value = nil
                case .unchanged:
                    break
                }
            default:
                continue
            }
        }
        return value
    }

    public static func overlay(
        committedRows: [EmbeddedKeyValue],
        writes: [EmbeddedWriteOperation],
        begin: EmbeddedKeySelector,
        end: EmbeddedKeySelector,
        reverse: Bool,
        limit: Int
    ) throws(EmbeddedRangeOverlayError) -> [EmbeddedKeyValue] {
        guard limit >= 0 else {
            throw .invalidRangeLimit
        }
        var rows = committedRows
        for write in writes {
            switch write {
            case .set(let key, let value):
                upsert(EmbeddedKeyValue(key: key, value: value), into: &rows)
            case .clear(let key):
                rows.removeAll { $0.key == key }
            case .clearRange(let begin, let end):
                rows.removeAll {
                    EmbeddedByteOrdering.compare($0.key, begin) >= 0
                        && EmbeddedByteOrdering.compare($0.key, end) < 0
                }
            case .atomic(let key, let param, let mutationType):
                let current = rows.first(where: { $0.key == key })?.value
                let result: EmbeddedAtomicMutationResult
                do {
                    result = try mutationType.apply(to: current, param: param)
                } catch {
                    throw .mutation(error)
                }
                switch result {
                case .set(let bytes):
                    upsert(EmbeddedKeyValue(key: key, value: bytes), into: &rows)
                case .clear:
                    rows.removeAll { $0.key == key }
                case .unchanged:
                    break
                }
            }
        }

        rows.sort { EmbeddedByteOrdering.lessThan($0.key, $1.key) }
        var keys: [[UInt8]] = []
        keys.reserveCapacity(rows.count)
        for row in rows {
            keys.append(row.key)
        }
        let start = max(0, begin.resolve(in: keys))
        let finish = min(rows.count, end.resolve(in: keys))
        guard start < finish else {
            return []
        }
        var result = Array(rows[start..<finish])
        if reverse {
            result.reverse()
        }
        if limit > 0 && result.count > limit {
            result = Array(result.prefix(limit))
        }
        return result
    }

    private static func upsert(_ row: EmbeddedKeyValue, into rows: inout [EmbeddedKeyValue]) {
        if let index = rows.firstIndex(where: { $0.key == row.key }) {
            rows[index] = row
        } else {
            rows.append(row)
        }
    }
}
