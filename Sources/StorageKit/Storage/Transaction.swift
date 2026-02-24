/// KV トランザクションの抽象プロトコル
///
/// database-framework が実際に使用する最小限のインターフェース。
/// FDB 固有のパラメータ（snapshot, StreamingMode）は除外している。
public protocol Transaction: Sendable {

    // MARK: - Read

    /// キーに対応する値を取得（存在しない場合は nil）
    func getValue(for key: Bytes) async throws -> Bytes?

    /// 範囲スキャン
    ///
    /// - Parameters:
    ///   - begin: 開始キー（含む）
    ///   - end: 終了キー（含まない）
    ///   - limit: 最大取得件数（0 は無制限）
    ///   - reverse: true の場合、逆順（end 側から）スキャン
    func getRange(
        begin: Bytes,
        end: Bytes,
        limit: Int,
        reverse: Bool
    ) async throws -> KeyValueSequence

    // MARK: - Write

    /// キーに値を設定（既存の値は上書き）
    func setValue(_ value: Bytes, for key: Bytes)

    /// キーを削除
    func clear(key: Bytes)

    /// 範囲内の全キーを削除
    ///
    /// - Parameters:
    ///   - begin: 開始キー（含む）
    ///   - end: 終了キー（含まない）
    func clearRange(begin: Bytes, end: Bytes)

    // MARK: - Transaction Management

    /// トランザクションをコミット
    func commit() async throws

    /// トランザクションをキャンセル（未コミットの変更を破棄）
    func cancel()
}
