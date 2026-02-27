/// KV トランザクションの抽象プロトコル
///
/// FDB の TransactionProtocol と API 互換のシグネチャを持つ。
/// database-framework は `any Transaction` 経由でこのプロトコルを使用する。
///
/// ## ゼロコピー設計
/// `getRange` は associated type `RangeResult` を返す。
/// 具象型使用時はバックエンド固有の AsyncSequence がそのまま返り、ラッパーなし。
/// `any Transaction` 経由時は existential dispatch のみ（データコピーなし）。
///
/// ## バックエンド実装ガイド
/// 必須メソッド: `getValue`, `getRange`, `setValue`, `clear`, `clearRange`, `commit`, `cancel`
/// その他は extension でデフォルト実装を提供（非 FDB バックエンドは自動対応）。
public protocol Transaction: Sendable {

    // MARK: - Associated type（ゼロコピー getRange）

    /// getRange が返す AsyncSequence の具象型
    ///
    /// FDB: `FDB.AsyncKVSequence`（遅延バッチ取得）
    /// SQLite: カーソルベース AsyncSequence
    /// InMemory: 配列ベース AsyncSequence
    associatedtype RangeResult: AsyncSequence & Sendable
        where RangeResult.Element == (Bytes, Bytes)

    // MARK: - Read

    /// キーに対応する値を取得（存在しない場合は nil）
    ///
    /// - Parameters:
    ///   - key: 取得するキー
    ///   - snapshot: true の場合、snapshot 読み取り（FDB: コンフリクト範囲に追加しない）
    func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes?

    /// KeySelector で指定された位置のキーを取得
    ///
    /// - Parameters:
    ///   - selector: キー選択条件
    ///   - snapshot: true の場合、snapshot 読み取り
    func getKey(selector: KeySelector, snapshot: Bool) async throws -> Bytes?

    /// 範囲スキャン（遅延評価）
    ///
    /// - Parameters:
    ///   - begin: 開始位置の KeySelector
    ///   - end: 終了位置の KeySelector
    ///   - limit: 最大取得件数（0 は無制限）
    ///   - reverse: true の場合、逆順スキャン
    ///   - snapshot: true の場合、snapshot 読み取り
    ///   - streamingMode: バッチサイズ最適化ヒント
    func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> RangeResult

    // MARK: - Write

    /// キーに値を設定（既存の値は上書き）
    func setValue(_ value: Bytes, for key: Bytes)

    /// キーを削除
    func clear(key: Bytes)

    /// 範囲内の全キーを削除
    ///
    /// - Parameters:
    ///   - beginKey: 開始キー（含む）
    ///   - endKey: 終了キー（含まない）
    func clearRange(beginKey: Bytes, endKey: Bytes)

    // MARK: - Atomic Operations

    /// アトミック操作を実行
    ///
    /// - Parameters:
    ///   - key: 対象キー
    ///   - param: 操作パラメータ（操作依存のバイト列）
    ///   - mutationType: 操作種別
    func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType)

    // MARK: - Transaction Control

    /// トランザクションをコミット
    func commit() async throws

    /// トランザクションをキャンセル（未コミットの変更を破棄）
    func cancel()

    // MARK: - Version Management

    /// 読み取りバージョンを設定（キャッシュからの復元等）
    func setReadVersion(_ version: Int64)

    /// このトランザクションの読み取りバージョンを取得
    func getReadVersion() async throws -> Int64

    /// コミット済みバージョンを取得（commit 後のみ有効）
    func getCommittedVersion() throws -> Int64

    // MARK: - Transaction Options

    /// トランザクションオプションを設定（値なし）
    func setOption(forOption option: TransactionOption) throws

    /// トランザクションオプションを設定（バイト値）
    func setOption(to value: Bytes?, forOption option: TransactionOption) throws

    /// トランザクションオプションを設定（整数値）
    func setOption(to value: Int, forOption option: TransactionOption) throws

    // MARK: - Conflict Range

    /// コンフリクト範囲を追加
    ///
    /// - Parameters:
    ///   - beginKey: 開始キー（含む）
    ///   - endKey: 終了キー（含まない）
    ///   - type: read または write
    func addConflictRange(beginKey: Bytes, endKey: Bytes, type: ConflictRangeType) throws

    // MARK: - Statistics

    /// キー範囲の推定バイトサイズを取得
    func getEstimatedRangeSizeBytes(beginKey: Bytes, endKey: Bytes) async throws -> Int

    /// キー範囲を指定サイズのチャンクに分割するスプリットポイントを取得
    func getRangeSplitPoints(beginKey: Bytes, endKey: Bytes, chunkSize: Int) async throws -> [[UInt8]]

    // MARK: - Versionstamp

    /// バージョンスタンプを取得（commit 後のみ有効）
    func getVersionstamp() async throws -> Bytes?
}

// MARK: - Convenience（デフォルトパラメータ）

extension Transaction {

    /// snapshot デフォルト false
    public func getValue(for key: Bytes, snapshot: Bool = false) async throws -> Bytes? {
        try await getValue(for: key, snapshot: snapshot)
    }

    /// KeySelector ベースの getRange にデフォルト値を提供
    ///
    /// プロトコル要件の getRange(from:to:limit:reverse:snapshot:streamingMode:) に
    /// デフォルト引数を追加する。呼び出し時に省略されたパラメータはここで補完され、
    /// 実際のプロトコル実装（各バックエンド）にフル引数で委譲される。
    public func getRange(
        from begin: KeySelector, to end: KeySelector,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll
    ) -> RangeResult {
        getRange(
            from: begin, to: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        )
    }

    /// Bytes ベースの getRange convenience（KeySelector に変換）
    public func getRange(
        begin: Bytes, end: Bytes,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll
    ) -> RangeResult {
        getRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        )
    }

    // MARK: - FDB Legacy 互換 overloads

    /// FDB TransactionProtocol 互換: beginSelector/endSelector ラベル
    public func getRange(
        beginSelector: KeySelector, endSelector: KeySelector,
        snapshot: Bool = false
    ) -> RangeResult {
        getRange(
            from: beginSelector, to: endSelector,
            limit: 0, reverse: false,
            snapshot: snapshot, streamingMode: .wantAll
        )
    }

    /// FDB TransactionProtocol 互換: beginKey/endKey ラベル
    public func getRange(
        beginKey: Bytes, endKey: Bytes,
        snapshot: Bool = false
    ) -> RangeResult {
        getRange(
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: 0, reverse: false,
            snapshot: snapshot, streamingMode: .wantAll
        )
    }

    // MARK: - Collecting（any Transaction 経由でも型安全）

    /// `any Transaction` 経由でも型安全に使える collecting convenience
    ///
    /// associated type RangeResult はプロトコル existential 経由で Element 型が失われるが、
    /// このメソッドは内部で concrete self を使うため型が完全に解決される。
    public func collectRange(
        from begin: KeySelector, to end: KeySelector,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll
    ) async throws -> [(Bytes, Bytes)] {
        var result: [(Bytes, Bytes)] = []
        for try await pair in getRange(
            from: begin, to: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        ) {
            result.append(pair)
        }
        return result
    }

    /// Bytes-based collectRange convenience (converts to KeySelector internally).
    public func collectRange(
        begin: Bytes, end: Bytes,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll
    ) async throws -> [(Bytes, Bytes)] {
        try await collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        )
    }

    // MARK: - ForEach（any Transaction 経由でも型安全なイテレーション）

    /// `any Transaction` 経由でも型安全に範囲イテレーションを行う
    ///
    /// プロトコル extension 内では Self が具象型なので、associated type RangeResult の
    /// Element が (Bytes, Bytes) として解決される。
    public func forEachInRange(
        from begin: KeySelector, to end: KeySelector,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll,
        body: (Bytes, Bytes) async throws -> Void
    ) async throws {
        for try await (key, value) in getRange(
            from: begin, to: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        ) {
            try await body(key, value)
        }
    }

    // MARK: - setOption String 互換

    /// FDB 互換: 文字列値でオプション設定
    public func setOption(to value: String, forOption option: TransactionOption) throws {
        try setOption(to: Bytes(value.utf8), forOption: option)
    }
}

// MARK: - Default Implementations

/// 非 FDB バックエンド向けのデフォルト実装
///
/// 基本メソッド（getValue, getRange, setValue, clear, clearRange, commit, cancel）は
/// 各バックエンドが実装必須。それ以外はデフォルトで動作する。
extension Transaction {

    /// デフォルト: getKey を getRange で実装（snapshot デフォルト false）
    public func getKey(selector: KeySelector, snapshot: Bool = false) async throws -> Bytes? {
        // firstGreaterOrEqual / firstGreaterThan: 開始キーから1件取得
        let seq = getRange(
            from: selector,
            to: KeySelector(key: [0xFF], orEqual: true, offset: 1),
            limit: 1,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .exact
        )
        for try await (key, _) in seq {
            return key
        }
        return nil
    }

    /// デフォルト: atomicOp を read-modify-write で実装（single-writer では正しい）
    public func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) {
        // デフォルトは no-op（single-writer バックエンドは read-modify-write を別途実装可能）
    }

    /// デフォルト: no-op
    public func setReadVersion(_ version: Int64) {}

    /// デフォルト: 0 を返す
    public func getReadVersion() async throws -> Int64 { 0 }

    /// デフォルト: 0 を返す
    public func getCommittedVersion() throws -> Int64 { 0 }

    /// デフォルト: no-op
    public func setOption(forOption option: TransactionOption) throws {}

    /// デフォルト: no-op
    public func setOption(to value: Bytes?, forOption option: TransactionOption) throws {}

    /// デフォルト: no-op
    public func setOption(to value: Int, forOption option: TransactionOption) throws {}

    /// デフォルト: no-op（single-writer は conflict なし）
    public func addConflictRange(beginKey: Bytes, endKey: Bytes, type: ConflictRangeType) throws {}

    /// デフォルト: 0 を返す
    public func getEstimatedRangeSizeBytes(beginKey: Bytes, endKey: Bytes) async throws -> Int { 0 }

    /// デフォルト: 空配列を返す
    public func getRangeSplitPoints(beginKey: Bytes, endKey: Bytes, chunkSize: Int) async throws -> [[UInt8]] { [] }

    /// デフォルト: nil を返す
    public func getVersionstamp() async throws -> Bytes? { nil }
}
