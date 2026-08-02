import Logging
import Synchronization

final class PostgreSQLClientLifecycleLogRecorder: Sendable {
    private let recordedMessages = Mutex<[String]>([])

    var messages: [String] {
        recordedMessages.withLock { $0 }
    }

    func makeLogger() -> Logger {
        Logger(label: "PostgreSQLClientLifecycleTests") { _ in
            Handler(recorder: self)
        }
    }

    private func record(_ message: Logger.Message) {
        recordedMessages.withLock { $0.append(message.description) }
    }

    private struct Handler: LogHandler {
        let recorder: PostgreSQLClientLifecycleLogRecorder
        var metadataProvider: Logger.MetadataProvider?
        var metadata: Logger.Metadata = [:]
        var logLevel: Logger.Level = .trace

        init(recorder: PostgreSQLClientLifecycleLogRecorder) {
            self.recorder = recorder
            self.metadataProvider = nil
        }

        subscript(metadataKey key: String) -> Logger.Metadata.Value? {
            get { metadata[key] }
            set { metadata[key] = newValue }
        }

        func log(
            level: Logger.Level,
            message: Logger.Message,
            metadata: Logger.Metadata?,
            source: String,
            file: String,
            function: String,
            line: UInt
        ) {
            recorder.record(message)
        }
    }
}
