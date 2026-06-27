// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "storage-kit",
    platforms: [.macOS(.v15), .iOS(.v18)],
    products: [
        .library(name: "StorageKit", targets: ["StorageKit"]),
        .library(name: "StorageKitEmbeddedCore", targets: ["StorageKitEmbeddedCore"]),
        .library(name: "FDBStorage", targets: ["FDBStorage"]),
        .library(name: "SQLiteStorage", targets: ["SQLiteStorage"]),
        .library(name: "PostgreSQLStorage", targets: ["PostgreSQLStorage"]),
        .library(name: "CloudflareDurableObjectStorage", targets: ["CloudflareDurableObjectStorage"]),
        .library(name: "CloudflareDurableObjectStorageEmbedded", targets: ["CloudflareDurableObjectStorageEmbedded"]),
        .executable(name: "CloudflareDurableObjectStorageWasm", targets: ["CloudflareDurableObjectStorageWasm"]),
    ],
    traits: [
        .default(enabledTraits: ["FoundationDB", "SQLite"]),
        .trait(name: "FoundationDB"),
        .trait(name: "SQLite"),
        .trait(name: "PostgreSQL"),
    ],
    dependencies: [
        .package(url: "https://github.com/1amageek/fdb-swift-bindings.git", from: "0.1.0"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.25.0"),
    ],
    targets: [
        .target(
            name: "StorageKitEmbeddedCore",
            dependencies: []
        ),
        .target(
            name: "StorageKit",
            dependencies: [
                "StorageKitEmbeddedCore",
            ]
        ),
        .target(
            name: "FDBStorage",
            dependencies: [
                "StorageKit",
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            swiftSettings: [
                .define("STORAGE_FOUNDATIONDB", .when(traits: ["FoundationDB"])),
            ]
        ),
        .target(
            name: "SQLiteStorage",
            dependencies: [
                "StorageKit",
            ],
            swiftSettings: [
                .define("STORAGE_SQLITE", .when(traits: ["SQLite"])),
            ]
        ),
        .target(
            name: "PostgreSQLStorage",
            dependencies: [
                "StorageKit",
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ],
            swiftSettings: [
                .define("STORAGE_POSTGRESQL", .when(traits: ["PostgreSQL"])),
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorage",
            dependencies: [
                "StorageKit",
                "CloudflareDurableObjectStorageEmbedded",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageEmbedded",
            dependencies: [
                "StorageKitEmbeddedCore",
            ]
        ),
        .executableTarget(
            name: "CloudflareDurableObjectStorageWasm",
            dependencies: [
                "CloudflareDurableObjectStorageEmbedded",
            ],
            swiftSettings: [
                .enableExperimentalFeature("Extern"),
            ]
        ),
        .testTarget(
            name: "StorageKitTests",
            dependencies: ["StorageKit"]
        ),
        .testTarget(
            name: "FDBStorageTests",
            dependencies: ["FDBStorage"],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib", "-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"]),
            ]
        ),
        .testTarget(
            name: "SQLiteStorageTests",
            dependencies: ["SQLiteStorage"]
        ),
        .testTarget(
            name: "PostgreSQLStorageTests",
            dependencies: ["PostgreSQLStorage"]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageTests",
            dependencies: [
                "CloudflareDurableObjectStorage",
                "CloudflareDurableObjectStorageEmbedded",
                "StorageKitEmbeddedCore",
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageEmbeddedTests",
            dependencies: [
                "CloudflareDurableObjectStorageEmbedded",
                "StorageKitEmbeddedCore",
            ]
        ),
    ]
)
