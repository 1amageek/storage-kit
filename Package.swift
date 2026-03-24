// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "storage-kit",
    platforms: [.macOS(.v15), .iOS(.v18)],
    products: [
        .library(name: "StorageKit", targets: ["StorageKit"]),
        .library(name: "FDBStorage", targets: ["FDBStorage"]),
        .library(name: "SQLiteStorage", targets: ["SQLiteStorage"]),
        .library(name: "PostgreSQLStorage", targets: ["PostgreSQLStorage"]),
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
            name: "StorageKit",
            dependencies: []
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
    ]
)
