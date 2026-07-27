// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "storage-kit",
    platforms: [.macOS(.v15), .iOS(.v18)],
    products: [
        .library(name: "StorageKit", targets: ["StorageKit"]),
        .library(name: "StorageKitSystemClock", targets: ["StorageKitSystemClock"]),
        .library(name: "StorageKitFoundation", targets: ["StorageKitFoundation"]),
        .library(name: "CloudflareDurableObjectStorageWire", targets: ["CloudflareDurableObjectStorageWire"]),
        .library(name: "FDBStorage", targets: ["FDBStorage"]),
        .library(name: "SQLiteStorage", targets: ["SQLiteStorage"]),
        .library(name: "PostgreSQLStorage", targets: ["PostgreSQLStorage"]),
        .library(name: "CloudflareDurableObjectStorage", targets: ["CloudflareDurableObjectStorage"]),
        .library(
            name: "CloudflareDurableObjectStorageTesting",
            targets: ["CloudflareDurableObjectStorageTesting"]
        ),
        .library(
            name: "CloudflareDurableObjectStorageHTTP",
            targets: ["CloudflareDurableObjectStorageHTTP"]
        ),
        .library(
            name: "CloudflareDurableObjectStorageHostTransport",
            targets: ["CloudflareDurableObjectStorageHostTransport"]
        ),
    ],
    dependencies: [
        .package(
            url: "https://github.com/1amageek/database-types.git",
            from: "26.0727.0"
        ),
        .package(
            url: "https://github.com/1amageek/fdb-swift-bindings.git",
            from: "0.3.1"
        ),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.25.0"),
    ],
    targets: [
        .target(
            name: "CloudflareDurableObjectStorageWire",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
            ]
        ),
        .target(
            name: "StorageKit",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
            ]
        ),
        .target(
            name: "StorageKitSystemClock",
            dependencies: [
                "StorageKit",
            ]
        ),
        .target(
            name: "StorageKitFoundation",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "DatabaseTypesFoundation", package: "database-types"),
                "StorageKit",
            ]
        ),
        .target(
            name: "FDBStorage",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "StorageKit",
                .product(
                    name: "FoundationDB",
                    package: "fdb-swift-bindings"
                ),
            ]
        ),
        .target(
            name: "SQLiteStorage",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "StorageKit",
            ]
        ),
        .target(
            name: "PostgreSQLStorage",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "StorageKit",
                .product(
                    name: "PostgresNIO",
                    package: "postgres-nio"
                ),
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorage",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "StorageKit",
                "StorageKitSystemClock",
                "CloudflareDurableObjectStorageWire",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageTesting",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorage",
                "CloudflareDurableObjectStorageWire",
                "StorageKit",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageHTTP",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorage",
                "StorageKit",
                "CloudflareDurableObjectStorageWire",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageHostTransport",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorage",
                "CloudflareDurableObjectStorageWire",
            ],
            swiftSettings: [
                .enableExperimentalFeature("Extern"),
            ]
        ),
        .testTarget(
            name: "StorageKitTests",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "StorageKit",
                "StorageKitFoundation",
            ]
        ),
        .testTarget(
            name: "FDBStorageTests",
            dependencies: [
                "FDBStorage",
                .product(name: "DatabaseTypes", package: "database-types"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib", "-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"]),
            ]
        ),
        .testTarget(
            name: "SQLiteStorageTests",
            dependencies: [
                "SQLiteStorage",
                .product(name: "DatabaseTypes", package: "database-types"),
            ]
        ),
        .testTarget(
            name: "PostgreSQLStorageTests",
            dependencies: [
                "PostgreSQLStorage",
                .product(name: "DatabaseTypes", package: "database-types"),
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageTests",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorage",
                "CloudflareDurableObjectStorageTesting",
                "CloudflareDurableObjectStorageWire",
                "StorageKitSystemClock",
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageWireTests",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorageWire",
            ],
            resources: [
                .copy("GoldenVectors"),
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageHostTransportTests",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorageHostTransport",
                "CloudflareDurableObjectStorageWire",
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageHTTPTests",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorageHTTP",
                "CloudflareDurableObjectStorageWire",
            ]
        ),
    ]
)
