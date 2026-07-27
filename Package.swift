// swift-tools-version: 6.4
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
        .library(
            name: "CloudflareDurableObjectStorageTesting",
            targets: ["CloudflareDurableObjectStorageTesting"]
        ),
        .library(
            name: "CloudflareDurableObjectStorageHTTP",
            targets: ["CloudflareDurableObjectStorageHTTP"]
        ),
        .library(name: "CloudflareDurableObjectStorageEmbedded", targets: ["CloudflareDurableObjectStorageEmbedded"]),
        .library(
            name: "CloudflareDurableObjectStorageHostTransport",
            targets: ["CloudflareDurableObjectStorageHostTransport"]
        ),
    ],
    dependencies: [
        .package(
            url: "https://github.com/1amageek/database-types.git",
            from: "26.0726.0"
        ),
        .package(
            url: "https://github.com/1amageek/fdb-swift-bindings.git",
            from: "0.2.0"
        ),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.25.0"),
    ],
    targets: [
        .target(
            name: "StorageKitEmbeddedCore",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
            ]
        ),
        .target(
            name: "StorageKit",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "StorageKitEmbeddedCore",
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
                "StorageKitEmbeddedCore",
                "CloudflareDurableObjectStorageEmbedded",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageTesting",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorage",
                "StorageKit",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageHTTP",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorage",
                "StorageKit",
                "StorageKitEmbeddedCore",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageEmbedded",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "StorageKitEmbeddedCore",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageHostTransport",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorage",
                "StorageKitEmbeddedCore",
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
                "StorageKitEmbeddedCore",
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
                "CloudflareDurableObjectStorageEmbedded",
                "StorageKitEmbeddedCore",
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageEmbeddedTests",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorageEmbedded",
                "StorageKitEmbeddedCore",
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
                "StorageKitEmbeddedCore",
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageHTTPTests",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorageHTTP",
                "StorageKitEmbeddedCore",
            ]
        ),
    ]
)
