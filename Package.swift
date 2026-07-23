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
        .package(path: "../fdb-swift-bindings"),
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
                .product(
                    name: "FoundationDB",
                    package: "fdb-swift-bindings"
                ),
            ]
        ),
        .target(
            name: "SQLiteStorage",
            dependencies: [
                "StorageKit",
            ]
        ),
        .target(
            name: "PostgreSQLStorage",
            dependencies: [
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
                "StorageKit",
                "StorageKitEmbeddedCore",
                "CloudflareDurableObjectStorageEmbedded",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageTesting",
            dependencies: [
                "CloudflareDurableObjectStorage",
                "StorageKit",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageHTTP",
            dependencies: [
                "CloudflareDurableObjectStorage",
                "StorageKit",
                "StorageKitEmbeddedCore",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageEmbedded",
            dependencies: [
                "StorageKitEmbeddedCore",
            ]
        ),
        .target(
            name: "CloudflareDurableObjectStorageHostTransport",
            dependencies: [
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
                "StorageKit",
                "StorageKitEmbeddedCore",
            ]
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
                "CloudflareDurableObjectStorageTesting",
                "CloudflareDurableObjectStorageEmbedded",
                "StorageKitEmbeddedCore",
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageEmbeddedTests",
            dependencies: [
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
                "CloudflareDurableObjectStorageHostTransport",
                "StorageKitEmbeddedCore",
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageHTTPTests",
            dependencies: [
                "CloudflareDurableObjectStorageHTTP",
                "StorageKitEmbeddedCore",
            ]
        ),
    ]
)
