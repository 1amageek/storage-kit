// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "storage-kit",
    platforms: [.macOS(.v15), .iOS(.v18)],
    products: [
        .library(name: "StorageKit", targets: ["StorageKit"]),
        .library(name: "StorageKitSystemClock", targets: ["StorageKitSystemClock"]),
        .library(name: "StorageKitFoundation", targets: ["StorageKitFoundation"]),
        .library(name: "StorageKitConformance", targets: ["StorageKitConformance"]),
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
            from: "26.0730.0"
        ),
        .package(
            url: "https://github.com/1amageek/fdb-swift-bindings.git",
            revision: "7fd4813dd118d3fda64838de4f8375bc4fb59131"
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
            ],
            exclude: [
                "DESIGN.md",
                "Directory/DESIGN.md",
                "Storage/DESIGN.md",
                "Tuple/DESIGN.md",
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
            name: "StorageKitConformance",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "StorageKit",
            ],
            exclude: ["DESIGN.md"]
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
            ],
            exclude: ["DESIGN.md"]
        ),
        .systemLibrary(
            name: "SQLiteLibrary",
            pkgConfig: "sqlite3",
            providers: [
                .brew(["sqlite3"]),
                .apt(["libsqlite3-dev"]),
            ]
        ),
        .target(
            name: "SQLiteStorage",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "SQLiteLibrary",
                "StorageKit",
            ],
            exclude: ["DESIGN.md"]
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
            ],
            exclude: ["DESIGN.md"]
        ),
        .target(
            name: "CloudflareDurableObjectStorage",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "StorageKit",
                "CloudflareDurableObjectStorageWire",
            ],
            exclude: ["DESIGN.md"]
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
                "StorageKitConformance",
                "StorageKitFoundation",
            ]
        ),
        .testTarget(
            name: "FDBStorageTests",
            dependencies: [
                "FDBStorage",
                "StorageKitConformance",
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
                "StorageKitConformance",
                .product(name: "DatabaseTypes", package: "database-types"),
            ]
        ),
        .testTarget(
            name: "PostgreSQLStorageTests",
            dependencies: [
                "PostgreSQLStorage",
                "StorageKitConformance",
                .product(name: "DatabaseTypes", package: "database-types"),
            ]
        ),
        .testTarget(
            name: "CloudflareDurableObjectStorageTests",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                "CloudflareDurableObjectStorage",
                "StorageKitConformance",
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
