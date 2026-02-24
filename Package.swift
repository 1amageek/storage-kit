// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "storage-kit",
    platforms: [.macOS(.v15), .iOS(.v18)],
    products: [
        .library(name: "StorageKit", targets: ["StorageKit"]),
        .library(name: "FDBStorage", targets: ["FDBStorage"]),
        .library(name: "SQLiteStorage", targets: ["SQLiteStorage"]),
    ],
    dependencies: [
        .package(url: "https://github.com/1amageek/fdb-swift-bindings.git", branch: "main"),
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
            ]
        ),
        .target(
            name: "SQLiteStorage",
            dependencies: [
                "StorageKit",
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
    ]
)
