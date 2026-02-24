// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "storage-kit",
    platforms: [.macOS(.v15), .iOS(.v18)],
    products: [
        .library(name: "StorageKit", targets: ["StorageKit"]),
    ],
    targets: [
        .target(
            name: "StorageKit",
            dependencies: []
        ),
        .testTarget(
            name: "StorageKitTests",
            dependencies: ["StorageKit"]
        ),
    ]
)
