// swift-tools-version:5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

// The MIT License (MIT)
//
// Copyright (c) 2020 Realm Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

import PackageDescription

let package = Package(
    name: "SwiftLint",
    platforms: [.macOS(.v12)],
    products: [
        .executable(name: "swiftlint", targets: ["swiftlint"]),
        .library(name: "SwiftLintFramework", targets: ["SwiftLintFramework"]),
        .plugin(name: "SwiftLintPlugin", targets: ["SwiftLintPlugin"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-argument-parser.git", .upToNextMinor(from: "1.2.1")),
        .package(url: "https://github.com/apple/swift-syntax.git", exact: "509.0.0"),
        .package(url: "https://github.com/jpsim/SourceKitten.git", .upToNextMinor(from: "0.34.1")),
        .package(url: "https://github.com/jpsim/Yams.git", from: "5.0.6"),
        .package(url: "https://github.com/scottrhoyt/SwiftyTextTable.git", from: "0.9.0"),
        .package(url: "https://github.com/JohnSundell/CollectionConcurrencyKit.git", from: "0.2.0"),
        .package(url: "https://github.com/krzyzanowskim/CryptoSwift.git", .upToNextMinor(from: "1.7.2"))
    ],
    targets: [
        .plugin(
            name: "SwiftLintPlugin",
            capability: .buildTool(),
            dependencies: [
                .target(name: "SwiftLintBinary", condition: .when(platforms: [.macOS])),
                .target(name: "swiftlint", condition: .when(platforms: [.linux]))
            ]
        ),
        .executableTarget(
            name: "swiftlint",
            dependencies: [
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
                "CollectionConcurrencyKit",
                "SwiftLintFramework",
                "SwiftyTextTable",
            ]
        ),
        .testTarget(
            name: "CLITests",
            dependencies: [
                "swiftlint"
            ]
        ),
        .target(
            name: "SwiftLintCore",
            dependencies: [
                .product(name: "CryptoSwift", package: "CryptoSwift", condition: .when(platforms: [.linux])),
                .target(name: "DyldWarningWorkaround", condition: .when(platforms: [.macOS])),
                .product(name: "SourceKittenFramework", package: "SourceKitten"),
                .product(name: "SwiftIDEUtils", package: "swift-syntax"),
                .product(name: "SwiftOperators", package: "swift-syntax"),
                .product(name: "SwiftParser", package: "swift-syntax"),
                .product(name: "SwiftSyntax", package: "swift-syntax"),
                .product(name: "SwiftSyntaxBuilder", package: "swift-syntax"),
                .product(name: "SwiftyTextTable", package: "SwiftyTextTable"),
                .product(name: "Yams", package: "Yams"),
            ]
        ),
        .target(
            name: "SwiftLintBuiltInRules",
            dependencies: ["SwiftLintCore"]
        ),
        .target(
            name: "SwiftLintExtraRules",
            dependencies: ["SwiftLintCore"]
        ),
        .target(
            name: "SwiftLintFramework",
            dependencies: [
                "SwiftLintBuiltInRules",
                "SwiftLintCore",
                "SwiftLintExtraRules"
            ]
        ),
        .target(name: "DyldWarningWorkaround"),
        .target(
            name: "SwiftLintTestHelpers",
            dependencies: [
                "SwiftLintFramework"
            ],
            path: "Tests/SwiftLintTestHelpers"
        ),
        .testTarget(
            name: "SwiftLintFrameworkTests",
            dependencies: [
                "SwiftLintFramework",
                "SwiftLintTestHelpers"
            ],
            exclude: [
                "Resources",
            ]
        ),
        .testTarget(
            name: "GeneratedTests",
            dependencies: [
                "SwiftLintFramework",
                "SwiftLintTestHelpers"
            ]
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: [
                "SwiftLintFramework",
                "SwiftLintTestHelpers"
            ]
        ),
        .testTarget(
            name: "ExtraRulesTests",
            dependencies: [
                "SwiftLintFramework",
                "SwiftLintTestHelpers"
            ]
        ),
        .binaryTarget(
            name: "SwiftLintBinary",
            path: "SwiftLintBinary.artifactbundle"
        )
    ]
)
