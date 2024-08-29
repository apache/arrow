// swift-tools-version:5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import PackageDescription

let package = Package(
    name: "ArrowFlight",
    platforms: [
        .macOS(.v10_15)
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "ArrowFlight",
            targets: ["ArrowFlight"]),
    ],
    dependencies: [
        .package(url: "https://github.com/grpc/grpc-swift.git", from: "1.15.0"),
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.6.0"),
        .package(path: "../Arrow")
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "ArrowFlight",
            dependencies: [
                .product(name: "Arrow", package: "Arrow"),
                .product(name: "GRPC", package: "grpc-swift"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf")
            ]),
        .testTarget(
            name: "ArrowFlightTests",
            dependencies: ["ArrowFlight"]),
    ]
)
