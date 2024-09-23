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

import XCTest
@testable import Arrow

final class ArrayBuilderTests: XCTestCase {
    func testIsValidTypeForBuilder() throws {
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(UInt8.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Int16.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Int32.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Int64.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(UInt8.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(UInt16.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(UInt32.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(UInt64.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Float.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Double.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Date.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Bool.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Int8?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Int16?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Int32?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Int64?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(UInt8?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(UInt16?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(UInt32?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(UInt64?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Float?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Double?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Date?.self))
        XCTAssertTrue(ArrowArrayBuilders.isValidBuilderType(Bool?.self))

        XCTAssertFalse(ArrowArrayBuilders.isValidBuilderType(Int.self))
        XCTAssertFalse(ArrowArrayBuilders.isValidBuilderType(UInt.self))
        XCTAssertFalse(ArrowArrayBuilders.isValidBuilderType(Int?.self))
        XCTAssertFalse(ArrowArrayBuilders.isValidBuilderType(UInt?.self))
    }

    func testLoadArrayBuilders() throws {
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Int8.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Int16.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Int32.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Int64.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(UInt8.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(UInt16.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(UInt32.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(UInt64.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Float.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Double.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Date.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Bool.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Int8?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Int16?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Int32?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Int64?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(UInt8?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(UInt16?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(UInt32?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(UInt64?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Float?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Double?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Date?.self))
        XCTAssertNotNil(try ArrowArrayBuilders.loadBuilder(Bool?.self))

        XCTAssertThrowsError(try ArrowArrayBuilders.loadBuilder(Int.self))
        XCTAssertThrowsError(try ArrowArrayBuilders.loadBuilder(UInt.self))
        XCTAssertThrowsError(try ArrowArrayBuilders.loadBuilder(Int?.self))
        XCTAssertThrowsError(try ArrowArrayBuilders.loadBuilder(UInt?.self))
    }
}
