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

final class ArrayTests: XCTestCase {
    func testPrimitiveArray() throws {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        let arrayBuilder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder();
        for i in 0..<100 {
            arrayBuilder.append(UInt8(i))
        }
        
        XCTAssertEqual(arrayBuilder.nullCount, 0)
        arrayBuilder.append(nil)
        XCTAssertEqual(arrayBuilder.length, 101)
        XCTAssertEqual(arrayBuilder.capacity, 136)
        XCTAssertEqual(arrayBuilder.nullCount, 1)
        let array = try arrayBuilder.finish()
        XCTAssertEqual(array.length, 101)
        XCTAssertEqual(array[1]!, 1)
        XCTAssertEqual(array[10]!, 10)
        XCTAssertEqual(try array.isNull(100), true)
        
        let doubleBuilder: NumberArrayBuilder<Double> = try ArrowArrayBuilders.loadNumberArrayBuilder();
        doubleBuilder.append(14)
        doubleBuilder.append(40.4)
        XCTAssertEqual(doubleBuilder.nullCount, 0)
        XCTAssertEqual(doubleBuilder.length, 2)
        XCTAssertEqual(doubleBuilder.capacity, 264)
        let doubleArray = try doubleBuilder.finish()
        XCTAssertEqual(doubleArray.length, 2)
        XCTAssertEqual(doubleArray[0]!, 14)
        XCTAssertEqual(doubleArray[1]!, 40.4)
    }
    
    func testStringArray() throws {
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder();
        for i in 0..<100 {
            if i % 10 == 9 {
                stringBuilder.append(nil)
            } else {
                stringBuilder.append("test" + String(i))
            }
        }
        XCTAssertEqual(stringBuilder.nullCount, 10)
        XCTAssertEqual(stringBuilder.length, 100)
        XCTAssertEqual(stringBuilder.capacity, 648)
        let stringArray = try stringBuilder.finish()
        XCTAssertEqual(stringArray.length, 100)
        for i in 0..<stringArray.length {
            if i % 10 == 9 {
                XCTAssertEqual(try stringArray.isNull(i), true)
            } else {
                XCTAssertEqual(stringArray[i]!, "test" + String(i))
            }
        }
        
        XCTAssertEqual(stringArray[1]!, "test1")
        XCTAssertEqual(stringArray[0]!, "test0")
    }
    
    func testBoolArray() throws {
        let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
        boolBuilder.append(true)
        boolBuilder.append(nil)
        boolBuilder.append(false)
        boolBuilder.append(false)
        XCTAssertEqual(boolBuilder.nullCount, 1)
        XCTAssertEqual(boolBuilder.length, 4)
        XCTAssertEqual(boolBuilder.capacity, 72)
        let boolArray = try boolBuilder.finish()
        XCTAssertEqual(boolArray.length, 4)
        XCTAssertEqual(boolArray[1], nil)
        XCTAssertEqual(boolArray[0]!, true)
        XCTAssertEqual(boolArray[2]!, false)
        
    }
    
    func testDate32Array() throws {
        let date32Builder: Date32ArrayBuilder = try ArrowArrayBuilders.loadDate32ArrayBuilder();
        let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
        let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
        date32Builder.append(date1)
        date32Builder.append(date2)
        date32Builder.append(nil)
        XCTAssertEqual(date32Builder.nullCount, 1)
        XCTAssertEqual(date32Builder.length, 3)
        XCTAssertEqual(date32Builder.capacity, 136)
        let date32Array = try date32Builder.finish()
        XCTAssertEqual(date32Array.length, 3)
        XCTAssertEqual(date32Array[1], date2)
        let adjustedDate1 = Date(timeIntervalSince1970: date1.timeIntervalSince1970 - 352)
        XCTAssertEqual(date32Array[0]!, adjustedDate1)
    }
    
    func testDate64Array() throws {
        let date64Builder: Date64ArrayBuilder = try ArrowArrayBuilders.loadDate64ArrayBuilder();
        let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
        let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
        date64Builder.append(date1)
        date64Builder.append(date2)
        date64Builder.append(nil)
        XCTAssertEqual(date64Builder.nullCount, 1)
        XCTAssertEqual(date64Builder.length, 3)
        XCTAssertEqual(date64Builder.capacity, 264)
        let date64Array = try date64Builder.finish()
        XCTAssertEqual(date64Array.length, 3)
        XCTAssertEqual(date64Array[1], date2)
        XCTAssertEqual(date64Array[0]!, date1)
        
    }
}
