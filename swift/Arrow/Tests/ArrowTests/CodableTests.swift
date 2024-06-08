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

final class CodableTests: XCTestCase {
    public class TestClass: Codable {
        public var propBool: Bool
        public var propInt8: Int8
        public var propInt16: Int16
        public var propInt32: Int32
        public var propInt64: Int64
        public var propUInt8: UInt8
        public var propUInt16: UInt16
        public var propUInt32: UInt32
        public var propUInt64: UInt64
        public var propFloat: Float
        public var propDouble: Double
        public var propString: String
        public var propDate: Date

        public required init() {
            self.propBool = false
            self.propInt8 = 1
            self.propInt16 = 2
            self.propInt32 = 3
            self.propInt64 = 4
            self.propUInt8 = 5
            self.propUInt16 = 6
            self.propUInt32 = 7
            self.propUInt64 = 8
            self.propFloat = 9
            self.propDouble = 10
            self.propString = "11"
            self.propDate = Date.now
        }
    }

    func testArrowKeyedDecoder() throws { // swiftlint:disable:this function_body_length
        let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)

        let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
        let int8Builder: NumberArrayBuilder<Int8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let int16Builder: NumberArrayBuilder<Int16> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let int32Builder: NumberArrayBuilder<Int32> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let int64Builder: NumberArrayBuilder<Int64> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let uint8Builder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let uint16Builder: NumberArrayBuilder<UInt16> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let uint32Builder: NumberArrayBuilder<UInt32> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let uint64Builder: NumberArrayBuilder<UInt64> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let floatBuilder: NumberArrayBuilder<Float> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let doubleBuilder: NumberArrayBuilder<Double> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        let dateBuilder = try ArrowArrayBuilders.loadDate64ArrayBuilder()

        boolBuilder.append(false, true, false)
        int8Builder.append(10, 11, 12)
        int16Builder.append(20, 21, 22)
        int32Builder.append(30, 31, 32)
        int64Builder.append(40, 41, 42)
        uint8Builder.append(50, 51, 52)
        uint16Builder.append(60, 61, 62)
        uint32Builder.append(70, 71, 72)
        uint64Builder.append(80, 81, 82)
        floatBuilder.append(90.1, 91.1, 92.1)
        doubleBuilder.append(100.1, 101.1, 102.1)
        stringBuilder.append("test0", "test1", "test2")
        dateBuilder.append(date1, date1, date1)
        let result = RecordBatch.Builder()
            .addColumn("propBool", arrowArray: try boolBuilder.toHolder())
            .addColumn("propInt8", arrowArray: try int8Builder.toHolder())
            .addColumn("propInt16", arrowArray: try int16Builder.toHolder())
            .addColumn("propInt32", arrowArray: try int32Builder.toHolder())
            .addColumn("propInt64", arrowArray: try int64Builder.toHolder())
            .addColumn("propUInt8", arrowArray: try uint8Builder.toHolder())
            .addColumn("propUInt16", arrowArray: try uint16Builder.toHolder())
            .addColumn("propUInt32", arrowArray: try uint32Builder.toHolder())
            .addColumn("propUInt64", arrowArray: try uint64Builder.toHolder())
            .addColumn("propFloat", arrowArray: try floatBuilder.toHolder())
            .addColumn("propDouble", arrowArray: try doubleBuilder.toHolder())
            .addColumn("propString", arrowArray: try stringBuilder.toHolder())
            .addColumn("propDate", arrowArray: try dateBuilder.toHolder())
            .finish()
        switch result {
        case .success(let rb):
            let decoder = ArrowDecoder(rb)
            var testClasses = try decoder.decode(TestClass.self)
            for index in 0..<testClasses.count {
                let testClass = testClasses[index]
                var col = 0
                XCTAssertEqual(testClass.propBool, index % 2 == 0 ? false : true)
                XCTAssertEqual(testClass.propInt8, Int8(index + 10))
                XCTAssertEqual(testClass.propInt16, Int16(index + 20))
                XCTAssertEqual(testClass.propInt32, Int32(index + 30))
                XCTAssertEqual(testClass.propInt64, Int64(index + 40))
                XCTAssertEqual(testClass.propUInt8, UInt8(index + 50))
                XCTAssertEqual(testClass.propUInt16, UInt16(index + 60))
                XCTAssertEqual(testClass.propUInt32, UInt32(index + 70))
                XCTAssertEqual(testClass.propUInt64, UInt64(index + 80))
                XCTAssertEqual(testClass.propFloat, Float(index) + 90.1)
                XCTAssertEqual(testClass.propDouble, Double(index) + 100.1)
                XCTAssertEqual(testClass.propString, "test\(index)")
                XCTAssertEqual(testClass.propDate, date1)
            }
        case .failure(let err):
            throw err
        }
    }

    func testArrowSingleDecoder() throws {
        let int8Builder: NumberArrayBuilder<Int8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        int8Builder.append(10, 11, 12, nil)
        let result = RecordBatch.Builder()
            .addColumn("propInt8", arrowArray: try int8Builder.toHolder())
            .finish()
        switch result {
        case .success(let rb):
            let decoder = ArrowDecoder(rb)
            let testData = try decoder.decode(Int8?.self)
            for index in 0..<testData.count {
                let val: Int8? = testData[index]
                if val != nil {
                    XCTAssertEqual(val!, Int8(index + 10))
                }
            }
        case .failure(let err):
            throw err
        }
    }

    func testArrowUnkeyedDecoder() throws {
        let int8Builder: NumberArrayBuilder<Int8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        int8Builder.append(10, 11, 12)
        stringBuilder.append("test0", "test1", "test2")
        let result = RecordBatch.Builder()
            .addColumn("propInt8", arrowArray: try int8Builder.toHolder())
            .addColumn("propString", arrowArray: try stringBuilder.toHolder())
            .finish()
        switch result {
        case .success(let rb):
            let decoder = ArrowDecoder(rb)
            let testData = try decoder.decode([Int8: String].self)
            var index: Int8 = 0
            for data in testData {
                let str = data[10 + index]
                XCTAssertEqual(str, "test\(index)")
                index += 1
            }
        case .failure(let err):
            throw err
        }
    }

}
