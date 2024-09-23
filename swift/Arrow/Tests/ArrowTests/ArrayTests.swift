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

final class ArrayTests: XCTestCase { // swiftlint:disable:this type_body_length
    func testPrimitiveArray() throws {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        let arrayBuilder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        for index in 0..<100 {
            arrayBuilder.append(UInt8(index))
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

        let doubleBuilder: NumberArrayBuilder<Double> = try ArrowArrayBuilders.loadNumberArrayBuilder()
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
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        for index in 0..<100 {
            if index % 10 == 9 {
                stringBuilder.append(nil)
            } else {
                stringBuilder.append("test" + String(index))
            }
        }

        XCTAssertEqual(stringBuilder.nullCount, 10)
        XCTAssertEqual(stringBuilder.length, 100)
        XCTAssertEqual(stringBuilder.capacity, 648)
        let stringArray = try stringBuilder.finish()
        XCTAssertEqual(stringArray.length, 100)
        for index in 0..<stringArray.length {
            if index % 10 == 9 {
                XCTAssertEqual(try stringArray.isNull(index), true)
            } else {
                XCTAssertEqual(stringArray[index]!, "test" + String(index))
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
        let date32Builder: Date32ArrayBuilder = try ArrowArrayBuilders.loadDate32ArrayBuilder()
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
        let date64Builder: Date64ArrayBuilder = try ArrowArrayBuilders.loadDate64ArrayBuilder()
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

    func testBinaryArray() throws {
        let binaryBuilder = try ArrowArrayBuilders.loadBinaryArrayBuilder()
        for index in 0..<100 {
            if index % 10 == 9 {
                binaryBuilder.append(nil)
            } else {
                binaryBuilder.append(("test" + String(index)).data(using: .utf8))
            }
        }

        XCTAssertEqual(binaryBuilder.nullCount, 10)
        XCTAssertEqual(binaryBuilder.length, 100)
        XCTAssertEqual(binaryBuilder.capacity, 648)
        let binaryArray = try binaryBuilder.finish()
        XCTAssertEqual(binaryArray.length, 100)
        for index in 0..<binaryArray.length {
            if index % 10 == 9 {
                XCTAssertEqual(try binaryArray.isNull(index), true)
            } else {
                let stringData = String(bytes: binaryArray[index]!, encoding: .utf8)
                XCTAssertEqual(stringData, "test" + String(index))
            }
        }
    }

    func testTime32Array() throws {
        let milliBuilder = try ArrowArrayBuilders.loadTime32ArrayBuilder(.milliseconds)
        milliBuilder.append(100)
        milliBuilder.append(1000000)
        milliBuilder.append(nil)
        XCTAssertEqual(milliBuilder.nullCount, 1)
        XCTAssertEqual(milliBuilder.length, 3)
        XCTAssertEqual(milliBuilder.capacity, 136)
        let milliArray = try milliBuilder.finish()
        let milliType = milliArray.arrowData.type as! ArrowTypeTime32 // swiftlint:disable:this force_cast
        XCTAssertEqual(milliType.unit, .milliseconds)
        XCTAssertEqual(milliArray.length, 3)
        XCTAssertEqual(milliArray[1], 1000000)
        XCTAssertEqual(milliArray[2], nil)

        let secBuilder = try ArrowArrayBuilders.loadTime32ArrayBuilder(.seconds)
        secBuilder.append(200)
        secBuilder.append(nil)
        secBuilder.append(2000011)
        XCTAssertEqual(secBuilder.nullCount, 1)
        XCTAssertEqual(secBuilder.length, 3)
        XCTAssertEqual(secBuilder.capacity, 136)
        let secArray = try secBuilder.finish()
        let secType = secArray.arrowData.type as! ArrowTypeTime32 // swiftlint:disable:this force_cast
        XCTAssertEqual(secType.unit, .seconds)
        XCTAssertEqual(secArray.length, 3)
        XCTAssertEqual(secArray[1], nil)
        XCTAssertEqual(secArray[2], 2000011)
    }

    func testTime64Array() throws {
        let nanoBuilder = try ArrowArrayBuilders.loadTime64ArrayBuilder(.nanoseconds)
        nanoBuilder.append(10000)
        nanoBuilder.append(nil)
        nanoBuilder.append(123456789)
        XCTAssertEqual(nanoBuilder.nullCount, 1)
        XCTAssertEqual(nanoBuilder.length, 3)
        XCTAssertEqual(nanoBuilder.capacity, 264)
        let nanoArray = try nanoBuilder.finish()
        let nanoType = nanoArray.arrowData.type as! ArrowTypeTime64 // swiftlint:disable:this force_cast
        XCTAssertEqual(nanoType.unit, .nanoseconds)
        XCTAssertEqual(nanoArray.length, 3)
        XCTAssertEqual(nanoArray[1], nil)
        XCTAssertEqual(nanoArray[2], 123456789)

        let microBuilder = try ArrowArrayBuilders.loadTime64ArrayBuilder(.microseconds)
        microBuilder.append(nil)
        microBuilder.append(20000)
        microBuilder.append(987654321)
        XCTAssertEqual(microBuilder.nullCount, 1)
        XCTAssertEqual(microBuilder.length, 3)
        XCTAssertEqual(microBuilder.capacity, 264)
        let microArray = try microBuilder.finish()
        let microType = microArray.arrowData.type as! ArrowTypeTime64 // swiftlint:disable:this force_cast
        XCTAssertEqual(microType.unit, .microseconds)
        XCTAssertEqual(microArray.length, 3)
        XCTAssertEqual(microArray[1], 20000)
        XCTAssertEqual(microArray[2], 987654321)
    }

    func testStructArray() throws { // swiftlint:disable:this function_body_length
        class StructTest {
            var fieldBool: Bool = false
            var fieldInt8: Int8 = 0
            var fieldInt16: Int16 = 0
            var fieldInt32: Int32 = 0
            var fieldInt64: Int64 = 0
            var fieldUInt8: UInt8 = 0
            var fieldUInt16: UInt16 = 0
            var fieldUInt32: UInt32 = 0
            var fieldUInt64: UInt64 = 0
            var fieldDouble: Double = 0
            var fieldFloat: Float = 0
            var fieldString: String = ""
            var fieldData = Data()
            var fieldDate: Date = Date.now
        }

        enum STIndex: Int {
            case bool, int8, int16, int32, int64
            case uint8, uint16, uint32, uint64, double
            case float, string, data, date
        }

        let testData = StructTest()
        let dateNow = Date.now
        let structBuilder = try ArrowArrayBuilders.loadStructArrayBuilderForType(testData)
        structBuilder.append([true, Int8(1), Int16(2), Int32(3), Int64(4),
                              UInt8(5), UInt16(6), UInt32(7), UInt64(8), Double(9.9),
                              Float(10.10), "11", Data("12".utf8), dateNow])
        structBuilder.append(nil)
        structBuilder.append([true, Int8(13), Int16(14), Int32(15), Int64(16),
                              UInt8(17), UInt16(18), UInt32(19), UInt64(20), Double(21.21),
                              Float(22.22), "23", Data("24".utf8), dateNow])
        XCTAssertEqual(structBuilder.length, 3)
        let structArray = try structBuilder.finish()
        XCTAssertEqual(structArray.length, 3)
        XCTAssertNil(structArray[1])
        XCTAssertEqual(structArray.arrowFields![0].length, 3)
        XCTAssertNil(structArray.arrowFields![0].array.asAny(1))
        XCTAssertEqual(structArray[0]![STIndex.bool.rawValue] as? Bool, true)
        XCTAssertEqual(structArray[0]![STIndex.int8.rawValue] as? Int8, 1)
        XCTAssertEqual(structArray[0]![STIndex.int16.rawValue] as? Int16, 2)
        XCTAssertEqual(structArray[0]![STIndex.int32.rawValue] as? Int32, 3)
        XCTAssertEqual(structArray[0]![STIndex.int64.rawValue] as? Int64, 4)
        XCTAssertEqual(structArray[0]![STIndex.uint8.rawValue] as? UInt8, 5)
        XCTAssertEqual(structArray[0]![STIndex.uint16.rawValue] as? UInt16, 6)
        XCTAssertEqual(structArray[0]![STIndex.uint32.rawValue] as? UInt32, 7)
        XCTAssertEqual(structArray[0]![STIndex.uint64.rawValue] as? UInt64, 8)
        XCTAssertEqual(structArray[0]![STIndex.double.rawValue] as? Double, 9.9)
        XCTAssertEqual(structArray[0]![STIndex.float.rawValue] as? Float, 10.10)
        XCTAssertEqual(structArray[2]![STIndex.string.rawValue] as? String, "23")
        XCTAssertEqual(
            String(decoding: (structArray[0]![STIndex.data.rawValue] as? Data)!, as: UTF8.self), "12")
        let dateFormatter = DateFormatter()
        dateFormatter.timeStyle = .full
        XCTAssertTrue(
            dateFormatter.string(from: (structArray[0]![STIndex.date.rawValue] as? Date)!) ==
            dateFormatter.string(from: dateNow))
    }

    func checkHolderForType(_ checkType: ArrowType) throws {
        let buffers = [ArrowBuffer(length: 0, capacity: 0,
                                rawPointer: UnsafeMutableRawPointer.allocate(byteCount: 0, alignment: .zero)),
                       ArrowBuffer(length: 0, capacity: 0,
                               rawPointer: UnsafeMutableRawPointer.allocate(byteCount: 0, alignment: .zero))]
        let field = ArrowField("", type: checkType, isNullable: true)
        switch makeArrayHolder(field, buffers: buffers, nullCount: 0, children: nil, rbLength: 0) {
        case .success(let holder):
            XCTAssertEqual(holder.type.id, checkType.id)
        case .failure(let err):
            throw err
        }
    }

    func testArrayHolders() throws {
        try checkHolderForType(ArrowType(ArrowType.ArrowInt8))
        try checkHolderForType(ArrowType(ArrowType.ArrowUInt8))
        try checkHolderForType(ArrowType(ArrowType.ArrowInt16))
        try checkHolderForType(ArrowType(ArrowType.ArrowUInt16))
        try checkHolderForType(ArrowType(ArrowType.ArrowInt32))
        try checkHolderForType(ArrowType(ArrowType.ArrowUInt32))
        try checkHolderForType(ArrowType(ArrowType.ArrowInt64))
        try checkHolderForType(ArrowType(ArrowType.ArrowUInt64))
        try checkHolderForType(ArrowTypeTime32(.seconds))
        try checkHolderForType(ArrowTypeTime32(.milliseconds))
        try checkHolderForType(ArrowTypeTime64(.microseconds))
        try checkHolderForType(ArrowTypeTime64(.nanoseconds))
        try checkHolderForType(ArrowType(ArrowType.ArrowBinary))
        try checkHolderForType(ArrowType(ArrowType.ArrowFloat))
        try checkHolderForType(ArrowType(ArrowType.ArrowDouble))
        try checkHolderForType(ArrowType(ArrowType.ArrowBool))
        try checkHolderForType(ArrowType(ArrowType.ArrowString))
    }

    func testArrowArrayHolderBuilder() throws {
        let uint8HBuilder: ArrowArrayHolderBuilder =
            (try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<UInt8>)
        for index in 0..<100 {
            uint8HBuilder.appendAny(UInt8(index))
        }

        let uint8Holder = try uint8HBuilder.toHolder()
        XCTAssertEqual(uint8Holder.nullCount, 0)
        XCTAssertEqual(uint8Holder.length, 100)

        let stringHBuilder: ArrowArrayHolderBuilder =
            (try ArrowArrayBuilders.loadStringArrayBuilder())
         for index in 0..<100 {
             if index % 10 == 9 {
                 stringHBuilder.appendAny(nil)
             } else {
                 stringHBuilder.appendAny("test" + String(index))
             }
         }

        let stringHolder = try stringHBuilder.toHolder()
        XCTAssertEqual(stringHolder.nullCount, 10)
        XCTAssertEqual(stringHolder.length, 100)
    }

    func testAddVArgs() throws {
        let arrayBuilder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        arrayBuilder.append(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
        XCTAssertEqual(arrayBuilder.length, 10)
        XCTAssertEqual(try arrayBuilder.finish()[2], 2)
        let doubleBuilder: NumberArrayBuilder<Double> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        doubleBuilder.append(0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8)
        XCTAssertEqual(doubleBuilder.length, 9)
        XCTAssertEqual(try doubleBuilder.finish()[4], 4.4)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        stringBuilder.append("0", "1", "2", "3", "4", "5", "6")
        XCTAssertEqual(stringBuilder.length, 7)
        XCTAssertEqual(try stringBuilder.finish()[4], "4")
        let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
        boolBuilder.append(true, false, true, false)
        XCTAssertEqual(try boolBuilder.finish()[2], true)
    }

    func testAddArray() throws {
        let arrayBuilder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        arrayBuilder.append([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        XCTAssertEqual(arrayBuilder.length, 10)
        XCTAssertEqual(try arrayBuilder.finish()[2], 2)
        let doubleBuilder: NumberArrayBuilder<Double> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        doubleBuilder.append([0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8])
        XCTAssertEqual(doubleBuilder.length, 9)
        XCTAssertEqual(try doubleBuilder.finish()[4], 4.4)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        stringBuilder.append(["0", "1", "2", "3", "4", "5", "6"])
        XCTAssertEqual(stringBuilder.length, 7)
        XCTAssertEqual(try stringBuilder.finish()[4], "4")
        let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
        boolBuilder.append([true, false, true, false])
        XCTAssertEqual(try boolBuilder.finish()[2], true)
    }
}
