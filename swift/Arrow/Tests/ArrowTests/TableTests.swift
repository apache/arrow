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

final class TableTests: XCTestCase {
    func testSchema() throws {
        let schemaBuilder = ArrowSchema.Builder()
        let schema = schemaBuilder.addField("col1", type: ArrowType(ArrowType.ArrowInt8), isNullable: true)
            .addField("col2", type: ArrowType(ArrowType.ArrowBool), isNullable: false)
            .finish()
        XCTAssertEqual(schema.fields.count, 2)
        XCTAssertEqual(schema.fields[0].name, "col1")
        XCTAssertEqual(schema.fields[0].type.info, ArrowType.ArrowInt8)
        XCTAssertEqual(schema.fields[0].isNullable, true)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type.info, ArrowType.ArrowBool)
        XCTAssertEqual(schema.fields[1].isNullable, false)
    }

    func testSchemaNested() {
        class StructTest {
            var field0: Bool = false
            var field1: Int8 = 0
            var field2: Int16 = 0
            var field3: Int32 = 0
            var field4: Int64 = 0
            var field5: UInt8 = 0
            var field6: UInt16 = 0
            var field7: UInt32 = 0
            var field8: UInt64 = 0
            var field9: Double = 0
            var field10: Float = 0
            var field11: String = ""
            var field12 = Data()
            var field13: Date = Date.now
        }

        let testObj = StructTest()
        var fields = [ArrowField]()
        let buildStructType = {() -> ArrowNestedType in
            let mirror = Mirror(reflecting: testObj)
            for (property, value) in mirror.children {
                let arrowType = ArrowType(ArrowType.infoForType(type(of: value)))
                fields.append(ArrowField(property!, type: arrowType, isNullable: true))
            }

            return ArrowNestedType(ArrowType.ArrowStruct, fields: fields)
        }

        let structType = buildStructType()
        XCTAssertEqual(structType.id, ArrowTypeId.strct)
        XCTAssertEqual(structType.fields.count, 14)
        XCTAssertEqual(structType.fields[0].type.id, ArrowTypeId.boolean)
        XCTAssertEqual(structType.fields[1].type.id, ArrowTypeId.int8)
        XCTAssertEqual(structType.fields[2].type.id, ArrowTypeId.int16)
        XCTAssertEqual(structType.fields[3].type.id, ArrowTypeId.int32)
        XCTAssertEqual(structType.fields[4].type.id, ArrowTypeId.int64)
        XCTAssertEqual(structType.fields[5].type.id, ArrowTypeId.uint8)
        XCTAssertEqual(structType.fields[6].type.id, ArrowTypeId.uint16)
        XCTAssertEqual(structType.fields[7].type.id, ArrowTypeId.uint32)
        XCTAssertEqual(structType.fields[8].type.id, ArrowTypeId.uint64)
        XCTAssertEqual(structType.fields[9].type.id, ArrowTypeId.double)
        XCTAssertEqual(structType.fields[10].type.id, ArrowTypeId.float)
        XCTAssertEqual(structType.fields[11].type.id, ArrowTypeId.string)
        XCTAssertEqual(structType.fields[12].type.id, ArrowTypeId.binary)
        XCTAssertEqual(structType.fields[13].type.id, ArrowTypeId.date64)
    }

    func testTable() throws {
        let doubleBuilder: NumberArrayBuilder<Double> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        doubleBuilder.append(11.11)
        doubleBuilder.append(22.22)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        stringBuilder.append("test10")
        stringBuilder.append("test22")
        let date32Builder: Date32ArrayBuilder = try ArrowArrayBuilders.loadDate32ArrayBuilder()
        let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
        let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
        date32Builder.append(date1)
        date32Builder.append(date2)
        let table = try ArrowTable.Builder()
            .addColumn("col1", arrowArray: doubleBuilder.finish())
            .addColumn("col2", arrowArray: stringBuilder.finish())
            .addColumn("col3", arrowArray: date32Builder.finish())
            .finish()
        let schema = table.schema
        XCTAssertEqual(schema.fields.count, 3)
        XCTAssertEqual(schema.fields[0].name, "col1")
        XCTAssertEqual(schema.fields[0].type.info, ArrowType.ArrowDouble)
        XCTAssertEqual(schema.fields[0].isNullable, false)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type.info, ArrowType.ArrowString)
        XCTAssertEqual(schema.fields[1].isNullable, false)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type.info, ArrowType.ArrowString)
        XCTAssertEqual(schema.fields[1].isNullable, false)
        XCTAssertEqual(table.columns.count, 3)
        let col1: ChunkedArray<Double> = table.columns[0].data()
        let col2: ChunkedArray<String> = table.columns[1].data()
        let col3: ChunkedArray<Date> = table.columns[2].data()
        XCTAssertEqual(col1.length, 2)
        XCTAssertEqual(col2.length, 2)
        XCTAssertEqual(col3.length, 2)
        XCTAssertEqual(col1[0], 11.11)
        XCTAssertEqual(col2[1], "test22")
    }

    func testTableWithChunkedData() throws {
        let uint8Builder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        uint8Builder.append(10)
        uint8Builder.append(22)
        let uint8Builder2: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        uint8Builder2.append(33)
        let uint8Builder3: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        uint8Builder3.append(44)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        stringBuilder.append("test10")
        stringBuilder.append("test22")
        let stringBuilder2 = try ArrowArrayBuilders.loadStringArrayBuilder()
        stringBuilder.append("test33")
        stringBuilder.append("test44")
        let date32Builder: Date32ArrayBuilder = try ArrowArrayBuilders.loadDate32ArrayBuilder()
        let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
        let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
        date32Builder.append(date1)
        date32Builder.append(date2)
        date32Builder.append(date1)
        date32Builder.append(date2)
        let intArray = try ChunkedArray([uint8Builder.finish(), uint8Builder2.finish(), uint8Builder3.finish()])
        let stringArray = try ChunkedArray([stringBuilder.finish(), stringBuilder2.finish()])
        let dateArray = try ChunkedArray([date32Builder.finish()])
        let table = ArrowTable.Builder()
            .addColumn("col1", chunked: intArray)
            .addColumn("col2", chunked: stringArray)
            .addColumn("col3", chunked: dateArray)
            .finish()
        let schema = table.schema
        XCTAssertEqual(schema.fields.count, 3)
        XCTAssertEqual(schema.fields[0].name, "col1")
        XCTAssertEqual(schema.fields[0].type.info, ArrowType.ArrowUInt8)
        XCTAssertEqual(schema.fields[0].isNullable, false)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type.info, ArrowType.ArrowString)
        XCTAssertEqual(schema.fields[1].isNullable, false)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type.info, ArrowType.ArrowString)
        XCTAssertEqual(schema.fields[1].isNullable, false)
        XCTAssertEqual(table.columns.count, 3)
        let col1: ChunkedArray<UInt8> = table.columns[0].data()
        let col2: ChunkedArray<String> = table.columns[1].data()
        let col3: ChunkedArray<Date> = table.columns[2].data()
        XCTAssertEqual(col1.length, 4)
        XCTAssertEqual(col2.length, 4)
        XCTAssertEqual(col3.length, 4)
        XCTAssertEqual(col1.asString(0), "10")
        XCTAssertEqual(col1.asString(3), "44")
        XCTAssertEqual(col2.asString(0), "test10")
        XCTAssertEqual(col2.asString(2), "test33")
    }

    func testTableToRecordBatch() throws {
        let uint8Builder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        uint8Builder.append(10)
        uint8Builder.append(22)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        stringBuilder.append("test10")
        stringBuilder.append("test22")
        let intHolder = ArrowArrayHolderImpl(try uint8Builder.finish())
        let stringHolder = ArrowArrayHolderImpl(try stringBuilder.finish())
        let result = RecordBatch.Builder()
            .addColumn("col1", arrowArray: intHolder)
            .addColumn("col2", arrowArray: stringHolder)
            .finish().flatMap({ rb in
                return ArrowTable.from(recordBatches: [rb])
            })
        switch result {
        case .success(let table):
            let schema = table.schema
            XCTAssertEqual(schema.fields.count, 2)
            XCTAssertEqual(schema.fields[0].name, "col1")
            XCTAssertEqual(schema.fields[0].type.info, ArrowType.ArrowUInt8)
            XCTAssertEqual(schema.fields[0].isNullable, false)
            XCTAssertEqual(schema.fields[1].name, "col2")
            XCTAssertEqual(schema.fields[1].type.info, ArrowType.ArrowString)
            XCTAssertEqual(schema.fields[1].isNullable, false)
            XCTAssertEqual(table.columns.count, 2)
            let col1: ChunkedArray<UInt8> = table.columns[0].data()
            let col2: ChunkedArray<String> = table.columns[1].data()
            XCTAssertEqual(col1.length, 2)
            XCTAssertEqual(col2.length, 2)
        case .failure(let error):
            throw error
        }
    }
}
