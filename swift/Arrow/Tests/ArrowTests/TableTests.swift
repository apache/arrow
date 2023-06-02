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
        let schemaBuilder = ArrowSchema.Builder();
        let schema = schemaBuilder.addField("col1", type: ArrowType.ArrowInt8, isNullable: true)
            .addField("col2", type: ArrowType.ArrowBool, isNullable: false)
            .finish()
        XCTAssertEqual(schema.fields.count, 2)
        XCTAssertEqual(schema.fields[0].name, "col1")
        XCTAssertEqual(schema.fields[0].type, ArrowType.ArrowInt8)
        XCTAssertEqual(schema.fields[0].isNullable, true)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type, ArrowType.ArrowBool)
        XCTAssertEqual(schema.fields[1].isNullable, false)
    }
    
    func testTable() throws {
        let uint8Builder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder();
        uint8Builder.append(10)
        uint8Builder.append(22)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder();
        stringBuilder.append("test10")
        stringBuilder.append("test22")
        let date32Builder: Date32ArrayBuilder = try ArrowArrayBuilders.loadDate32ArrayBuilder();
        let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
        let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
        date32Builder.append(date1)
        date32Builder.append(date2)

        let table = try ArrowTable.Builder()
            .addColumn("col1", arrowArray: uint8Builder.finish())
            .addColumn("col2", arrowArray: stringBuilder.finish())
            .addColumn("col3", arrowArray: date32Builder.finish())
            .finish();
        
        let schema = table.schema
        XCTAssertEqual(schema.fields.count, 3)
        XCTAssertEqual(schema.fields[0].name, "col1")
        XCTAssertEqual(schema.fields[0].type, ArrowType.ArrowUInt8)
        XCTAssertEqual(schema.fields[0].isNullable, false)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type, ArrowType.ArrowString)
        XCTAssertEqual(schema.fields[1].isNullable, false)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type, ArrowType.ArrowString)
        XCTAssertEqual(schema.fields[1].isNullable, false)
        XCTAssertEqual(table.columns.count, 3)
        let col1: ChunkedArray<UInt8> = table.columns[0].data();
        let col2: ChunkedArray<String> = table.columns[1].data();
        let col3: ChunkedArray<Date> = table.columns[2].data();
        XCTAssertEqual(col1.length, 2)
        XCTAssertEqual(col2.length, 2)
        XCTAssertEqual(col3.length, 2)
    }

    func testTableWithChunkedData() throws {
        let uint8Builder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder();
        uint8Builder.append(10)
        uint8Builder.append(22)
        let uint8Builder2: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder();
        uint8Builder2.append(33)
        let uint8Builder3: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder();
        uint8Builder3.append(44)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder();
        stringBuilder.append("test10")
        stringBuilder.append("test22")
        let stringBuilder2 = try ArrowArrayBuilders.loadStringArrayBuilder();
        stringBuilder.append("test33")
        stringBuilder.append("test44")
        
        let date32Builder: Date32ArrayBuilder = try ArrowArrayBuilders.loadDate32ArrayBuilder();
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
            .finish();
        
        let schema = table.schema
        XCTAssertEqual(schema.fields.count, 3)
        XCTAssertEqual(schema.fields[0].name, "col1")
        XCTAssertEqual(schema.fields[0].type, ArrowType.ArrowUInt8)
        XCTAssertEqual(schema.fields[0].isNullable, false)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type, ArrowType.ArrowString)
        XCTAssertEqual(schema.fields[1].isNullable, false)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].type, ArrowType.ArrowString)
        XCTAssertEqual(schema.fields[1].isNullable, false)
        XCTAssertEqual(table.columns.count, 3)
        let col1: ChunkedArray<UInt8> = table.columns[0].data();
        let col2: ChunkedArray<String> = table.columns[1].data();
        let col3: ChunkedArray<Date> = table.columns[2].data();
        XCTAssertEqual(col1.length, 4)
        XCTAssertEqual(col2.length, 4)
        XCTAssertEqual(col3.length, 4)
        XCTAssertEqual(col1.asString(0), "10")
        XCTAssertEqual(col1.asString(3), "44")
        XCTAssertEqual(col2.asString(0), "test10")
        XCTAssertEqual(col2.asString(2), "test33")
    }

    func testTableToRecordBatch() throws {
        let uint8Builder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder();
        uint8Builder.append(10)
        uint8Builder.append(22)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder();
        stringBuilder.append("test10")
        stringBuilder.append("test22")
        
        let intHolder = ArrowArrayHolder(try uint8Builder.finish())
        let stringHolder = ArrowArrayHolder(try stringBuilder.finish())
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
            XCTAssertEqual(schema.fields[0].type, ArrowType.ArrowUInt8)
            XCTAssertEqual(schema.fields[0].isNullable, false)
            XCTAssertEqual(schema.fields[1].name, "col2")
            XCTAssertEqual(schema.fields[1].type, ArrowType.ArrowString)
            XCTAssertEqual(schema.fields[1].isNullable, false)
            XCTAssertEqual(table.columns.count, 2)
            let col1: ChunkedArray<UInt8> = table.columns[0].data();
            let col2: ChunkedArray<String> = table.columns[1].data();
            XCTAssertEqual(col1.length, 2)
            XCTAssertEqual(col2.length, 2)
        case .failure(let error):
            throw error
        }
    }
}
