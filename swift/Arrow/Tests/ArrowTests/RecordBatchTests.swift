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

final class RecordBatchTests: XCTestCase {
    func testRecordBatch() throws {
        let uint8Builder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        uint8Builder.append(10)
        uint8Builder.append(22)
        uint8Builder.append(nil)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        stringBuilder.append("test10")
        stringBuilder.append("test22")
        stringBuilder.append("test33")

        let intHolder = ArrowArrayHolder(try uint8Builder.finish())
        let stringHolder = ArrowArrayHolder(try stringBuilder.finish())
        let result = RecordBatch.Builder()
            .addColumn("col1", arrowArray: intHolder)
            .addColumn("col2", arrowArray: stringHolder)
            .finish()
        switch result {
        case .success(let recordBatch):
            let schema = recordBatch.schema
            XCTAssertEqual(schema.fields.count, 2)
            XCTAssertEqual(schema.fields[0].name, "col1")
            XCTAssertEqual(schema.fields[0].type.info, ArrowType.ArrowUInt8)
            XCTAssertEqual(schema.fields[0].isNullable, true)
            XCTAssertEqual(schema.fields[1].name, "col2")
            XCTAssertEqual(schema.fields[1].type.info, ArrowType.ArrowString)
            XCTAssertEqual(schema.fields[1].isNullable, false)
            XCTAssertEqual(recordBatch.columns.count, 2)
            let col1: ArrowArray<UInt8> = recordBatch.data(for: 0)
            let col2: ArrowArray<String> = recordBatch.data(for: 1)
            XCTAssertEqual(col1.length, 3)
            XCTAssertEqual(col2.length, 3)
            XCTAssertEqual(col1.nullCount, 1)
        case .failure(let error):
            throw error
        }
    }
}
