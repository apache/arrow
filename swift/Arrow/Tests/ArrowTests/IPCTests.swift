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
import FlatBuffers
@testable import Arrow

final class IPCTests: XCTestCase {
    func currentDirectory(path: String = #file) -> URL {
        return URL(fileURLWithPath: path).deletingLastPathComponent()
    }

    func testFileReader() throws {
        let fileURL = currentDirectory().appendingPathComponent("../../testdata_double.arrow")
        let arrowReader = ArrowReader()
        let recordBatchs = try arrowReader.fromFile(fileURL)
        XCTAssertEqual(recordBatchs.count, 1)
        for recordBatch in recordBatchs {
            XCTAssertEqual(recordBatch.length, 5)
            XCTAssertEqual(recordBatch.columns.count, 2)
            XCTAssertEqual(recordBatch.schema.fields.count, 2)
            XCTAssertEqual(recordBatch.schema.fields[0].name, "one")
            XCTAssertEqual(recordBatch.schema.fields[0].type, ArrowType.ArrowDouble)
            XCTAssertEqual(recordBatch.schema.fields[1].name, "two")
            XCTAssertEqual(recordBatch.schema.fields[1].type, ArrowType.ArrowString)
            for index in 0..<recordBatch.length {
                let column = recordBatch.columns[1]
                let str = column.holder as! AsString
                let val = "\(str.asString(index))"
                if index != 1 {
                    XCTAssertNotEqual(val, "")
                } else {
                    XCTAssertEqual(val, "")
                }
            }
        }
    }
    
    func testFileReader_bool() throws {
        let fileURL = currentDirectory().appendingPathComponent("../../testdata_bool.arrow")
        let arrowReader = ArrowReader()
        let recordBatchs = try arrowReader.fromFile(fileURL)
        XCTAssertEqual(recordBatchs.count, 1)
        for recordBatch in recordBatchs {
            XCTAssertEqual(recordBatch.length, 5)
            XCTAssertEqual(recordBatch.columns.count, 2)
            XCTAssertEqual(recordBatch.schema.fields.count, 2)
            XCTAssertEqual(recordBatch.schema.fields[0].name, "one")
            XCTAssertEqual(recordBatch.schema.fields[0].type, ArrowType.ArrowBool)
            XCTAssertEqual(recordBatch.schema.fields[1].name, "two")
            XCTAssertEqual(recordBatch.schema.fields[1].type, ArrowType.ArrowString)
            for index in 0..<recordBatch.length {
                let column = recordBatch.columns[0]
                let str = column.holder as! AsString
                let val = "\(str.asString(index))"
                if index == 0 || index == 4 {
                    XCTAssertEqual(val, "true")
                } else if index == 2 {
                    XCTAssertEqual(val, "")
                } else {
                    XCTAssertEqual(val, "false")
                }
            }
        }
    }
}
