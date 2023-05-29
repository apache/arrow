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

func checkBoolRecordBatch(_ recordBatches: [RecordBatch]) {
    XCTAssertEqual(recordBatches.count, 1)
    for recordBatch in recordBatches {
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

func currentDirectory(path: String = #file) -> URL {
    return URL(fileURLWithPath: path).deletingLastPathComponent()
}

final class IPCFileReaderTests: XCTestCase {
    func testFileReader_double() throws {
        let fileURL = currentDirectory().appendingPathComponent("../../testdata_double.arrow")
        let arrowReader = ArrowReader()
        let recordBatches = try arrowReader.fromFile(fileURL)
        XCTAssertEqual(recordBatches.count, 1)
        for recordBatch in recordBatches {
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
        let fileRBs = try arrowReader.fromFile(fileURL)
        checkBoolRecordBatch(fileRBs)
    }
    
    func testFileWriter_bool() throws {
        //read existing file
        let fileURL = currentDirectory().appendingPathComponent("../../testdata_bool.arrow")
        let arrowReader = ArrowReader()
        let fileRBs = try arrowReader.fromFile(fileURL)
        checkBoolRecordBatch(fileRBs)
        let arrowWriter = ArrowWriter()
        //write data from file to a stream
        let writeData = try arrowWriter.toStream(fileRBs[0].schema, batches: fileRBs)
        //read stream back into recordbatches
        checkBoolRecordBatch(try arrowReader.fromStream(writeData))
        //write file record batches to another file
        let outputUrl = currentDirectory().appendingPathComponent("../../testfilewriter_bool.arrow")
        try arrowWriter.toFile(outputUrl, schema: fileRBs[0].schema, batches: fileRBs)
        checkBoolRecordBatch(try arrowReader.fromFile(outputUrl))
    }

    func makeSchema() throws -> ArrowSchema {
        let schemaBuilder = ArrowSchema.Builder();
        return schemaBuilder.addField("col1", type: ArrowType.ArrowUInt8, isNullable: true)
            .addField("col2", type: ArrowType.ArrowString, isNullable: false)
            .addField("col3", type: ArrowType.ArrowDate32, isNullable: false)
            .finish()
    }

    func makeRecordBatch() throws -> RecordBatch {
        let uint8Builder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder();
        uint8Builder.append(10)
        uint8Builder.append(22)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder();
        stringBuilder.append("test10")
        stringBuilder.append("test22")
        let date32Builder = try ArrowArrayBuilders.loadDate32ArrayBuilder();
        let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
        let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
        date32Builder.append(date1)
        date32Builder.append(date2)
        
        let intHolder = ChunkedArrayHolder(try ChunkedArray([uint8Builder.finish()]))
        let stringHolder = ChunkedArrayHolder(try ChunkedArray([stringBuilder.finish()]))
        let date32Holder = ChunkedArrayHolder(try ChunkedArray([date32Builder.finish()]))
        return RecordBatch.Builder()
            .addColumn("col1", chunked: intHolder)
            .addColumn("col2", chunked: stringHolder)
            .addColumn("col3", chunked: date32Holder)
            .finish()
    }
    
    func testInMemoryToFromStream() throws {
        //read existing file
        let schema = try makeSchema()
        let recordBatch = try makeRecordBatch()
        let arrowWriter = ArrowWriter()
        let writeData = try arrowWriter.toStream(schema, batches: [recordBatch])
        let arrowReader = ArrowReader()
        let recordBatches = try arrowReader.fromStream(writeData)
        XCTAssertEqual(recordBatches.count, 1)
        for recordBatch in recordBatches {
            XCTAssertEqual(recordBatch.length, 2)
            XCTAssertEqual(recordBatch.columns.count, 3)
            XCTAssertEqual(recordBatch.schema.fields.count, 3)
            XCTAssertEqual(recordBatch.schema.fields[0].name, "col1")
            XCTAssertEqual(recordBatch.schema.fields[0].type, ArrowType.ArrowUInt8)
            XCTAssertEqual(recordBatch.schema.fields[1].name, "col2")
            XCTAssertEqual(recordBatch.schema.fields[1].type, ArrowType.ArrowString)
            XCTAssertEqual(recordBatch.schema.fields[2].name, "col3")
            XCTAssertEqual(recordBatch.schema.fields[2].type, ArrowType.ArrowDate32)
            let dateVal = "\((recordBatch.columns[2].holder as! AsString).asString(0))"
            XCTAssertEqual(dateVal, "2014-09-10 00:00:00 +0000")
            let stringVal = "\((recordBatch.columns[1].holder as! AsString).asString(1))"
            XCTAssertEqual(stringVal, "test22")
            let uintVal = "\((recordBatch.columns[0].holder as! AsString).asString(0))"
            XCTAssertEqual(uintVal, "10")
        }
    }
}
