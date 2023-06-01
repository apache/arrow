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

@discardableResult
func checkBoolRecordBatch(_ result: Result<[RecordBatch], ArrowError>) throws -> [RecordBatch] {
    let recordBatches: [RecordBatch]
    switch result {
    case .success(let rbBatches):
        recordBatches = rbBatches
    case .failure(let error):
        throw error
    }
    
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
            let str = column.array as! AsString
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
    
    return recordBatches
}

func currentDirectory(path: String = #file) -> URL {
    return URL(fileURLWithPath: path).deletingLastPathComponent()
}

final class IPCFileReaderTests: XCTestCase {
    func testFileReader_double() throws {
        let fileURL = currentDirectory().appendingPathComponent("../../testdata_double.arrow")
        let arrowReader = ArrowReader()
        let result = arrowReader.fromFile(fileURL)
        let recordBatches: [RecordBatch]
        switch result {
        case .success(let rbBatches):
            recordBatches = rbBatches
        case .failure(let error):
            throw error
        }

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
                let str = column.array as! AsString
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
        try checkBoolRecordBatch(arrowReader.fromFile(fileURL))
    }
    
    
    func testFileWriter_bool() throws {
        //read existing file
        let fileURL = currentDirectory().appendingPathComponent("../../testdata_bool.arrow")
        let arrowReader = ArrowReader()
        let fileRBs = try checkBoolRecordBatch(arrowReader.fromFile(fileURL))
        let arrowWriter = ArrowWriter()
        //write data from file to a stream
        switch arrowWriter.toStream(fileRBs[0].schema, batches: fileRBs) {
        case .success(let writeData):
            //read stream back into recordbatches
            try checkBoolRecordBatch(arrowReader.fromStream(writeData))
        case .failure(let error):
            throw error
        }

        //write file record batches to another file
        let outputUrl = currentDirectory().appendingPathComponent("../../testfilewriter_bool.arrow")
        switch arrowWriter.toFile(outputUrl, schema: fileRBs[0].schema, batches: fileRBs) {
        case .success(_):
            try checkBoolRecordBatch(arrowReader.fromFile(outputUrl))
        case .failure(let error):
            throw error
        }
        
    }

    func makeSchema() -> ArrowSchema {
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
        uint8Builder.append(33)
        uint8Builder.append(44)
        let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder();
        stringBuilder.append("test10")
        stringBuilder.append("test22")
        stringBuilder.append("test33")
        stringBuilder.append("test44")
        let date32Builder = try ArrowArrayBuilders.loadDate32ArrayBuilder();
        let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
        let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
        date32Builder.append(date1)
        date32Builder.append(date2)
        date32Builder.append(date1)
        date32Builder.append(date2)

        let intHolder = ArrowArrayHolder(try uint8Builder.finish())
        let stringHolder = ArrowArrayHolder(try stringBuilder.finish())
        let date32Holder = ArrowArrayHolder(try date32Builder.finish())
        let result = RecordBatch.Builder()
            .addColumn("col1", arrowArray: intHolder)
            .addColumn("col2", arrowArray: stringHolder)
            .addColumn("col3", arrowArray: date32Holder)
            .finish()
        switch result {
        case .success(let recordBatch):
            return recordBatch
        case .failure(let error):
            throw error
        }
    }
    
    func testInMemoryToFromStream() throws {
        //read existing file
        let schema = makeSchema()
        let recordBatch = try makeRecordBatch()
        let arrowWriter = ArrowWriter()
        switch arrowWriter.toStream(schema, batches: [recordBatch]) {
        case .success(let writeData):
            let arrowReader = ArrowReader()
            switch arrowReader.fromStream(writeData) {
            case .success(let recordBatches):
                XCTAssertEqual(recordBatches.count, 1)
                for recordBatch in recordBatches {
                    XCTAssertEqual(recordBatch.length, 4)
                    XCTAssertEqual(recordBatch.columns.count, 3)
                    XCTAssertEqual(recordBatch.schema.fields.count, 3)
                    XCTAssertEqual(recordBatch.schema.fields[0].name, "col1")
                    XCTAssertEqual(recordBatch.schema.fields[0].type, ArrowType.ArrowUInt8)
                    XCTAssertEqual(recordBatch.schema.fields[1].name, "col2")
                    XCTAssertEqual(recordBatch.schema.fields[1].type, ArrowType.ArrowString)
                    XCTAssertEqual(recordBatch.schema.fields[2].name, "col3")
                    XCTAssertEqual(recordBatch.schema.fields[2].type, ArrowType.ArrowDate32)
                    let dateVal = "\((recordBatch.columns[2].array as! AsString).asString(0))"
                    XCTAssertEqual(dateVal, "2014-09-10 00:00:00 +0000")
                    let stringVal = "\((recordBatch.columns[1].array as! AsString).asString(1))"
                    XCTAssertEqual(stringVal, "test22")
                    let uintVal = "\((recordBatch.columns[0].array as! AsString).asString(0))"
                    XCTAssertEqual(uintVal, "10")
                    let stringVal2 = "\((recordBatch.columns[1].array as! AsString).asString(3))"
                    XCTAssertEqual(stringVal2, "test44")
                    let uintVal2 = "\((recordBatch.columns[0].array as! AsString).asString(3))"
                    XCTAssertEqual(uintVal2, "44")
                }
            case.failure(let error):
                throw error
            }
        case .failure(let error):
            throw error
        }
    }
}
