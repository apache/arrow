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

import Arrow
import ArrowC

@_cdecl("stringTypeFromSwift")
func stringTypeFromSwift(cSchema: UnsafePointer<ArrowC.ArrowSchema>) {
    let unsafePointer = UnsafeMutablePointer(mutating: cSchema)
    let exporter = ArrowCExporter()
    switch exporter.exportType(&unsafePointer.pointee, arrowType: ArrowType(ArrowType.ArrowString), name: "col1") {
    case .success:
        return
    case .failure(let err):
        fatalError("Error exporting string type from swift: \(err)")
    }
}

@_cdecl("stringTypeToSwift")
func stringTypeToSwift(cSchema: UnsafePointer<ArrowC.ArrowSchema>) {
    let importer = ArrowCImporter()
    switch importer.importField(cSchema.pointee) {
    case .success(let field):
        if field.name != "col1" {
            fatalError("Field name was incorrect expected: col1 but found: \(field.name)")
        }

        if field.type.id != ArrowTypeId.string {
            fatalError("Field type was incorrect expected: string but found: \(field.type.id)")
        }
    case .failure(let err):
        fatalError("Error importing string type to swift: \(err)")
    }
}

@_cdecl("arrayIntFromSwift")
func arrayIntFromSwift(cArray: UnsafePointer<ArrowC.ArrowArray>) {
    do {
        let unsafePointer = UnsafeMutablePointer(mutating: cArray)
        let arrayBuilder: NumberArrayBuilder<Int32> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        for index in 0..<100 {
            arrayBuilder.append(Int32(index))
        }

        let array = try arrayBuilder.finish()
        let exporter = ArrowCExporter()
        exporter.exportArray(&unsafePointer.pointee, arrowData: array.arrowData)
    } catch let err {
        fatalError("Error exporting array from swift \(err)")
    }
}

@_cdecl("arrayStringFromSwift")
func arrayStringFromSwift(cArray: UnsafePointer<ArrowC.ArrowArray>) {
    do {
        let unsafePointer = UnsafeMutablePointer(mutating: cArray)
        let arrayBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        for index in 0..<100 {
            arrayBuilder.append("test" + String(index))
        }

        let array = try arrayBuilder.finish()
        let exporter = ArrowCExporter()
        exporter.exportArray(&unsafePointer.pointee, arrowData: array.arrowData)
    } catch let err {
        fatalError("Error exporting array from swift \(err)")
    }
}

@_cdecl("arrayIntToSwift")
func arrayIntToSwift(cArray: UnsafePointer<ArrowC.ArrowArray>) {
    let importer = ArrowCImporter()
    switch importer.importArray(cArray, arrowType: ArrowType(ArrowType.ArrowInt32)) {
    case .success(let int32Holder):
        let result = RecordBatch.Builder()
            .addColumn("col1", arrowArray: int32Holder)
            .finish()
        switch result {
        case .success(let recordBatch):
            let col1: Arrow.ArrowArray<Int32> = recordBatch.data(for: 0)
            for index in 0..<col1.length {
                let colVal = col1[index]!
                if colVal != index + 1 {
                    fatalError("Data is incorrect: expected: \(index + 1) but found: \(colVal)")
                }
            }
        case .failure(let err):
            fatalError("Error makeing RecordBatch from imported array \(err)")
        }
    case .failure(let err):
        fatalError("Error importing int32 array to swift: \(err)")
    }
}

@_cdecl("arrayStringToSwift")
func arrayStringToSwift(cArray: UnsafePointer<ArrowC.ArrowArray>) {
    let importer = ArrowCImporter()
    switch importer.importArray(cArray, arrowType: ArrowType(ArrowType.ArrowString)) {
    case .success(let dataHolder):
        let result = RecordBatch.Builder()
            .addColumn("col1", arrowArray: dataHolder)
            .finish()
        switch result {
        case .success(let recordBatch):
            let col1: Arrow.ArrowArray<String> = recordBatch.data(for: 0)
            for index in 0..<col1.length {
                let colVal = col1[index]!
                if colVal != "test\(index)" {
                    fatalError("Data is incorrect: expected: \(index + 1) but found: \(colVal)")
                }
            }
        case .failure(let err):
            fatalError("Error makeing RecordBatch from imported array \(err)")
        }
    case .failure(let err):
        fatalError("Error importing int32 array to swift: \(err)")
    }
}
