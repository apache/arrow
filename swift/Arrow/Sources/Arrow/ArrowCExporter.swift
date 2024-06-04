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

import Foundation
import ArrowC
import Atomics

// The memory used by UnsafeAtomic is not automatically
// reclaimed. Since this value is initialized once
// and used until the program/app is closed it's
// memory will be released on program/app exit
let exportDataCounter: UnsafeAtomic<Int> = .create(0)

public class ArrowCExporter {
    private class ExportData {
        let id: Int
        init() {
            id = exportDataCounter.loadThenWrappingIncrement(ordering: .relaxed)
            ArrowCExporter.exportedData[id] = self
        }
    }

    private class ExportSchema: ExportData {
        public let arrowTypeNameCstr: UnsafePointer<CChar>
        public let nameCstr: UnsafePointer<CChar>
        private let arrowType: ArrowType
        private let name: String
        private let arrowTypeName: String
        init(_ arrowType: ArrowType, name: String = "") throws {
            self.arrowType = arrowType
            // keeping the name str to ensure the cstring buffer remains valid
            self.name = name
            self.arrowTypeName = try arrowType.cDataFormatId
            self.nameCstr = (self.name as NSString).utf8String!
            self.arrowTypeNameCstr = (self.arrowTypeName as NSString).utf8String!
            super.init()
        }
    }

    private class ExportArray: ExportData {
        private let arrowData: ArrowData
        private(set) var data = [UnsafeRawPointer?]()
        private(set) var buffers: UnsafeMutablePointer<UnsafeRawPointer?>
        init(_ arrowData: ArrowData) {
            // keep a reference to the ArrowData
            // obj so the memory doesn't get
            // deallocated
            self.arrowData = arrowData
            for arrowBuffer in arrowData.buffers {
                data.append(arrowBuffer.rawPointer)
            }

            self.buffers = UnsafeMutablePointer(mutating: data)
            super.init()
        }
    }

    private static var exportedData = [Int: ExportData]()
    public init() {}

    public func exportType(_ cSchema: inout ArrowC.ArrowSchema, arrowType: ArrowType, name: String = "") ->
        Result<Bool, ArrowError> {
        do {
            let exportSchema = try ExportSchema(arrowType, name: name)
            cSchema.format = exportSchema.arrowTypeNameCstr
            cSchema.name = exportSchema.nameCstr
            cSchema.private_data =
                UnsafeMutableRawPointer(mutating: UnsafeRawPointer(bitPattern: exportSchema.id))
            cSchema.release = {(data: UnsafeMutablePointer<ArrowC.ArrowSchema>?) in
                let arraySchema = data!.pointee
                let exportId = Int(bitPattern: arraySchema.private_data)
                guard ArrowCExporter.exportedData[exportId] != nil else {
                    fatalError("Export schema not found with id \(exportId)")
                }

                // the data associated with this exportSchema object
                // which includes the C strings for the format and name
                // be deallocated upon removal
                ArrowCExporter.exportedData.removeValue(forKey: exportId)
                ArrowC.ArrowSwiftClearReleaseSchema(data)
            }
        } catch {
            return .failure(.unknownError("\(error)"))
        }
        return .success(true)
    }

    public func exportField(_ schema: inout ArrowC.ArrowSchema, field: ArrowField) ->
        Result<Bool, ArrowError> {
            return exportType(&schema, arrowType: field.type, name: field.name)
    }

    public func exportArray(_ cArray: inout ArrowC.ArrowArray, arrowData: ArrowData) {
        let exportArray = ExportArray(arrowData)
        cArray.buffers = exportArray.buffers
        cArray.length = Int64(arrowData.length)
        cArray.null_count = Int64(arrowData.nullCount)
        cArray.n_buffers = Int64(arrowData.buffers.count)
        // Swift Arrow does not currently support children or dictionaries
        // This will need to be updated once support has been added
        cArray.n_children = 0
        cArray.children = nil
        cArray.dictionary = nil
        cArray.private_data =
            UnsafeMutableRawPointer(mutating: UnsafeRawPointer(bitPattern: exportArray.id))
        cArray.release = {(data: UnsafeMutablePointer<ArrowC.ArrowArray>?) in
            let arrayData = data!.pointee
            let exportId = Int(bitPattern: arrayData.private_data)
            guard ArrowCExporter.exportedData[exportId] != nil else {
                fatalError("Export data not found with id \(exportId)")
            }

            // the data associated with this exportArray object
            // which includes the entire arrowData object
            // and the buffers UnsafeMutablePointer[] will
            // be deallocated upon removal
            ArrowCExporter.exportedData.removeValue(forKey: exportId)
            ArrowC.ArrowSwiftClearReleaseArray(data)
        }
    }
}
