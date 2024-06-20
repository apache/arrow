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

public class ImportArrayHolder: ArrowArrayHolder {
    let cArrayPtr: UnsafePointer<ArrowC.ArrowArray>
    public var type: ArrowType {self.holder.type}
    public var length: UInt {self.holder.length}
    public var nullCount: UInt {self.holder.nullCount}
    public var array: Any {self.holder.array}
    public var data: ArrowData {self.holder.data}
    public var getBufferData: () -> [Data] {self.holder.getBufferData}
    public var getBufferDataSizes: () -> [Int] {self.holder.getBufferDataSizes}
    public var getArrowColumn: (ArrowField, [ArrowArrayHolder]) throws -> ArrowColumn {self.holder.getArrowColumn}
    private let holder: ArrowArrayHolder
    init(_ holder: ArrowArrayHolder, cArrayPtr: UnsafePointer<ArrowC.ArrowArray>) {
        self.cArrayPtr = cArrayPtr
        self.holder = holder
    }

    deinit {
        if self.cArrayPtr.pointee.release != nil {
            ArrowCImporter.release(self.cArrayPtr)
        }
    }
}

public class ArrowCImporter {
    private func appendToBuffer(
        _ cBuffer: UnsafeRawPointer?,
        arrowBuffers: inout [ArrowBuffer],
        length: UInt) {
        if cBuffer == nil {
            arrowBuffers.append(ArrowBuffer.createEmptyBuffer())
            return
        }

        let pointer = UnsafeMutableRawPointer(mutating: cBuffer)!
        arrowBuffers.append(
            ArrowBuffer(length: length, capacity: length, rawPointer: pointer, isMemoryOwner: false))
    }

    public init() {}

    public func importType(_ cArrow: String, name: String = "") ->
        Result<ArrowField, ArrowError> {
        do {
            let type = try ArrowType.fromCDataFormatId(cArrow)
            return .success(ArrowField(name, type: ArrowType(type.info), isNullable: true))
        } catch {
            return .failure(.invalid("Error occurred while attempting to import type: \(error)"))
        }
    }

    public func importField(_ cSchema: ArrowC.ArrowSchema) ->
        Result<ArrowField, ArrowError> {
        if cSchema.n_children > 0 {
            ArrowCImporter.release(cSchema)
            return .failure(.invalid("Children currently not supported"))
        } else if cSchema.dictionary != nil {
            ArrowCImporter.release(cSchema)
            return .failure(.invalid("Dictinoary types currently not supported"))
        }

        switch importType(
            String(cString: cSchema.format), name: String(cString: cSchema.name)) {
        case .success(let field):
            ArrowCImporter.release(cSchema)
            return .success(field)
        case .failure(let err):
            ArrowCImporter.release(cSchema)
            return .failure(err)
        }
    }

    public func importArray(
        _ cArray: UnsafePointer<ArrowC.ArrowArray>,
        arrowType: ArrowType,
        isNullable: Bool = false
    ) -> Result<ArrowArrayHolder, ArrowError> {
        let arrowField = ArrowField("", type: arrowType, isNullable: isNullable)
        return importArray(cArray, arrowField: arrowField)
    }

    public func importArray( // swiftlint:disable:this cyclomatic_complexity function_body_length
        _ cArrayPtr: UnsafePointer<ArrowC.ArrowArray>,
        arrowField: ArrowField
    ) -> Result<ArrowArrayHolder, ArrowError> {
        let cArray = cArrayPtr.pointee
        if cArray.null_count < 0 {
            ArrowCImporter.release(cArrayPtr)
            return .failure(.invalid("Uncomputed null count is not supported"))
        } else if cArray.n_children > 0 {
            ArrowCImporter.release(cArrayPtr)
            return .failure(.invalid("Children currently not supported"))
        } else if cArray.dictionary != nil {
            ArrowCImporter.release(cArrayPtr)
            return .failure(.invalid("Dictionary types currently not supported"))
        } else if cArray.offset != 0 {
            ArrowCImporter.release(cArrayPtr)
            return .failure(.invalid("Offset of 0 is required but found offset: \(cArray.offset)"))
        }

        let arrowType = arrowField.type
        let length = UInt(cArray.length)
        let nullCount = UInt(cArray.null_count)
        var arrowBuffers = [ArrowBuffer]()

        if cArray.n_buffers > 0 {
            if cArray.buffers == nil {
                ArrowCImporter.release(cArrayPtr)
                return .failure(.invalid("C array buffers is nil"))
            }

            switch arrowType.info {
            case .variableInfo:
                if cArray.n_buffers != 3 {
                    ArrowCImporter.release(cArrayPtr)
                    return .failure(
                        .invalid("Variable buffer count expected 3 but found \(cArray.n_buffers)"))
                }

                appendToBuffer(cArray.buffers[0], arrowBuffers: &arrowBuffers, length: UInt(ceil(Double(length) / 8)))
                appendToBuffer(cArray.buffers[1], arrowBuffers: &arrowBuffers, length: length)
                let lastOffsetLength = cArray.buffers[1]!
                    .advanced(by: Int(length) * MemoryLayout<Int32>.stride)
                    .load(as: Int32.self)
                appendToBuffer(cArray.buffers[2], arrowBuffers: &arrowBuffers, length: UInt(lastOffsetLength))
            default:
                if cArray.n_buffers != 2 {
                    ArrowCImporter.release(cArrayPtr)
                    return .failure(.invalid("Expected buffer count 2 but found \(cArray.n_buffers)"))
                }

                appendToBuffer(cArray.buffers[0], arrowBuffers: &arrowBuffers, length: UInt(ceil(Double(length) / 8)))
                appendToBuffer(cArray.buffers[1], arrowBuffers: &arrowBuffers, length: length)
            }
        }

        switch makeArrayHolder(arrowField, buffers: arrowBuffers, nullCount: nullCount) {
        case .success(let holder):
            return .success(ImportArrayHolder(holder, cArrayPtr: cArrayPtr))
        case .failure(let err):
            ArrowCImporter.release(cArrayPtr)
            return .failure(err)
        }
    }

    public static func release(_ cArrayPtr: UnsafePointer<ArrowC.ArrowArray>) {
        if cArrayPtr.pointee.release != nil {
            let cSchemaMutablePtr = UnsafeMutablePointer<ArrowC.ArrowArray>(mutating: cArrayPtr)
            cArrayPtr.pointee.release(cSchemaMutablePtr)
        }
    }

    public static func release(_ cSchema: ArrowC.ArrowSchema) {
        if cSchema.release != nil {
            let cSchemaPtr = UnsafeMutablePointer<ArrowC.ArrowSchema>.allocate(capacity: 1)
            cSchemaPtr.initialize(to: cSchema)
            cSchema.release(cSchemaPtr)
        }
    }
}
