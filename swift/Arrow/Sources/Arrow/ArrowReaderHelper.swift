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

import FlatBuffers
import Foundation

fileprivate func makeFixedHolder<T>(_: T.Type, buffers: [ArrowBuffer]) throws -> ChunkedArrayHolder {
    let nullCount = buffers[0].length
    let arrowData = try ArrowData(ArrowType.ArrowInt32, buffers: buffers,
                                  nullCount: nullCount, stride: MemoryLayout<T>.stride)
    let chuckedArray = try ChunkedArray<T>([FixedArray<T>(arrowData)])
    return ChunkedArrayHolder(chuckedArray)
}

func makeArrayHolder(_ field: org_apache_arrow_flatbuf_Field, buffers: [ArrowBuffer]) throws -> ChunkedArrayHolder {
    let type = field.typeType
    switch type {
    case .int:
        let intType = field.type(type: org_apache_arrow_flatbuf_Int.self)!
        let bitWidth = intType.bitWidth
        if bitWidth == 8 {
            if intType.isSigned {
                return try makeFixedHolder(Int8.self, buffers: buffers)
            } else {
                return try makeFixedHolder(UInt8.self, buffers: buffers)
            }
        } else if bitWidth == 16 {
            if intType.isSigned {
                return try makeFixedHolder(Int16.self, buffers: buffers)
            } else {
                return try makeFixedHolder(UInt16.self, buffers: buffers)
            }
        } else if bitWidth == 32 {
            if intType.isSigned {
                return try makeFixedHolder(Int32.self, buffers: buffers)
            } else {
                return try makeFixedHolder(UInt32.self, buffers: buffers)
            }
        } else if bitWidth == 64 {
            if intType.isSigned {
                return try makeFixedHolder(Int64.self, buffers: buffers)
            } else {
                return try makeFixedHolder(UInt64.self, buffers: buffers)
            }
        }
        throw ValidationError.unknownType
    case .bool:
        let arrowData = try ArrowData(ArrowType.ArrowInt32, buffers: buffers,
                                      nullCount: buffers[0].length, stride: MemoryLayout<UInt8>.stride)
        let chuckedArray = try ChunkedArray<Bool>([BoolArray(arrowData)])
        return ChunkedArrayHolder(chuckedArray)
    case .floatingpoint:
        let floatType = field.type(type: org_apache_arrow_flatbuf_FloatingPoint.self)!
        switch floatType.precision {
        case .single:
            return try makeFixedHolder(Float.self, buffers: buffers)
        case .double:
            return try makeFixedHolder(Double.self, buffers: buffers)
        default:
            throw ValidationError.unknownType
        }
    case .utf8:
        let arrowData = try ArrowData(ArrowType.ArrowString, buffers: buffers,
                                      nullCount: buffers[0].length, stride: MemoryLayout<Int8>.stride)
        let chuckedArray = try ChunkedArray<String>([StringArray(arrowData)])
        return ChunkedArrayHolder(chuckedArray)
    default:
        throw ValidationError.unknownType
    }
}

func makeBuffer(_ buffer: org_apache_arrow_flatbuf_Buffer, fileData: Data,
                length: UInt, messageOffset: Int64) -> ArrowBuffer {
    let startOffset = messageOffset + buffer.offset
    let endOffset = startOffset + buffer.length
    let bufferData = [UInt8](fileData[startOffset ..< endOffset])
    return ArrowBuffer.createBuffer(bufferData, length: length)
}

func isFixedPrimitive(_ type: org_apache_arrow_flatbuf_Type_) -> Bool {
    switch type {
    case .int, .bool, .floatingpoint, .date:
        return true
    default:
        return false
    }
}

func findArrowType(_ field: org_apache_arrow_flatbuf_Field) -> ArrowType.Info {
    let type = field.typeType
    switch type {
    case .int:
        let intType = field.type(type: org_apache_arrow_flatbuf_Int.self)!
        let bitWidth = intType.bitWidth
        if bitWidth == 8 { return intType.isSigned ? ArrowType.ArrowInt8 : ArrowType.ArrowUInt8 }
        if bitWidth == 16 { return intType.isSigned ? ArrowType.ArrowInt16 : ArrowType.ArrowUInt16 }
        if bitWidth == 32 { return intType.isSigned ? ArrowType.ArrowInt32 : ArrowType.ArrowUInt32 }
        if bitWidth == 64 { return intType.isSigned ? ArrowType.ArrowInt64 : ArrowType.ArrowUInt64 }
        return ArrowType.ArrowUnknown
    case .bool:
        return ArrowType.ArrowBool
    case .floatingpoint:
        let floatType = field.type(type: org_apache_arrow_flatbuf_FloatingPoint.self)!
        switch floatType.precision {
        case .single:
            return ArrowType.ArrowFloat
        case .double:
            return ArrowType.ArrowDouble
        default:
            return ArrowType.ArrowUnknown
        }
    case .utf8:
        return ArrowType.ArrowString
    default:
        return ArrowType.ArrowUnknown
    }
}

func validateBufferIndex(_ recordBatch: org_apache_arrow_flatbuf_RecordBatch, index: Int32) throws {
    if index >= recordBatch.buffersCount {
        throw ArrowError.runtimeError("Buffer index is out of bounds: \(index)")
    }
}

func validateFileData(_ data: Data) -> Bool {
    let markerLength = FILEMARKER.utf8.count
    let startString = String(decoding: data[..<markerLength], as: UTF8.self)
    let endString = String(decoding: data[(data.count - markerLength)...], as: UTF8.self)
    return startString == FILEMARKER && endString == FILEMARKER
}
