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

private func makeBinaryHolder(_ buffers: [ArrowBuffer]) -> Result<ArrowArrayHolder, ArrowError> {
    do {
        let arrowData = try ArrowData(ArrowType(ArrowType.ArrowBinary), buffers: buffers,
                                      nullCount: buffers[0].length, stride: MemoryLayout<Int8>.stride)
        return .success(ArrowArrayHolder(BinaryArray(arrowData)))
    } catch let error as ArrowError {
        return .failure(error)
    } catch {
        return .failure(.unknownError("\(error)"))
    }
}

private func makeStringHolder(_ buffers: [ArrowBuffer]) -> Result<ArrowArrayHolder, ArrowError> {
    do {
        let arrowData = try ArrowData(ArrowType(ArrowType.ArrowString), buffers: buffers,
                                      nullCount: buffers[0].length, stride: MemoryLayout<Int8>.stride)
        return .success(ArrowArrayHolder(StringArray(arrowData)))
    } catch let error as ArrowError {
        return .failure(error)
    } catch {
        return .failure(.unknownError("\(error)"))
    }
}

private func makeFloatHolder(_ floatType: org_apache_arrow_flatbuf_FloatingPoint,
                             buffers: [ArrowBuffer]
) -> Result<ArrowArrayHolder, ArrowError> {
    switch floatType.precision {
    case .single:
        return makeFixedHolder(Float.self, buffers: buffers, arrowType: ArrowType.ArrowFloat)
    case .double:
        return makeFixedHolder(Double.self, buffers: buffers, arrowType: ArrowType.ArrowDouble)
    default:
        return .failure(.unknownType("Float precision \(floatType.precision) currently not supported"))
    }
}

private func makeDateHolder(_ dateType: org_apache_arrow_flatbuf_Date,
                            buffers: [ArrowBuffer]
) -> Result<ArrowArrayHolder, ArrowError> {
    do {
        if dateType.unit == .day {
            let arrowData = try ArrowData(ArrowType(ArrowType.ArrowString), buffers: buffers,
                                          nullCount: buffers[0].length, stride: MemoryLayout<Date>.stride)
            return .success(ArrowArrayHolder(Date32Array(arrowData)))
        }

        let arrowData = try ArrowData(ArrowType(ArrowType.ArrowString), buffers: buffers,
                                      nullCount: buffers[0].length, stride: MemoryLayout<Date>.stride)
        return .success(ArrowArrayHolder(Date64Array(arrowData)))
    } catch let error as ArrowError {
        return .failure(error)
    } catch {
        return .failure(.unknownError("\(error)"))
    }
}

private func makeTimeHolder(_ timeType: org_apache_arrow_flatbuf_Time,
                            buffers: [ArrowBuffer]
) -> Result<ArrowArrayHolder, ArrowError> {
    do {
        if timeType.unit == .second || timeType.unit == .millisecond {
            let arrowUnit: ArrowTime32Unit = timeType.unit == .second ? .seconds : .milliseconds
            let arrowData = try ArrowData(ArrowTypeTime32(arrowUnit), buffers: buffers,
                                          nullCount: buffers[0].length, stride: MemoryLayout<Time32>.stride)
            return .success(ArrowArrayHolder(FixedArray<Time32>(arrowData)))
        }

        let arrowUnit: ArrowTime64Unit = timeType.unit == .microsecond ? .microseconds : .nanoseconds
        let arrowData = try ArrowData(ArrowTypeTime64(arrowUnit), buffers: buffers,
                                      nullCount: buffers[0].length, stride: MemoryLayout<Time64>.stride)
        return .success(ArrowArrayHolder(FixedArray<Time64>(arrowData)))
    } catch let error as ArrowError {
        return .failure(error)
    } catch {
        return .failure(.unknownError("\(error)"))
    }
}

private func makeBoolHolder(_ buffers: [ArrowBuffer]) -> Result<ArrowArrayHolder, ArrowError> {
    do {
        let arrowData = try ArrowData(ArrowType(ArrowType.ArrowBool), buffers: buffers,
                                      nullCount: buffers[0].length, stride: MemoryLayout<UInt8>.stride)
        return .success(ArrowArrayHolder(BoolArray(arrowData)))
    } catch let error as ArrowError {
        return .failure(error)
    } catch {
        return .failure(.unknownError("\(error)"))
    }
}

private func makeFixedHolder<T>(
    _: T.Type, buffers: [ArrowBuffer],
    arrowType: ArrowType.Info
) -> Result<ArrowArrayHolder, ArrowError> {
    do {
        let arrowData = try ArrowData(ArrowType(arrowType), buffers: buffers,
                                      nullCount: buffers[0].length, stride: MemoryLayout<T>.stride)
        return .success(ArrowArrayHolder(FixedArray<T>(arrowData)))
    } catch let error as ArrowError {
        return .failure(error)
    } catch {
        return .failure(.unknownError("\(error)"))
    }
}

func makeArrayHolder( // swiftlint:disable:this cyclomatic_complexity
    _ field: org_apache_arrow_flatbuf_Field,
    buffers: [ArrowBuffer]
) -> Result<ArrowArrayHolder, ArrowError> {
    let type = field.typeType
    switch type {
    case .int:
        let intType = field.type(type: org_apache_arrow_flatbuf_Int.self)!
        let bitWidth = intType.bitWidth
        if bitWidth == 8 {
            if intType.isSigned {
                return makeFixedHolder(Int8.self, buffers: buffers, arrowType: ArrowType.ArrowInt8)
            } else {
                return makeFixedHolder(UInt8.self, buffers: buffers, arrowType: ArrowType.ArrowUInt8)
            }
        } else if bitWidth == 16 {
            if intType.isSigned {
                return makeFixedHolder(Int16.self, buffers: buffers, arrowType: ArrowType.ArrowInt16)
            } else {
                return makeFixedHolder(UInt16.self, buffers: buffers, arrowType: ArrowType.ArrowUInt16)
            }
        } else if bitWidth == 32 {
            if intType.isSigned {
                return makeFixedHolder(Int32.self, buffers: buffers, arrowType: ArrowType.ArrowInt32)
            } else {
                return makeFixedHolder(UInt32.self, buffers: buffers, arrowType: ArrowType.ArrowUInt32)
            }
        } else if bitWidth == 64 {
            if intType.isSigned {
                return makeFixedHolder(Int64.self, buffers: buffers, arrowType: ArrowType.ArrowInt64)
            } else {
                return makeFixedHolder(UInt64.self, buffers: buffers, arrowType: ArrowType.ArrowUInt64)
            }
        }
        return .failure(.unknownType("Int width \(bitWidth) currently not supported"))
    case .bool:
        return makeBoolHolder(buffers)
    case .floatingpoint:
        let floatType = field.type(type: org_apache_arrow_flatbuf_FloatingPoint.self)!
        return makeFloatHolder(floatType, buffers: buffers)
    case .utf8:
        return makeStringHolder(buffers)
    case .binary:
        return makeBinaryHolder(buffers)
    case .date:
        let dateType = field.type(type: org_apache_arrow_flatbuf_Date.self)!
        return makeDateHolder(dateType, buffers: buffers)
    case .time:
        let timeType = field.type(type: org_apache_arrow_flatbuf_Time.self)!
        return makeTimeHolder(timeType, buffers: buffers)
    default:
        return .failure(.unknownType("Type \(type) currently not supported"))
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
    case .int, .bool, .floatingpoint, .date, .time:
        return true
    default:
        return false
    }
}

func findArrowType( // swiftlint:disable:this cyclomatic_complexity
    _ field: org_apache_arrow_flatbuf_Field) -> ArrowType {
    let type = field.typeType
    switch type {
    case .int:
        let intType = field.type(type: org_apache_arrow_flatbuf_Int.self)!
        let bitWidth = intType.bitWidth
        if bitWidth == 8 { return ArrowType(intType.isSigned ? ArrowType.ArrowInt8 : ArrowType.ArrowUInt8) }
        if bitWidth == 16 { return ArrowType(intType.isSigned ? ArrowType.ArrowInt16 : ArrowType.ArrowUInt16) }
        if bitWidth == 32 { return ArrowType(intType.isSigned ? ArrowType.ArrowInt32 : ArrowType.ArrowUInt32) }
        if bitWidth == 64 { return ArrowType(intType.isSigned ? ArrowType.ArrowInt64 : ArrowType.ArrowUInt64) }
        return ArrowType(ArrowType.ArrowUnknown)
    case .bool:
        return ArrowType(ArrowType.ArrowBool)
    case .floatingpoint:
        let floatType = field.type(type: org_apache_arrow_flatbuf_FloatingPoint.self)!
        switch floatType.precision {
        case .single:
            return ArrowType(ArrowType.ArrowFloat)
        case .double:
            return ArrowType(ArrowType.ArrowDouble)
        default:
            return ArrowType(ArrowType.ArrowUnknown)
        }
    case .utf8:
        return ArrowType(ArrowType.ArrowString)
    case .binary:
        return ArrowType(ArrowType.ArrowBinary)
    case .date:
        let dateType = field.type(type: org_apache_arrow_flatbuf_Date.self)!
        if dateType.unit == .day {
            return ArrowType(ArrowType.ArrowDate32)
        }

        return ArrowType(ArrowType.ArrowDate64)
    case .time:
        let timeType = field.type(type: org_apache_arrow_flatbuf_Time.self)!
        if timeType.unit == .second || timeType.unit == .millisecond {
            return ArrowTypeTime32(timeType.unit == .second ? .seconds : .milliseconds)
        }

        return ArrowTypeTime64(timeType.unit == .microsecond ? .microseconds : .nanoseconds)
    default:
        return ArrowType(ArrowType.ArrowUnknown)
    }
}

func validateBufferIndex(_ recordBatch: org_apache_arrow_flatbuf_RecordBatch, index: Int32) throws {
    if index >= recordBatch.buffersCount {
        throw ArrowError.outOfBounds(index: Int64(index))
    }
}

func validateFileData(_ data: Data) -> Bool {
    let markerLength = FILEMARKER.utf8.count
    let startString = String(decoding: data[..<markerLength], as: UTF8.self)
    let endString = String(decoding: data[(data.count - markerLength)...], as: UTF8.self)
    return startString == FILEMARKER && endString == FILEMARKER
}
