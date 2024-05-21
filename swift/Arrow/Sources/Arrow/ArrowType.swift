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

public typealias Time32 = Int32
public typealias Time64 = Int64
public typealias Date32 = Int32
public typealias Date64 = Int64

func FlatBuffersVersion_23_1_4() { // swiftlint:disable:this identifier_name
}

public enum ArrowError: Error {
    case none
    case unknownType(String)
    case runtimeError(String)
    case outOfBounds(index: Int64)
    case arrayHasNoElements
    case unknownError(String)
    case notImplemented
    case ioError(String)
    case invalid(String)
}

public enum ArrowTypeId {
    case binary
    case boolean
    case date32
    case date64
    case dateType
    case decimal128
    case decimal256
    case dictionary
    case double
    case fixedSizeBinary
    case fixedWidthType
    case float
    // case HalfFloatType
    case int16
    case int32
    case int64
    case int8
    case integer
    case intervalUnit
    case list
    case nested
    case null
    case number
    case string
    case strct
    case time32
    case time64
    case time
    case uint16
    case uint32
    case uint64
    case uint8
    case union
    case unknown
}

public enum ArrowTime32Unit {
    case seconds
    case milliseconds
}

public enum ArrowTime64Unit {
    case microseconds
    case nanoseconds
}

public class ArrowTypeTime32: ArrowType {
    let unit: ArrowTime32Unit
    public init(_ unit: ArrowTime32Unit) {
        self.unit = unit
        super.init(ArrowType.ArrowTime32)
    }
}

public class ArrowTypeTime64: ArrowType {
    let unit: ArrowTime64Unit
    public init(_ unit: ArrowTime64Unit) {
        self.unit = unit
        super.init(ArrowType.ArrowTime64)
    }
}

public class ArrowType {
    public private(set) var info: ArrowType.Info
    public static let ArrowInt8 = Info.primitiveInfo(ArrowTypeId.int8)
    public static let ArrowInt16 = Info.primitiveInfo(ArrowTypeId.int16)
    public static let ArrowInt32 = Info.primitiveInfo(ArrowTypeId.int32)
    public static let ArrowInt64 = Info.primitiveInfo(ArrowTypeId.int64)
    public static let ArrowUInt8 = Info.primitiveInfo(ArrowTypeId.uint8)
    public static let ArrowUInt16 = Info.primitiveInfo(ArrowTypeId.uint16)
    public static let ArrowUInt32 = Info.primitiveInfo(ArrowTypeId.uint32)
    public static let ArrowUInt64 = Info.primitiveInfo(ArrowTypeId.uint64)
    public static let ArrowFloat = Info.primitiveInfo(ArrowTypeId.float)
    public static let ArrowDouble = Info.primitiveInfo(ArrowTypeId.double)
    public static let ArrowUnknown = Info.primitiveInfo(ArrowTypeId.unknown)
    public static let ArrowString = Info.variableInfo(ArrowTypeId.string)
    public static let ArrowBool = Info.primitiveInfo(ArrowTypeId.boolean)
    public static let ArrowDate32 = Info.primitiveInfo(ArrowTypeId.date32)
    public static let ArrowDate64 = Info.primitiveInfo(ArrowTypeId.date64)
    public static let ArrowBinary = Info.variableInfo(ArrowTypeId.binary)
    public static let ArrowTime32 = Info.timeInfo(ArrowTypeId.time32)
    public static let ArrowTime64 = Info.timeInfo(ArrowTypeId.time64)

    public init(_ info: ArrowType.Info) {
        self.info = info
    }

    public var id: ArrowTypeId {
        switch self.info {
        case .primitiveInfo(let id):
            return id
        case .timeInfo(let id):
            return id
        case .variableInfo(let id):
            return id
        }
    }

    public enum Info {
        case primitiveInfo(ArrowTypeId)
        case variableInfo(ArrowTypeId)
        case timeInfo(ArrowTypeId)
    }

    public static func infoForNumericType<T>(_ type: T.Type) -> ArrowType.Info {
        if type == Int8.self {
            return ArrowType.ArrowInt8
        } else if type == Int16.self {
            return ArrowType.ArrowInt16
        } else if type == Int32.self {
            return ArrowType.ArrowInt32
        } else if type == Int64.self {
            return ArrowType.ArrowInt64
        } else if type == UInt8.self {
            return ArrowType.ArrowUInt8
        } else if type == UInt16.self {
            return ArrowType.ArrowUInt16
        } else if type == UInt32.self {
            return ArrowType.ArrowUInt32
        } else if type == UInt64.self {
            return ArrowType.ArrowUInt64
        } else if type == Float.self {
            return ArrowType.ArrowFloat
        } else if type == Double.self {
            return ArrowType.ArrowDouble
        } else {
            return ArrowType.ArrowUnknown
        }
    }

    public func getStride( // swiftlint:disable:this cyclomatic_complexity
    ) -> Int {
        switch self.id {
        case .int8:
            return MemoryLayout<Int8>.stride
        case .int16:
            return MemoryLayout<Int16>.stride
        case .int32:
            return MemoryLayout<Int32>.stride
        case .int64:
            return MemoryLayout<Int64>.stride
        case .uint8:
            return MemoryLayout<UInt8>.stride
        case .uint16:
            return MemoryLayout<UInt16>.stride
        case .uint32:
            return MemoryLayout<UInt32>.stride
        case .uint64:
            return MemoryLayout<UInt64>.stride
        case .float:
            return MemoryLayout<Float>.stride
        case .double:
            return MemoryLayout<Double>.stride
        case .boolean:
            return MemoryLayout<Bool>.stride
        case .date32:
            return MemoryLayout<Date32>.stride
        case .date64:
            return MemoryLayout<Date64>.stride
        case .time32:
            return MemoryLayout<Time32>.stride
        case .time64:
            return MemoryLayout<Time64>.stride
        case .binary:
            return MemoryLayout<Int8>.stride
        case .string:
            return MemoryLayout<Int8>.stride
        default:
            fatalError("Stride requested for unknown type: \(self)")
        }
    }
}

extension ArrowType.Info: Equatable {
    public static func == (lhs: ArrowType.Info, rhs: ArrowType.Info) -> Bool {
        switch(lhs, rhs) {
        case (.primitiveInfo(let lhsId), .primitiveInfo(let rhsId)):
            return lhsId == rhsId
        case (.variableInfo(let lhsId), .variableInfo(let rhsId)):
            return lhsId == rhsId
        case (.timeInfo(let lhsId), .timeInfo(let rhsId)):
            return lhsId == rhsId
        default:
            return false
        }
    }
}

func getBytesFor<T>(_ data: T) -> Data? {
    if let temp = data as? String {
        return temp.data(using: .utf8)
    } else if T.self == Data.self {
        return data as? Data
    } else {
        return nil
    }
}
