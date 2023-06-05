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

func FlatBuffersVersion_23_1_4() {
}
                                                                                                                
public enum ArrowError: Error {
    case none
    case unknownType
    case runtimeError(String)
    case outOfBounds(index: Int64)
    case arrayHasNoElements
    case unknownError(String)
    case notImplemented
    case ioError(String)
    case invalid(String)
}

public enum ArrowTypeId {
    case Binary
    case Boolean
    case Date32
    case Date64
    case DateType
    case Decimal128
    case Decimal256
    case Dictionary
    case Double
    case FixedSizeBinary
    case FixedWidthType
    case Float
    //case HalfFloatType
    case Int16
    case Int32
    case Int64
    case Int8
    case Integer
    case IntervalUnit
    case List
    case Nested
    case Null
    case Number
    case String
    case Struct
    case Time32
    case Time64
    case Time
    case UInt16
    case UInt32
    case UInt64
    case UInt8
    case Union
    case Unknown
}

public class ArrowType {
    public static let ArrowInt8 = Info.PrimitiveInfo(ArrowTypeId.Int8)
    public static let ArrowInt16 = Info.PrimitiveInfo(ArrowTypeId.Int16)
    public static let ArrowInt32 = Info.PrimitiveInfo(ArrowTypeId.Int32)
    public static let ArrowInt64 = Info.PrimitiveInfo(ArrowTypeId.Int64)
    public static let ArrowUInt8 = Info.PrimitiveInfo(ArrowTypeId.UInt8)
    public static let ArrowUInt16 = Info.PrimitiveInfo(ArrowTypeId.UInt16)
    public static let ArrowUInt32 = Info.PrimitiveInfo(ArrowTypeId.UInt32)
    public static let ArrowUInt64 = Info.PrimitiveInfo(ArrowTypeId.UInt64)
    public static let ArrowFloat = Info.PrimitiveInfo(ArrowTypeId.Float)
    public static let ArrowDouble = Info.PrimitiveInfo(ArrowTypeId.Double)
    public static let ArrowUnknown = Info.PrimitiveInfo(ArrowTypeId.Unknown)
    public static let ArrowString = Info.VariableInfo(ArrowTypeId.String)
    public static let ArrowBool = Info.PrimitiveInfo(ArrowTypeId.Boolean)
    public static let ArrowDate32 = Info.PrimitiveInfo(ArrowTypeId.Date32)
    public static let ArrowDate64 = Info.PrimitiveInfo(ArrowTypeId.Date64)

    public enum Info {
        case PrimitiveInfo(ArrowTypeId)
        case VariableInfo(ArrowTypeId)
    }

    public static func infoForNumericType<T>(_ t: T.Type) -> ArrowType.Info {
        if t == Int8.self {
            return ArrowType.ArrowInt8
        }else if t == Int16.self {
            return ArrowType.ArrowInt16
        }else if t == Int32.self {
            return ArrowType.ArrowInt32
        }else if t == Int64.self {
            return ArrowType.ArrowInt64
        }else if t == UInt8.self {
            return ArrowType.ArrowUInt8
        }else if t == UInt16.self {
            return ArrowType.ArrowUInt16
        }else if t == UInt32.self {
            return ArrowType.ArrowUInt32
        }else if t == UInt64.self {
            return ArrowType.ArrowUInt64
        }else if t == Float.self {
            return ArrowType.ArrowFloat
        }else if t == Double.self {
            return ArrowType.ArrowDouble
        }else {
            return ArrowType.ArrowUnknown
        }
    }
}

extension ArrowType.Info: Equatable {
    public static func==(lhs: ArrowType.Info, rhs: ArrowType.Info) -> Bool {
        switch(lhs, rhs) {
            case (.PrimitiveInfo(let lhsId), .PrimitiveInfo(let rhsId)):
                return lhsId == rhsId
            case (.VariableInfo(let lhsId), .VariableInfo(let rhsId)):
                return lhsId == rhsId
            case (.VariableInfo(_), .PrimitiveInfo(_)), (.PrimitiveInfo(_), .VariableInfo(_)):
                return false
        }
    }
}

func getBytesFor<T>(_ data: T) -> Data? {
    let t = T.self
    if t == String.self {
        let temp = data as! String
        return temp.data(using: .utf8);
    }else {
        return nil
    }
}
