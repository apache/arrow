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

func fromProto( // swiftlint:disable:this cyclomatic_complexity
    field: org_apache_arrow_flatbuf_Field
) -> ArrowField {
    let type = field.typeType
    var arrowType = ArrowType(ArrowType.ArrowUnknown)
    switch type {
    case .int:
        let intType = field.type(type: org_apache_arrow_flatbuf_Int.self)!
        let bitWidth = intType.bitWidth
        if bitWidth == 8 {
            arrowType = ArrowType(intType.isSigned ? ArrowType.ArrowInt8 : ArrowType.ArrowUInt8)
        } else if bitWidth == 16 {
            arrowType = ArrowType(intType.isSigned ? ArrowType.ArrowInt16 : ArrowType.ArrowUInt16)
        } else if bitWidth == 32 {
            arrowType = ArrowType(intType.isSigned ? ArrowType.ArrowInt32 : ArrowType.ArrowUInt32)
        } else if bitWidth == 64 {
            arrowType = ArrowType(intType.isSigned ? ArrowType.ArrowInt64 : ArrowType.ArrowUInt64)
        }
    case .bool:
        arrowType = ArrowType(ArrowType.ArrowBool)
    case .floatingpoint:
        let floatType = field.type(type: org_apache_arrow_flatbuf_FloatingPoint.self)!
        if floatType.precision == .single {
            arrowType = ArrowType(ArrowType.ArrowFloat)
        } else if floatType.precision == .double {
            arrowType = ArrowType(ArrowType.ArrowDouble)
        }
    case .utf8:
        arrowType = ArrowType(ArrowType.ArrowString)
    case .binary:
        arrowType = ArrowType(ArrowType.ArrowBinary)
    case .date:
        let dateType = field.type(type: org_apache_arrow_flatbuf_Date.self)!
        if dateType.unit == .day {
            arrowType = ArrowType(ArrowType.ArrowDate32)
        } else {
            arrowType = ArrowType(ArrowType.ArrowDate64)
        }
    case .time:
        let timeType = field.type(type: org_apache_arrow_flatbuf_Time.self)!
        if timeType.unit == .second || timeType.unit == .millisecond {
            let arrowUnit: ArrowTime32Unit = timeType.unit == .second ? .seconds : .milliseconds
            arrowType = ArrowTypeTime32(arrowUnit)
        } else {
            let arrowUnit: ArrowTime64Unit = timeType.unit == .microsecond ? .microseconds : .nanoseconds
            arrowType = ArrowTypeTime64(arrowUnit)
        }
    default:
        arrowType = ArrowType(ArrowType.ArrowUnknown)
    }

    return ArrowField(field.name ?? "", type: arrowType, isNullable: field.nullable)
}
