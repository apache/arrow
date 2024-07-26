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
import FlatBuffers

extension Data {
    func hexEncodedString() -> String {
        return map { String(format: "%02hhx", $0) }.joined()
    }
}

func toFBTypeEnum(_ arrowType: ArrowType) -> Result<org_apache_arrow_flatbuf_Type_, ArrowError> {
    let typeId = arrowType.id
    switch typeId {
    case .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64:
        return .success(org_apache_arrow_flatbuf_Type_.int)
    case .float, .double:
        return .success(org_apache_arrow_flatbuf_Type_.floatingpoint)
    case .string:
        return .success(org_apache_arrow_flatbuf_Type_.utf8)
    case .binary:
        return .success(org_apache_arrow_flatbuf_Type_.binary)
    case .boolean:
        return .success(org_apache_arrow_flatbuf_Type_.bool)
    case .date32, .date64:
        return .success(org_apache_arrow_flatbuf_Type_.date)
    case .time32, .time64:
        return .success(org_apache_arrow_flatbuf_Type_.time)
    case .strct:
        return .success(org_apache_arrow_flatbuf_Type_.struct_)
    default:
        return .failure(.unknownType("Unable to find flatbuf type for Arrow type: \(typeId)"))
    }
}

func toFBType( // swiftlint:disable:this cyclomatic_complexity function_body_length
    _ fbb: inout FlatBufferBuilder,
    arrowType: ArrowType
) -> Result<Offset, ArrowError> {
    let infoType = arrowType.info
    switch arrowType.id {
    case .int8, .uint8:
        return .success(org_apache_arrow_flatbuf_Int.createInt(
            &fbb, bitWidth: 8, isSigned: infoType == ArrowType.ArrowInt8))
    case .int16, .uint16:
        return .success(org_apache_arrow_flatbuf_Int.createInt(
            &fbb, bitWidth: 16, isSigned: infoType == ArrowType.ArrowInt16))
    case .int32, .uint32:
        return .success(org_apache_arrow_flatbuf_Int.createInt(
            &fbb, bitWidth: 32, isSigned: infoType == ArrowType.ArrowInt32))
    case .int64, .uint64:
        return .success(org_apache_arrow_flatbuf_Int.createInt(
            &fbb, bitWidth: 64, isSigned: infoType == ArrowType.ArrowInt64))
    case .float:
        return .success(org_apache_arrow_flatbuf_FloatingPoint.createFloatingPoint(&fbb, precision: .single))
    case .double:
        return .success(org_apache_arrow_flatbuf_FloatingPoint.createFloatingPoint(&fbb, precision: .double))
    case .string:
        return .success(org_apache_arrow_flatbuf_Utf8.endUtf8(
            &fbb, start: org_apache_arrow_flatbuf_Utf8.startUtf8(&fbb)))
    case .binary:
        return .success(org_apache_arrow_flatbuf_Binary.endBinary(
            &fbb, start: org_apache_arrow_flatbuf_Binary.startBinary(&fbb)))
    case .boolean:
        return .success(org_apache_arrow_flatbuf_Bool.endBool(
            &fbb, start: org_apache_arrow_flatbuf_Bool.startBool(&fbb)))
    case .date32:
        let startOffset = org_apache_arrow_flatbuf_Date.startDate(&fbb)
        org_apache_arrow_flatbuf_Date.add(unit: .day, &fbb)
        return .success(org_apache_arrow_flatbuf_Date.endDate(&fbb, start: startOffset))
    case .date64:
        let startOffset = org_apache_arrow_flatbuf_Date.startDate(&fbb)
        org_apache_arrow_flatbuf_Date.add(unit: .millisecond, &fbb)
        return .success(org_apache_arrow_flatbuf_Date.endDate(&fbb, start: startOffset))
    case .time32:
        let startOffset = org_apache_arrow_flatbuf_Time.startTime(&fbb)
        if let timeType = arrowType as? ArrowTypeTime32 {
            org_apache_arrow_flatbuf_Time.add(unit: timeType.unit == .seconds ? .second : .millisecond, &fbb)
            return .success(org_apache_arrow_flatbuf_Time.endTime(&fbb, start: startOffset))
        }

        return .failure(.invalid("Unable to case to Time32"))
    case .time64:
        let startOffset = org_apache_arrow_flatbuf_Time.startTime(&fbb)
        if let timeType = arrowType as? ArrowTypeTime64 {
            org_apache_arrow_flatbuf_Time.add(unit: timeType.unit == .microseconds ? .microsecond : .nanosecond, &fbb)
            return .success(org_apache_arrow_flatbuf_Time.endTime(&fbb, start: startOffset))
        }

        return .failure(.invalid("Unable to case to Time64"))
    case .strct:
        let startOffset = org_apache_arrow_flatbuf_Struct_.startStruct_(&fbb)
        return .success(org_apache_arrow_flatbuf_Struct_.endStruct_(&fbb, start: startOffset))
    default:
        return .failure(.unknownType("Unable to add flatbuf type for Arrow type: \(infoType)"))
    }
}

func addPadForAlignment(_ data: inout Data, alignment: Int = 8) {
    let padding = data.count % Int(alignment)
    if padding > 0 {
        data.append(Data([UInt8](repeating: 0, count: alignment - padding)))
    }
}

func addPadForAlignment(_ writer: inout DataWriter, alignment: Int = 8) {
    let padding = writer.count % Int(alignment)
    if padding > 0 {
        writer.append(Data([UInt8](repeating: 0, count: alignment - padding)))
    }
}

func getPadForAlignment(_ count: Int, alignment: Int = 8) -> Int {
    let padding = count % Int(alignment)
    if padding > 0 {
        return count + (alignment - padding)
    }

    return count
}
