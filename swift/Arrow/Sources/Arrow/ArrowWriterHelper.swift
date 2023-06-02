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

func toFBTypeEnum(_ infoType: ArrowType.Info) -> Result<org_apache_arrow_flatbuf_Type_, ArrowError> {
    if infoType == ArrowType.ArrowInt8 || infoType == ArrowType.ArrowInt16 ||
        infoType == ArrowType.ArrowInt64 || infoType == ArrowType.ArrowUInt8 ||
        infoType == ArrowType.ArrowUInt16 || infoType == ArrowType.ArrowUInt32 ||
        infoType == ArrowType.ArrowUInt64 || infoType == ArrowType.ArrowInt32 {
        return .success(org_apache_arrow_flatbuf_Type_.int)
    } else if infoType == ArrowType.ArrowFloat || infoType == ArrowType.ArrowDouble {
        return .success(org_apache_arrow_flatbuf_Type_.floatingpoint)
    } else if infoType == ArrowType.ArrowString {
        return .success(org_apache_arrow_flatbuf_Type_.utf8)
    } else if infoType == ArrowType.ArrowBool {
        return .success(org_apache_arrow_flatbuf_Type_.bool)
    } else if infoType == ArrowType.ArrowDate32 || infoType == ArrowType.ArrowDate64 {
        return .success(org_apache_arrow_flatbuf_Type_.date)
    }

    return .failure(.unknownType)
}

func toFBType(_ fbb: inout FlatBufferBuilder, infoType: ArrowType.Info) -> Result<Offset, ArrowError> {
    if infoType == ArrowType.ArrowInt8 || infoType == ArrowType.ArrowUInt8 {
        return .success(org_apache_arrow_flatbuf_Int.createInt(&fbb, bitWidth: 8, isSigned: infoType == ArrowType.ArrowInt8))
    } else if infoType == ArrowType.ArrowInt16 || infoType == ArrowType.ArrowUInt16 {
        return .success(org_apache_arrow_flatbuf_Int.createInt(&fbb, bitWidth: 16, isSigned: infoType == ArrowType.ArrowInt16))
    } else if infoType == ArrowType.ArrowInt32 || infoType == ArrowType.ArrowUInt32 {
        return .success(org_apache_arrow_flatbuf_Int.createInt(&fbb, bitWidth: 32, isSigned: infoType == ArrowType.ArrowInt32))
    } else if infoType == ArrowType.ArrowInt64 || infoType == ArrowType.ArrowUInt64 {
        return .success(org_apache_arrow_flatbuf_Int.createInt(&fbb, bitWidth: 64, isSigned: infoType == ArrowType.ArrowInt64))
    } else if infoType == ArrowType.ArrowFloat {
        return .success(org_apache_arrow_flatbuf_FloatingPoint.createFloatingPoint(&fbb, precision: .single))
    } else if infoType == ArrowType.ArrowDouble {
        return .success(org_apache_arrow_flatbuf_FloatingPoint.createFloatingPoint(&fbb, precision: .double))
    } else if infoType == ArrowType.ArrowString {
        return .success(org_apache_arrow_flatbuf_Utf8.endUtf8(&fbb, start: org_apache_arrow_flatbuf_Utf8.startUtf8(&fbb)))
    } else if infoType == ArrowType.ArrowBool {
        return .success(org_apache_arrow_flatbuf_Bool.endBool(&fbb, start: org_apache_arrow_flatbuf_Bool.startBool(&fbb)))
    } else if infoType == ArrowType.ArrowDate32 {
        let startOffset = org_apache_arrow_flatbuf_Date.startDate(&fbb)
        org_apache_arrow_flatbuf_Date.add(unit: .day, &fbb)
        return .success(org_apache_arrow_flatbuf_Date.endDate(&fbb, start: startOffset))
    } else if infoType == ArrowType.ArrowDate64 {
        let startOffset = org_apache_arrow_flatbuf_Date.startDate(&fbb)
        org_apache_arrow_flatbuf_Date.add(unit: .millisecond, &fbb)
        return .success(org_apache_arrow_flatbuf_Date.endDate(&fbb, start: startOffset))
    }
    
    return .failure(.unknownType)
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
