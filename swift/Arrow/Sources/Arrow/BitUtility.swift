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

class BitUtility {
    static func isSet(_ bit: UInt, buffer: ArrowBuffer) -> Bool {
        let byteIndex = UInt(bit / 8)
        let theByte = buffer.rawPointer.load(fromByteOffset: Int(byteIndex), as: UInt8.self)
        return theByte & UInt8(1 << (bit % 8)) > 0
    }

    static func setBit(_ bit: UInt, buffer: ArrowBuffer) {
        let byteIndex = UInt(bit / 8)
        var theByte = buffer.rawPointer.load(fromByteOffset: Int(byteIndex), as: UInt8.self)
        theByte |= UInt8(1 << (bit % 8))
        buffer.rawPointer.storeBytes(of: theByte, toByteOffset: Int(byteIndex), as: UInt8.self)
    }

    static func clearBit(_ bit: UInt, buffer: ArrowBuffer) {
        let byteIndex = UInt(bit / 8)
        var theByte = buffer.rawPointer.load(fromByteOffset: Int(byteIndex), as: UInt8.self)
        theByte &= ~(UInt8(1 << (bit % 8)))
        buffer.rawPointer.storeBytes(of: theByte, toByteOffset: Int(byteIndex), as: UInt8.self)
    }
}

