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

public class ArrowBuffer {
    static let minLength: UInt = 1 << 5
    static let maxLength = UInt.max
    fileprivate(set) var length: UInt
    let capacity: UInt
    public let rawPointer: UnsafeMutableRawPointer
    let isMemoryOwner: Bool

    init(length: UInt, capacity: UInt, rawPointer: UnsafeMutableRawPointer, isMemoryOwner: Bool = true) {
        self.length = length
        self.capacity = capacity
        self.rawPointer = rawPointer
        self.isMemoryOwner = isMemoryOwner
    }

    deinit {
        if isMemoryOwner {
            self.rawPointer.deallocate()
        }
    }

    func append(to data: inout Data) {
        let ptr  = UnsafePointer(rawPointer.assumingMemoryBound(to: UInt8.self))
        data.append(ptr, count: Int(capacity))
    }

    static func createEmptyBuffer() -> ArrowBuffer {
        return ArrowBuffer(
            length: 0,
            capacity: 0,
            rawPointer: UnsafeMutableRawPointer.allocate(byteCount: 0, alignment: .zero))
    }

    static func createBuffer(_ data: [UInt8], length: UInt) -> ArrowBuffer {
        let byteCount = UInt(data.count)
        let capacity = alignTo64(byteCount)
        let memory = MemoryAllocator(64)
        let rawPointer = memory.allocateArray(Int(capacity))
        rawPointer.copyMemory(from: data, byteCount: data.count)
        return ArrowBuffer(length: length, capacity: capacity, rawPointer: rawPointer)
    }

    static func createBuffer(_ length: UInt, size: UInt, doAlign: Bool = true) -> ArrowBuffer {
        let actualLen = max(length, ArrowBuffer.minLength)
        let byteCount = size * actualLen
        var capacity = byteCount
        if doAlign {
            capacity = alignTo64(byteCount)
        }

        let memory = MemoryAllocator(64)
        let rawPointer = memory.allocateArray(Int(capacity))
        rawPointer.initializeMemory(as: UInt8.self, repeating: 0, count: Int(capacity))
        return ArrowBuffer(length: length, capacity: capacity, rawPointer: rawPointer)
    }

    static func copyCurrent(_ from: ArrowBuffer, to: inout ArrowBuffer, len: UInt) {
        to.rawPointer.copyMemory(from: from.rawPointer, byteCount: Int(len))
    }

    private static func alignTo64(_ length: UInt) -> UInt {
        let bufAlignment = length % 64
        if bufAlignment != 0 {
            return length + (64 - bufAlignment) + 8
        }

        return length + 8
    }
}
