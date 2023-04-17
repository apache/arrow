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

public protocol ArrowBufferBuilder {
    associatedtype ItemType
    var capacity: UInt {get}
    var length: UInt {get}
    var nullCount : UInt {get}
    var offset: UInt {get}
    init() throws
    func append(_ newValue: ItemType?)
    func isNull(_ index: UInt) -> Bool
    func resize(_ length: UInt)
    func finish() -> [ArrowBuffer]
}

public class BaseBufferBuilder<T> {
    var values: ArrowBuffer
    var nulls: ArrowBuffer
    var stride: Int;
    public var offset: UInt = 0
    public var capacity: UInt {get{return self.values.capacity}}
    public var length: UInt = 0
    public var nullCount : UInt  = 0

    init(values: ArrowBuffer, nulls: ArrowBuffer, stride: Int = MemoryLayout<T>.stride) throws {
        self.stride = stride
        self.values = values
        self.nulls = nulls
    }

    public func isNull(_ index: UInt) -> Bool {
        return self.nulls.length == 0 || BitUtility.isSet(index + self.offset, buffer: self.nulls)
    }

    func resizeLength(_ data: ArrowBuffer, len: UInt = 0) -> UInt {
        if len == 0 || len < data.length * 2 {
            return UInt(data.length * 2);
        }
        
        return UInt(len * 2);
    }
}

public class FixedBufferBuilder<T>: BaseBufferBuilder<T>, ArrowBufferBuilder {
    public typealias ItemType = T
    private let defaultVal: ItemType
    public required init() throws {
        self.defaultVal = try FixedBufferBuilder<T>.defaultValueForType()
        let values = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<T>.stride))
        let nulls = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<UInt8>.stride))
        try super.init(values: values, nulls: nulls)
    }

    public func append(_ newValue: ItemType?) {
        let index = UInt(self.length)
        let byteIndex = self.stride * Int(index)
        self.length += 1
        if length > self.values.length {
            self.resize(length)
        }

        if let val = newValue {
            self.values.rawPointer.advanced(by: byteIndex).storeBytes(of: val, as: T.self)
        } else {
            self.nullCount += 1
            BitUtility.setBit(index + self.offset, buffer: self.nulls)
            self.values.rawPointer.advanced(by: byteIndex).storeBytes(of: defaultVal, as: T.self)
        }
    }

    public func resize(_ length: UInt) {
        if length > self.values.length {
            let resizeLength = resizeLength(self.values);
            var values = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<T>.size))
            var nulls = ArrowBuffer.createBuffer(resizeLength/8 + 1, size: UInt(MemoryLayout<UInt8>.size))
            ArrowBuffer.copyCurrent(self.values, to: &values, len: self.values.capacity)
            ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
            self.values = values;  
            self.nulls = nulls;
        }
    }

    public func finish() -> [ArrowBuffer] {
        let length = self.length
        var values = ArrowBuffer.createBuffer(length, size: UInt(MemoryLayout<T>.size))
        var nulls = ArrowBuffer.createBuffer(length/8 + 1, size: UInt(MemoryLayout<UInt8>.size))
        ArrowBuffer.copyCurrent(self.values, to: &values, len: values.capacity)
        ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
        return [nulls, values]
    }
    
    fileprivate static func defaultValueForType() throws -> T {
        let t = T.self
        if t == Int8.self {
            return Int8(0) as! T
        }else if t == Int16.self {
            return Int16(0) as! T
        }else if t == Int32.self {
            return Int32(0) as! T
        }else if t == Int64.self {
            return Int64(0) as! T
        }else if t == UInt8.self {
            return UInt8(0) as! T
        }else if t == UInt16.self {
            return UInt16(0) as! T
        }else if t == UInt32.self {
            return UInt32(0) as! T
        }else if t == UInt64.self {
            return UInt64(0) as! T
        }else if t == Float.self {
            return Float(0) as! T
        }else if t == Double.self {
            return Double(0) as! T
        }
        
        throw ValidationError.unknownType
    }
}

public class BoolBufferBuilder: BaseBufferBuilder<Bool>, ArrowBufferBuilder {
    public typealias ItemType = Bool
    public required init() throws {
        let values = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<UInt8>.stride))
        let nulls = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<UInt8>.stride))
        try super.init(values: values, nulls: nulls)
    }

    public func append(_ newValue: ItemType?) {
        let index = UInt(self.length)
        self.length += 1
        if (length/8) > self.values.length {
            self.resize(length)
        }

        if newValue != nil {
            if newValue == true {
                BitUtility.setBit(index + self.offset, buffer: self.values)
            } else {
                BitUtility.clearBit(index + self.offset, buffer: self.values)
            }
            
        } else {
            self.nullCount += 1
            BitUtility.setBit(index + self.offset, buffer: self.nulls)
            BitUtility.clearBit(index + self.offset, buffer: self.values)
        }
    }

    public func resize(_ length: UInt) {
        if (length/8) > self.values.length {
            let resizeLength = resizeLength(self.values)
            var values = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<UInt8>.size))
            var nulls = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<UInt8>.size))
            ArrowBuffer.copyCurrent(self.values, to: &values, len: self.values.capacity)
            ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
            self.values = values;
            self.nulls = nulls;
        }
    }

    public func finish() -> [ArrowBuffer] {
        let length = self.length
        var values = ArrowBuffer.createBuffer(length, size: UInt(MemoryLayout<UInt8>.size))
        var nulls = ArrowBuffer.createBuffer(length, size: UInt(MemoryLayout<UInt8>.size))
        ArrowBuffer.copyCurrent(self.values, to: &values, len: values.capacity)
        ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
        return [nulls, values]
    }
}

public class VariableBufferBuilder<T>: BaseBufferBuilder<T>, ArrowBufferBuilder {
    public typealias ItemType = T    
    var offsets: ArrowBuffer
    let binaryStride = MemoryLayout<UInt8>.stride
    public required init() throws {
        let values = ArrowBuffer.createBuffer(0, size: UInt(binaryStride))
        let nulls = ArrowBuffer.createBuffer(0, size: UInt(binaryStride))
        self.offsets = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<Int32>.stride))
        try super.init(values: values, nulls: nulls, stride: binaryStride)
    }

    public func append(_ newValue: ItemType?) {
        let index = UInt(self.length)
        self.length += 1
        let offsetIndex = MemoryLayout<Int32>.stride * Int(index)
        if self.length >= self.offsets.length {
            self.resize(UInt( self.offsets.length + 1))
        }
        var binData: Data
        var isNull = false
        if let val = newValue {
            binData = getBytesFor(val)!
        }else {
            var nullVal = 0
            isNull = true
            binData = Data(bytes: &nullVal, count: MemoryLayout<UInt32>.size)
        }

        var currentIndex: Int32 = 0;
        var currentOffset: Int32 = Int32(binData.count);
        if index > 0 {
            currentIndex = self.offsets.rawPointer.advanced(by: offsetIndex).load(as: Int32.self)
            currentOffset += currentIndex
            if currentOffset > self.values.length {
                self.value_resize(UInt(currentOffset))
            }
        }

        if isNull {
            self.nullCount += 1
            BitUtility.setBit(index + self.offset, buffer: self.nulls)
        }

        binData.withUnsafeBytes { bufferPointer in
            let rawPointer = bufferPointer.baseAddress!
             self.values.rawPointer.advanced(by: Int(currentIndex)).copyMemory(from: rawPointer, byteCount: binData.count)
        }

        self.offsets.rawPointer.advanced(by: (offsetIndex + MemoryLayout<Int32>.stride)).storeBytes(of: currentOffset, as: Int32.self)
    }

    public func value_resize(_ length: UInt) {
        if length > self.values.length {
            let resizeLength = resizeLength(self.values, len: length);
            var values = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<UInt8>.size))
            ArrowBuffer.copyCurrent(self.values, to: &values, len: self.values.capacity)
            self.values = values;
        }
    }
    
    public func resize(_ length: UInt) {
        if length > self.offsets.length {
            let resizeLength = resizeLength(self.offsets, len: length);
            var nulls = ArrowBuffer.createBuffer(resizeLength/8 + 1, size: UInt(MemoryLayout<UInt8>.size))
            var offsets = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<Int32>.size))
            ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
            ArrowBuffer.copyCurrent(self.offsets, to: &offsets, len: self.offsets.capacity)
            self.nulls = nulls;
            self.offsets = offsets;
        }
    }

    public func finish() -> [ArrowBuffer] {
        let length = self.length
        var values = ArrowBuffer.createBuffer(self.values.length, size: UInt(MemoryLayout<UInt8>.size))
        var nulls = ArrowBuffer.createBuffer(length/8 + 1, size: UInt(MemoryLayout<UInt8>.size))
        var offsets = ArrowBuffer.createBuffer(length, size: UInt(MemoryLayout<Int32>.size))
        ArrowBuffer.copyCurrent(self.values, to: &values, len: values.capacity)
        ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
        ArrowBuffer.copyCurrent(self.offsets, to: &offsets, len: offsets.capacity)
        return [nulls, offsets, values]
    }
}

public class Date32BufferBuilder: ArrowBufferBuilder {
    public typealias ItemType = Date
    public var capacity: UInt {get{return self.bufferBuilder.capacity}}
    public var length: UInt {get{return self.bufferBuilder.length}}
    public var nullCount : UInt {get{return self.bufferBuilder.nullCount}}
    public var offset: UInt {get{return self.bufferBuilder.offset}}
    private let bufferBuilder: FixedBufferBuilder<Int32>
    public required init() throws {
        self.bufferBuilder = try FixedBufferBuilder()
    }

    public func append(_ newValue: ItemType?) {
        if let val = newValue {
            let daysSinceEpoch = Int32(val.timeIntervalSince1970 / 86400)
            self.bufferBuilder.append(daysSinceEpoch)
        } else {
            self.bufferBuilder.append(nil)
        }
    }

    public func isNull(_ index: UInt) -> Bool {
        return self.bufferBuilder.isNull(index)
    }
    
    public func resize(_ length: UInt) {
        self.bufferBuilder.resize(length)
    }
    
    public func finish() -> [ArrowBuffer] {
        return self.bufferBuilder.finish()
    }
}

public class Date64BufferBuilder: ArrowBufferBuilder {
    public typealias ItemType = Date
    public var capacity: UInt {get{return self.bufferBuilder.capacity}}
    public var length: UInt {get{return self.bufferBuilder.length}}
    public var nullCount : UInt {get{return self.bufferBuilder.nullCount}}
    public var offset: UInt {get{return self.bufferBuilder.offset}}
    private let bufferBuilder: FixedBufferBuilder<Int64>
    public required init() throws {
        self.bufferBuilder = try FixedBufferBuilder()
    }

    public func append(_ newValue: ItemType?) {
        if let val = newValue {
            let daysSinceEpoch = Int64(val.timeIntervalSince1970 * 1000)
            self.bufferBuilder.append(daysSinceEpoch)
        } else {
            self.bufferBuilder.append(nil)
        }
    }

    public func isNull(_ index: UInt) -> Bool {
        return self.bufferBuilder.isNull(index)
    }
    
    public func resize(_ length: UInt) {
        self.bufferBuilder.resize(length)
    }
    
    public func finish() -> [ArrowBuffer] {
        return self.bufferBuilder.finish()
    }
}
