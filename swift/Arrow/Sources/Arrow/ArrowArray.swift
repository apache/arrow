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

public class ArrowArrayHolder {
    public let type: ArrowType.Info
    public let length: UInt
    public let nullCount: UInt
    public let array: Any
    public let getBufferData: () -> [Data]
    public let getBufferDataSizes: () -> [Int]
    private let getArrowColumn: (ArrowField, [ArrowArrayHolder]) throws -> ArrowColumn
    public init<T>(_ arrowArray: ArrowArray<T>) {
        self.array = arrowArray
        self.length = arrowArray.length
        self.type = arrowArray.arrowData.type
        self.nullCount = arrowArray.nullCount
        self.getBufferData = {() -> [Data] in
            var bufferData = [Data]()
            for buffer in arrowArray.arrowData.buffers {
                bufferData.append(Data())
                buffer.append(to: &bufferData[bufferData.count - 1])
            }

            return bufferData;
        }
        
        self.getBufferDataSizes = {() -> [Int] in
            var bufferDataSizes = [Int]()
            for buffer in arrowArray.arrowData.buffers {
                bufferDataSizes.append(Int(buffer.capacity))
            }

            return bufferDataSizes
        }
        
        self.getArrowColumn = {(field: ArrowField, arrayHolders: [ArrowArrayHolder]) throws -> ArrowColumn in
            var arrays = [ArrowArray<T>]()
            for arrayHolder in arrayHolders {
                if let array = arrayHolder.array as? ArrowArray<T> {
                    arrays.append(array)
                }
            }
            
            return ArrowColumn(field, chunked: ChunkedArrayHolder(try ChunkedArray<T>(arrays)))
        }
    }
    
    public static func makeArrowColumn(_ field: ArrowField, holders: [ArrowArrayHolder]) -> Result<ArrowColumn, ArrowError> {
        do {
            return .success(try holders[0].getArrowColumn(field, holders))
        } catch {
            return .failure(.runtimeError("\(error)"))
        }
    }
}

public class ArrowArray<T>: AsString {
    public typealias ItemType = T
    public let arrowData: ArrowData
    public var nullCount : UInt {get{return self.arrowData.nullCount}}
    public var length: UInt {get{return self.arrowData.length}}
    
    public required init(_ arrowData: ArrowData) {
        self.arrowData = arrowData
    }

    public func isNull(_ at: UInt) throws -> Bool {
        if at >= self.length {
            throw ArrowError.outOfBounds(index: Int64(at))
        }
        
        return self.arrowData.isNull(at)
    }

    public subscript(_ index: UInt) -> T? {
        get{
            fatalError("subscript() has not been implemented")
        }
    }
    
    public func asString(_ index: UInt) -> String {
        if self[index] == nil {
            return ""
        }
        
        return "\(self[index]!)"
    }
}

public class FixedArray<T>: ArrowArray<T> {
    public override subscript(_ index: UInt) -> T? {
        get{
            if self.arrowData.isNull(index) {
                return nil
            }
            
            let byteOffset = self.arrowData.stride * Int(index);
            return self.arrowData.buffers[1].rawPointer.advanced(by: byteOffset).load(as: T.self)
        }
    }
}

public class StringArray: ArrowArray<String> {
    public override subscript(_ index: UInt) -> String? {
        get{
            let offsetIndex = MemoryLayout<Int32>.stride * Int(index)
            if self.arrowData.isNull(index) {
                return nil
            }
            
            let offsets = self.arrowData.buffers[1]
            let values = self.arrowData.buffers[2]

            var startIndex: Int32 = 0
            if index > 0 {
                startIndex = offsets.rawPointer.advanced(by: offsetIndex).load(as: Int32.self)    
            }

            let endIndex = offsets.rawPointer.advanced(by: offsetIndex + MemoryLayout<Int32>.stride ).load(as: Int32.self)
            let arrayLength = Int(endIndex - startIndex);
            let rawPointer =  values.rawPointer.advanced(by: Int(startIndex)).bindMemory(to: UInt8.self, capacity: arrayLength)
            let buffer = UnsafeBufferPointer<UInt8>(start: rawPointer, count: arrayLength);
            let byteArray = Array(buffer)
            return String(data: Data(byteArray), encoding: .utf8)
        }
    }
}

public class BoolArray: ArrowArray<Bool> {
    public override subscript(_ index: UInt) -> Bool? {
        get{
            if self.arrowData.isNull(index) {
                return nil
            }

            let valueBuffer = self.arrowData.buffers[1];
            return BitUtility.isSet(index, buffer: valueBuffer)
        }
    }
}

public class Date32Array: ArrowArray<Date> {
    public override subscript(_ index: UInt) -> Date? {
        get{
            if self.arrowData.isNull(index) {
                return nil
            }

            let byteOffset = self.arrowData.stride * Int(index);
            let milliseconds = self.arrowData.buffers[1].rawPointer.advanced(by: byteOffset).load(as: UInt32.self)
            return Date(timeIntervalSince1970: TimeInterval(milliseconds * 86400))
        }
    }
}

public class Date64Array: ArrowArray<Date> {
    public override subscript(_ index: UInt) -> Date? {
        get{
            if self.arrowData.isNull(index) {
                return nil
            }

            let byteOffset = self.arrowData.stride * Int(index);
            let milliseconds = self.arrowData.buffers[1].rawPointer.advanced(by: byteOffset).load(as: UInt64.self)
            return Date(timeIntervalSince1970: TimeInterval(milliseconds / 1000))
        }
    }
}
