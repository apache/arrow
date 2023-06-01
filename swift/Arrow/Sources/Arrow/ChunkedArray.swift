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

public protocol AsString {
    func asString(_ index: UInt) -> String
}

public class ChunkedArrayHolder {
    public let type: ArrowType.Info
    public let length: UInt
    public let nullCount: UInt
    public let holder: Any
    public let getBufferData: () -> Result<[Data], ArrowError>
    public let getBufferDataSizes: () -> Result<[Int], ArrowError>
    public init<T>(_ chunked: ChunkedArray<T>) {
        self.holder = chunked
        self.length = chunked.length
        self.type = chunked.type
        self.nullCount = chunked.nullCount
        self.getBufferData = {() -> Result<[Data], ArrowError> in
            var bufferData = [Data]()
            var numBuffers = 2;
            switch toFBTypeEnum(chunked.type) {
            case .success(let fbType):
                if !isFixedPrimitive(fbType) {
                    numBuffers = 3
                }
            case .failure(let error):
                return .failure(error)
            }

            for _ in 0 ..< numBuffers {
                bufferData.append(Data())
            }

            for arrow_data in chunked.arrays {
                for index in 0 ..< numBuffers {
                    arrow_data.arrowData.buffers[index].append(to: &bufferData[index])
                }
            }

            return .success(bufferData);
        }
        
        self.getBufferDataSizes = {() -> Result<[Int], ArrowError> in
            var bufferDataSizes = [Int]()
            var numBuffers = 2;
            
            switch toFBTypeEnum(chunked.type) {
            case .success(let fbType):
                if !isFixedPrimitive(fbType) {
                    numBuffers = 3
                }
            case .failure(let error):
                return .failure(error)
            }
            
            for _ in 0 ..< numBuffers {
                bufferDataSizes.append(Int(0))
            }
            
            for arrow_data in chunked.arrays {
                for index in 0 ..< numBuffers {
                    bufferDataSizes[index] += Int(arrow_data.arrowData.buffers[index].capacity)
                }
            }

            return .success(bufferDataSizes)
        }

    }
}

public class ChunkedArray<T> : AsString {
    public let arrays: [ArrowArray<T>]
    public let type: ArrowType.Info
    public let nullCount: UInt
    public let length: UInt
    public var arrayCount: UInt {get{return UInt(self.arrays.count)}}
    
    public init(_ arrays: [ArrowArray<T>]) throws {
        if arrays.count == 0 {
            throw ArrowError.arrayHasNoElements
        }
        
        self.type = arrays[0].arrowData.type
        var len: UInt = 0
        var nullCount: UInt = 0
        for array in arrays {
            len += array.length
            nullCount += array.nullCount
        }
        
        self.arrays = arrays
        self.length = len
        self.nullCount = nullCount
    }
    
    public subscript(_ index: UInt) -> T? {
        if arrays.count == 0 {
            return nil
        }
        
        var localIndex = index
        var arrayIndex = 0;
        var len: UInt = arrays[arrayIndex].length
        while localIndex > (len - 1) {
            arrayIndex += 1
            if arrayIndex > arrays.count {
                return nil
            }
            
            localIndex -= len
            len = arrays[arrayIndex].length
        }
        
        return arrays[arrayIndex][localIndex]
    }
    
    public func asString(_ index: UInt) -> String {
        if self[index] == nil {
            return ""
        }
        
        return "\(self[index]!)"
    }
}
