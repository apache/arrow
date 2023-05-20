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
public class ChunkedArray<T> : AsString {
    public let arrays: [ArrowArray<T>]
    public let type: ArrowType.Info
    public let nullCount: UInt
    public let length: UInt
    public var arrayCount: UInt {get{return UInt(self.arrays.count)}}
    
    public init(_ arrays: [ArrowArray<T>]) throws {
        if arrays.count == 0 {
            throw ValidationError.arrayHasNoElements
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
        while localIndex > len {
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
