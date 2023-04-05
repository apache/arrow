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

public class ChunkedArray<T> {
    let arrays: [ArrowArray<T>]
    let type: ArrowType.Info
    let nullCount: UInt
    let length: UInt
    var arrayCount: UInt {get{return UInt(self.arrays.count)}}
    
    init(_ arrays: [ArrowArray<T>]) throws {
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
    
    subscript(_ index: UInt) -> ArrowArray<T> {
        return arrays[Int(index)]
    }
    
}
