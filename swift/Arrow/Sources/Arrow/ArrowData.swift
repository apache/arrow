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

public class ArrowData {
    let type: ArrowType.Info
    let buffers: [ArrowBuffer]
    let nullCount: UInt
    let length: UInt
    let stride: Int

    init(_ type: ArrowType.Info, buffers: [ArrowBuffer], nullCount: UInt, stride: Int) throws {
        switch(type) {
            case let .PrimitiveInfo(typeId):
                if typeId == ArrowTypeId.Unknown {
                    throw ValidationError.unknownType
                }
            case let .VariableInfo(typeId):
                if typeId == ArrowTypeId.Unknown {
                    throw ValidationError.unknownType
                }
        }

        self.type = type
        self.buffers = buffers
        self.nullCount = nullCount
        self.length = buffers[1].length
        self.stride = stride
    }

    func isNull(_ at: UInt) -> Bool {
        let nullBuffer = buffers[0];
        return nullBuffer.length == 0 || BitUtility.isSet(at, buffer: nullBuffer)
    }
}
