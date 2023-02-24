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

public class ArrowArrayBuilder<T: ArrowBufferBuilder, U: ArrowArray<T.ItemType>> {
    let type: ArrowType.Info
    let bufferBuilder: T
    var length: UInt {get{return self.bufferBuilder.length}}
    var capacity: UInt {get{return self.bufferBuilder.capacity}}
    var nullCount : UInt {get{return self.bufferBuilder.nullCount}}
    var offset: UInt {get{return self.bufferBuilder.offset}}
    
    fileprivate init(_ type: ArrowType.Info) throws {
        self.type = type;
        self.bufferBuilder = try T()
    }

    func append(_ val: T.ItemType?) {
        self.bufferBuilder.append(val)
    }

    func finish() throws -> ArrowArray<T.ItemType> {
        let buffers = self.bufferBuilder.finish();
        let arrowData = try ArrowData(self.type, buffers: buffers, nullCount: self.nullCount, stride: self.getStride());
        return U(arrowData);
    }
    
    func getStride() -> Int {
        MemoryLayout<T.ItemType>.stride
    }
}

public class NumberArrayBuilder<T> : ArrowArrayBuilder<FixedBufferBuilder<T>, FixedArray<T>> {
    fileprivate convenience init() throws {
        try self.init(ArrowType.infoForNumericType(T.self));
    }
}

public class StringArrayBuilder : ArrowArrayBuilder<VariableBufferBuilder<String>, StringArray> {
    fileprivate convenience init() throws {
        try self.init(ArrowType.ArrowString);
    }
}

public class BoolArrayBuilder : ArrowArrayBuilder<BoolBufferBuilder, BoolArray> {
    fileprivate convenience init() throws {
        try self.init(ArrowType.ArrowBool);
    }
}

public class Date32ArrayBuilder : ArrowArrayBuilder<Date32BufferBuilder, Date32Array> {
    fileprivate convenience init() throws {
        try self.init(ArrowType.ArrowDate32)
    }
    
    override func getStride() -> Int {
        MemoryLayout<Int32>.stride
    }
}

public class Date64ArrayBuilder : ArrowArrayBuilder<Date64BufferBuilder, Date64Array> {
    fileprivate convenience init() throws {
        try self.init(ArrowType.ArrowDate64)
    }

    override func getStride() -> Int {
        MemoryLayout<Int64>.stride
    }
}

public class ArrowArrayBuilders {
    public static func loadNumberArrayBuilder<T>() throws -> NumberArrayBuilder<T> {
        let t = T.self
        if t == Int8.self {
            return try NumberArrayBuilder<T>()
        } else if t == Int16.self  {
            return try NumberArrayBuilder<T>()
        } else if t == Int32.self  {
            return try NumberArrayBuilder<T>()
        } else if t == Int64.self  {
            return try NumberArrayBuilder<T>()
        } else if t == UInt8.self  {
            return try NumberArrayBuilder<T>()
        } else if t == UInt16.self {
            return try NumberArrayBuilder<T>()
        } else if t == UInt32.self  {
            return try NumberArrayBuilder<T>()
        } else if t == UInt64.self {
            return try NumberArrayBuilder<T>()
        } else if t == Float.self  {
            return try NumberArrayBuilder<T>()
        } else if t == Double.self {
            return try NumberArrayBuilder<T>()
        } else {
            throw ValidationError.unknownType
        }
    }

    public static func loadStringArrayBuilder() throws -> StringArrayBuilder {
        return try StringArrayBuilder()
    }

    public static func loadBoolArrayBuilder() throws -> BoolArrayBuilder {
        return try BoolArrayBuilder()
    }

    public static func loadDate32ArrayBuilder() throws -> Date32ArrayBuilder {
        return try Date32ArrayBuilder()
    }

    public static func loadDate64ArrayBuilder() throws -> Date64ArrayBuilder {
        return try Date64ArrayBuilder()
    }
}
