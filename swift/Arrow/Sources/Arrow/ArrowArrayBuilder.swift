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
    let type: ArrowType
    let bufferBuilder: T
    public var length: UInt {return self.bufferBuilder.length}
    public var capacity: UInt {return self.bufferBuilder.capacity}
    public var nullCount: UInt {return self.bufferBuilder.nullCount}
    public var offset: UInt {return self.bufferBuilder.offset}

    fileprivate init(_ type: ArrowType) throws {
        self.type = type
        self.bufferBuilder = try T()
    }

    public func append(_ val: T.ItemType?) {
        self.bufferBuilder.append(val)
    }

    public func finish() throws -> ArrowArray<T.ItemType> {
        let buffers = self.bufferBuilder.finish()
        let arrowData = try ArrowData(self.type, buffers: buffers, nullCount: self.nullCount)
        return U(arrowData)
    }

    public func getStride() -> Int {
        return self.type.getStride()
    }
}

public class NumberArrayBuilder<T>: ArrowArrayBuilder<FixedBufferBuilder<T>, FixedArray<T>> {
    fileprivate convenience init() throws {
        try self.init(ArrowType(ArrowType.infoForNumericType(T.self)))
    }
}

public class StringArrayBuilder: ArrowArrayBuilder<VariableBufferBuilder<String>, StringArray> {
    fileprivate convenience init() throws {
        try self.init(ArrowType(ArrowType.ArrowString))
    }
}

public class BinaryArrayBuilder: ArrowArrayBuilder<VariableBufferBuilder<Data>, BinaryArray> {
    fileprivate convenience init() throws {
        try self.init(ArrowType(ArrowType.ArrowBinary))
    }
}

public class BoolArrayBuilder: ArrowArrayBuilder<BoolBufferBuilder, BoolArray> {
    fileprivate convenience init() throws {
        try self.init(ArrowType(ArrowType.ArrowBool))
    }
}

public class Date32ArrayBuilder: ArrowArrayBuilder<Date32BufferBuilder, Date32Array> {
    fileprivate convenience init() throws {
        try self.init(ArrowType(ArrowType.ArrowDate32))
    }
}

public class Date64ArrayBuilder: ArrowArrayBuilder<Date64BufferBuilder, Date64Array> {
    fileprivate convenience init() throws {
        try self.init(ArrowType(ArrowType.ArrowDate64))
    }
}

public class Time32ArrayBuilder: ArrowArrayBuilder<FixedBufferBuilder<Time32>, Time32Array> {
    fileprivate convenience init(_ unit: ArrowTime32Unit) throws {
        try self.init(ArrowTypeTime32(unit))
    }
}

public class Time64ArrayBuilder: ArrowArrayBuilder<FixedBufferBuilder<Time64>, Time64Array> {
    fileprivate convenience init(_ unit: ArrowTime64Unit) throws {
        try self.init(ArrowTypeTime64(unit))
    }
}

public class ArrowArrayBuilders {
    public static func loadNumberArrayBuilder<T>() throws -> NumberArrayBuilder<T> {
        let type = T.self
        if type == Int8.self {
            return try NumberArrayBuilder<T>()
        } else if type == Int16.self {
            return try NumberArrayBuilder<T>()
        } else if type == Int32.self {
            return try NumberArrayBuilder<T>()
        } else if type == Int64.self {
            return try NumberArrayBuilder<T>()
        } else if type == UInt8.self {
            return try NumberArrayBuilder<T>()
        } else if type == UInt16.self {
            return try NumberArrayBuilder<T>()
        } else if type == UInt32.self {
            return try NumberArrayBuilder<T>()
        } else if type == UInt64.self {
            return try NumberArrayBuilder<T>()
        } else if type == Float.self {
            return try NumberArrayBuilder<T>()
        } else if type == Double.self {
            return try NumberArrayBuilder<T>()
        } else {
            throw ArrowError.unknownType("Type is invalid for NumberArrayBuilder")
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

    public static func loadBinaryArrayBuilder() throws -> BinaryArrayBuilder {
        return try BinaryArrayBuilder()
    }

    public static func loadTime32ArrayBuilder(_ unit: ArrowTime32Unit) throws -> Time32ArrayBuilder {
        return try Time32ArrayBuilder(unit)
    }

    public static func loadTime64ArrayBuilder(_ unit: ArrowTime64Unit) throws -> Time64ArrayBuilder {
        return try Time64ArrayBuilder(unit)
    }
}
