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

public class ArrowEncoder: Encoder {
    public private(set) var builders = [String: ArrowArrayHolderBuilder]()
    private var byIndex = [String]()
    public var codingPath: [CodingKey] = []
    public var userInfo: [CodingUserInfoKey: Any] = [:]
    var errorMsg: String?
    // this is used for Dictionary types.  A dictionary type
    // will give each key and value there own index so instead
    // of having a 2 column RecordBatch you would have
    // 2 * length(dictionary) column RecordBatch. Which would not
    // be the expected output.
    var modForIndex: Int?

    public init() {}

    public init(_ builders: [String: ArrowArrayHolderBuilder], byIndex: [String]) {
        self.builders = builders
        self.byIndex = byIndex
    }

    public static func encode<T: Encodable>(_ data: T) throws -> RecordBatch {
        let encoder = try loadEncoder(data)
        try data.encode(to: encoder)
        return try encoder.finish()
    }

    public static func encode<T: Encodable>(_ rows: [T]) throws -> RecordBatch? {
        if rows.isEmpty {
            return nil
        }

        let encoder = try loadEncoder(rows[0])
        for row in rows {
            try row.encode(to: encoder)
        }

        return try encoder.finish()
    }

    static func loadEncoder<T>(_ data: T) throws -> ArrowEncoder {
        // this will check if T is a simple built in type
        // (UInt, Int, Int8, String, Date, etc...).
        if ArrowArrayBuilders.isValidBuilderType(T.self) {
            let builders = ["col0": try ArrowArrayBuilders.loadBuilder(T.self)]
            return ArrowEncoder(builders, byIndex: ["col0"])
        } else {
            let encoder = ArrowEncoder()
            if data is [AnyHashable: Any] {
                encoder.modForIndex = 2
            }

            return encoder
        }
    }

    public func finish() throws -> RecordBatch {
        try throwIfInvalid()
        let batchBuilder = RecordBatch.Builder()
        for key in byIndex {
            batchBuilder.addColumn(key, arrowArray: try builders[key]!.toHolder())
        }

        switch batchBuilder.finish() {
        case .success(let rb):
            return rb
        case .failure(let error):
            throw error
        }
    }

    public func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key: CodingKey {
        var container = ArrowKeyedEncoding<Key>(self)
        container.codingPath = codingPath
        return KeyedEncodingContainer(container)
    }

    public func unkeyedContainer() -> UnkeyedEncodingContainer {
        return ArrowUnkeyedEncoding(self, codingPath: self.codingPath)
    }

    public func singleValueContainer() -> SingleValueEncodingContainer {
        return ArrowSingleValueEncoding(self, codingPath: codingPath)
    }

    func doEncodeNil(key: CodingKey) throws {
        try throwIfInvalid()
        guard let builder = builders[key.stringValue] else {
            throw ArrowError.invalid("Column not found for key: \(key)")
        }

        builder.appendAny(nil)
    }

    // This is required by the keyed and unkeyed encoders as columns are
    // added when the first row of the data is encoded.  This is done due
    // to limitations in the Swifts Mirror API (ex: it is unable to correctly
    // find the type for String? in [Int: String?])
    @discardableResult
    func ensureColumnExists<T>(_ value: T, key: String) throws -> ArrowArrayHolderBuilder {
        try throwIfInvalid()
        var builder = builders[key]
        if builder == nil {
            builder = try ArrowArrayBuilders.loadBuilder(T.self)
            builders[key] = builder
            byIndex.append(key)
        }

        return builder!
    }

    func getIndex(_ index: Int) -> Int {
        return self.modForIndex == nil ? index : index % self.modForIndex!
    }

    func doEncodeNil(_ keyIndex: Int) throws {
        try throwIfInvalid()
        let index = self.getIndex(keyIndex)
        if index >= builders.count {
            throw ArrowError.outOfBounds(index: Int64(index))
        }

        builders[byIndex[index]]!.appendAny(nil)
    }

    func doEncode<T>(_ value: T, key: CodingKey) throws {
        try throwIfInvalid()
        let builder = try ensureColumnExists(value, key: key.stringValue)
        builder.appendAny(value)
    }

    func doEncode<T>(_ value: T, keyIndex: Int) throws {
        try throwIfInvalid()
        let index = self.getIndex(keyIndex)
        if index >= builders.count {
            if index == builders.count {
                try ensureColumnExists(value, key: "col\(index)")
            } else {
                throw ArrowError.outOfBounds(index: Int64(index))
            }
        }

        builders[byIndex[index]]!.appendAny(value)
    }

    func throwIfInvalid() throws {
        if let errorMsg = self.errorMsg {
            throw ArrowError.invalid(errorMsg)
        }
    }
}

private struct ArrowKeyedEncoding<Key: CodingKey>: KeyedEncodingContainerProtocol {
    var codingPath: [CodingKey] = []
    let encoder: ArrowEncoder
    init(_ encoder: ArrowEncoder) {
        self.encoder = encoder
    }

    // If this method is called on row 0 and the encoder is
    // lazily bulding holders then this will produce an error
    // as this method does not know what the underlying type
    // is for the column.  This method is not called for
    // nullable types (String?, Int32?, Date?) and the workaround
    // for this issue would be to predefine the builders for the
    // encoder. (I have only encoutered this issue when allowing
    // nullable types at the encode func level which is currently
    // not allowed)
    mutating func encodeNil(forKey key: Key) throws {
        try encoder.doEncodeNil(key: key)
    }

    mutating func doEncodeIf<T>(_ value: T?, forKey key: Key) throws {
        if value == nil {
            try encoder.ensureColumnExists(value, key: key.stringValue)
            try encoder.doEncodeNil(key: key)
        } else {
            try encoder.doEncode(value, key: key)
        }
    }

    mutating func encode(_ value: Bool, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: Bool?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: String, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: String?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: Double, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: Double?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: Float, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: Float?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: Int, forKey key: Key) throws {
        throw ArrowError.invalid(
            "Int type is not supported (please use Int8, Int16, Int32 or Int64)")
    }

    mutating func encodeIfPresent(_ value: Int?, forKey key: Key) throws {
        throw ArrowError.invalid(
            "Int type is not supported (please use Int8, Int16, Int32 or Int64)")
    }

    mutating func encode(_ value: Int8, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: Int8?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: Int16, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: Int16?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: Int32, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: Int32?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: Int64, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: Int64?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: UInt, forKey key: Key) throws {
        throw ArrowError.invalid(
            "UInt type is not supported (please use UInt8, UInt16, UInt32 or UInt64)")
    }

    mutating func encodeIfPresent(_ value: UInt?, forKey key: Key) throws {
        throw ArrowError.invalid(
            "UInt type is not supported (please use UInt8, UInt16, UInt32 or UInt64)")
    }

    mutating func encode(_ value: UInt8, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: UInt8?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: UInt16, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: UInt16?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: UInt32, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: UInt32?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode(_ value: UInt64, forKey key: Key) throws {
        try encoder.doEncode(value, key: key)
    }

    mutating func encodeIfPresent(_ value: UInt64?, forKey key: Key) throws {
        try doEncodeIf(value, forKey: key)
    }

    mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
        if ArrowArrayBuilders.isValidBuilderType(T.self) {
            try encoder.doEncode(value, key: key)
        } else {
            throw ArrowError.invalid("Type \(T.self) is currently not supported")
        }
    }

    mutating func encodeIfPresent<T>(_ value: T?, forKey key: Self.Key) throws where T: Encodable {
        if ArrowArrayBuilders.isValidBuilderType(T?.self) {
            try doEncodeIf(value, forKey: key)
        } else {
            throw ArrowError.invalid("Type \(T.self) is currently not supported")
        }
    }

    // nested container is currently not allowed.  This method doesn't throw
    // so setting an error mesg that will be throw by the encoder at the next
    // method call that throws
    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) -> KeyedEncodingContainer<NestedKey> {
        self.encoder.errorMsg = "Nested decoding is currently not supported."
        var container = ArrowKeyedEncoding<NestedKey>(self.encoder)
        container.codingPath = codingPath
        return KeyedEncodingContainer(container)
    }

    // nested container is currently not allowed.  This method doesn't throw
    // so setting an error mesg that will be throw by the encoder at the next
    // method call that throws
    mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        self.encoder.errorMsg = "Nested decoding is currently not supported."
        return ArrowUnkeyedEncoding(self.encoder, codingPath: self.codingPath)
    }

    // super encoding is currently not allowed.  This method doesn't throw
    // so setting an error mesg that will be throw by the encoder at the next
    // method call that throws
    mutating func superEncoder() -> Encoder {
        self.encoder.errorMsg = "super encoding is currently not supported."
        return self.encoder
    }

    // super encoding is currently not allowed.  This method doesn't throw
    // so setting an error mesg that will be throw by the encoder at the next
    // method call that throws
    mutating func superEncoder(forKey key: Key) -> Encoder {
        self.encoder.errorMsg = "super encoding is currently not supported."
        return self.encoder
    }
}

private struct ArrowUnkeyedEncoding: UnkeyedEncodingContainer {
    public private(set) var encoder: ArrowEncoder
    var codingPath: [CodingKey] = []
    var currentIndex: Int
    var count: Int = 0

    init(_ encoder: ArrowEncoder, codingPath: [CodingKey], currentIndex: Int = 0) {
        self.encoder = encoder
        self.currentIndex = currentIndex
    }

    mutating func increment() {
        self.currentIndex += 1
    }

    // If this method is called on row 0 and the encoder is
    // lazily bulding holders then this will produce an error
    // as this method does not know what the underlying type
    // is for the column.  This method is not called for
    // nullable types (String?, Int32?, Date?) and the workaround
    // for this issue would be to predefine the builders for the
    // encoder. (I have only encoutered this issue when allowing
    // nullable types at the encode func level which is currently
    // not allowed)
    mutating func encodeNil() throws {
        try encoder.doEncodeNil(self.currentIndex)
    }

    mutating func encode<T>(_ value: T) throws where T: Encodable {
        let type = T.self
        if ArrowArrayBuilders.isValidBuilderType(type) {
            defer {increment()}
            return try self.encoder.doEncode(value, keyIndex: self.currentIndex)
        } else {
            throw ArrowError.invalid("Type \(type) is currently not supported")
        }
    }

    // nested container is currently not allowed.  This method doesn't throw
    // so setting an error mesg that will be throw by the encoder at the next
    // method call that throws
    mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type
    ) -> KeyedEncodingContainer<NestedKey> where NestedKey: CodingKey {
        self.encoder.errorMsg = "Nested decoding is currently not supported."
        var container = ArrowKeyedEncoding<NestedKey>(self.encoder)
        container.codingPath = codingPath
        return KeyedEncodingContainer(container)
    }

    // nested container is currently not allowed.  This method doesn't throw
    // so setting an error mesg that will be throw by the encoder at the next
    // method call that throws
    mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        self.encoder.errorMsg = "Nested decoding is currently not supported."
        return ArrowUnkeyedEncoding(self.encoder, codingPath: self.codingPath)
    }

    // super encoding is currently not allowed.  This method doesn't throw
    // so setting an error mesg that will be throw by the encoder at the next
    // method call that throws
    mutating func superEncoder() -> Encoder {
        self.encoder.errorMsg = "super encoding is currently not supported."
        return self.encoder
    }
}

private struct ArrowSingleValueEncoding: SingleValueEncodingContainer {
    public private(set) var encoder: ArrowEncoder
    var codingPath: [CodingKey] = []

    public init(_ encoder: ArrowEncoder, codingPath: [CodingKey]) {
        self.encoder = encoder
        self.codingPath = codingPath
    }

    mutating func encodeNil() throws {
        return try self.encoder.doEncodeNil(0)
    }

    mutating func encode<T: Encodable>(_ value: T) throws {
        if ArrowArrayBuilders.isValidBuilderType(T.self) {
            return try self.encoder.doEncode(value, keyIndex: 0)
        } else {
            throw ArrowError.invalid("Type \(T.self) is currently not supported")
        }
    }
}
// swiftlint:disable:this file_length
