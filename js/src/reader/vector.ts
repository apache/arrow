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

import { VectorReaderContext } from './arrow';
import * as Schema_ from '../format/Schema';
import * as Message_ from '../format/Message';
import { TypedArray, TypedArrayConstructor } from '../vector/types';
import {
    Vector, BoolVector, BinaryVector, DictionaryVector,
    Int8Vector, Int16Vector, Int32Vector, Int64Vector,
    Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector,
    Utf8Vector, ListVector, FixedSizeListVector, StructVector,
    Float16Vector, Float32Vector, Float64Vector, DecimalVector,
    Date32Vector, Date64Vector, Time32Vector, Time64Vector, TimestampVector,
} from '../vector/arrow';

import Int = Schema_.org.apache.arrow.flatbuf.Int;
import Date = Schema_.org.apache.arrow.flatbuf.Date;
import Time = Schema_.org.apache.arrow.flatbuf.Time;
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import Field = Schema_.org.apache.arrow.flatbuf.Field;
import Buffer = Schema_.org.apache.arrow.flatbuf.Buffer;
import Decimal = Schema_.org.apache.arrow.flatbuf.Decimal;
import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
import TimeUnit = Schema_.org.apache.arrow.flatbuf.TimeUnit;
// import Interval = Schema_.org.apache.arrow.flatbuf.Interval;
import Timestamp = Schema_.org.apache.arrow.flatbuf.Timestamp;
// import IntervalUnit = Schema_.org.apache.arrow.flatbuf.IntervalUnit;
import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
import FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;
import FixedSizeList = Schema_.org.apache.arrow.flatbuf.FixedSizeList;
import FloatingPoint = Schema_.org.apache.arrow.flatbuf.FloatingPoint;
import FixedSizeBinary = Schema_.org.apache.arrow.flatbuf.FixedSizeBinary;

export function readVector(field: Field, state: VectorReaderContext) {
    return readDictionaryVector(field, state) || readValueVector(field, state);
}

/* a dictionary index defaults to signed 32 bit int if unspecified */
const defaultDictionaryIndexType = { bitWidth: () => 32, isSigned: () => true } as Int;
const intVectors = [
    [/* unsigned */ [Uint8Vector,  Uint8Array ],  /* signed */ [Int8Vector , Int8Array ]],
    [/* unsigned */ [Uint16Vector, Uint16Array],  /* signed */ [Int16Vector, Int16Array]],
    [/* unsigned */ [Uint32Vector, Uint32Array],  /* signed */ [Int32Vector, Int32Array]],,
    [/* unsigned */ [Uint64Vector, Uint32Array],  /* signed */ [Int64Vector, Int32Array]]
] as [any, TypedArrayConstructor][][];

export function readDictionaryVector(field: Field, state: VectorReaderContext) {
    const encoding = field.dictionary()!;
    if (encoding) {
        const type = encoding.indexType() || defaultDictionaryIndexType;
        const data = state.dictionaries.get(encoding.id().toFloat64().toString())!;
        const [IntVector, IntArray] = intVectors[type.bitWidth() >>> 4]![+type.isSigned()];
        const { fieldNode, validity, data: keys } = readNumericBuffers(field, state, IntArray);
        return new DictionaryVector({
            validity, data, field, fieldNode,
            keys: new IntVector({ field, fieldNode, data: keys })
        });
    }
    return null;
}

export function readValueVector(field: Field, state: VectorReaderContext): Vector {
    switch (field.typeType()) {
        case Type.NONE: return readNullVector();
        case Type.Null: return readNullVector();
        // case Type.Map: return readMapVector(field, state);
        case Type.Int: return readIntVector(field, state);
        case Type.Bool: return readBoolVector(field, state);
        case Type.Date: return readDateVector(field, state);
        case Type.List: return readListVector(field, state);
        case Type.Utf8: return readUtf8Vector(field, state);
        case Type.Time: return readTimeVector(field, state);
        // case Type.Union: return readUnionVector(field, state);
        case Type.Binary: return readBinaryVector(field, state);
        case Type.Decimal: return readDecimalVector(field, state);
        case Type.Struct_: return readStructVector(field, state);
        case Type.FloatingPoint: return readFloatVector(field, state);
        case Type.Timestamp: return readTimestampVector(field, state);
        case Type.FixedSizeList: return readFixedSizeListVector(field, state);
        case Type.FixedSizeBinary: return readFixedSizeBinaryVector(field, state);
    }
    throw new Error(`Unrecognized Vector { name: ${Type[field.typeType()]}, type: ${field.typeType()} }`);
}

export function readNullVector() {
    return new Vector();
}

export function readBoolVector(field: Field, state: VectorReaderContext) {
    return new BoolVector(readNumericBuffers(field, state, Uint8Array));
}

export function readDateVector(field: Field, state: VectorReaderContext) {
    const type = field.type(new Date())!;
    switch (type.unit()) {
        case DateUnit.DAY: return new Date32Vector({ ...readNumericBuffers(field, state, Int32Array), unit: DateUnit[type.unit()] });
        case DateUnit.MILLISECOND: return new Date64Vector({ ...readNumericBuffers(field, state, Int32Array), unit: DateUnit[type.unit()] });
    }
    throw new Error(`Unrecognized Date { unit: ${type.unit()} }`);
}

export function readTimeVector(field: Field, state: VectorReaderContext) {
    const type = field.type(new Time())!;
    switch (type.bitWidth()) {
        case 32: return new Time32Vector({ ...readNumericBuffers(field, state, Int32Array), unit: TimeUnit[type.unit()] });
        case 64: return new Time64Vector({ ...readNumericBuffers(field, state, Uint32Array), unit: TimeUnit[type.unit()] });
    }
    throw new Error(`Unrecognized Time { unit: ${type.unit()}, bitWidth: ${type.bitWidth()} }`);
}

export function readTimestampVector(field: Field, state: VectorReaderContext) {
    const type = field.type(new Timestamp())!;
    const { fieldNode, validity, data } = readNumericBuffers(field, state, Uint32Array);
    return new TimestampVector({
        field, fieldNode, validity, data,
        timezone: type.timezone()!,
        unit: TimeUnit[type.unit()],
    });
}

export function readListVector(field: Field, state: VectorReaderContext) {
    const { fieldNode, validity, offsets } = readListBuffers(field, state);
    return new ListVector({
        field, fieldNode, validity, offsets,
        values: readVector(field.children(0)!, state)
    });
}

export function readStructVector(field: Field, state: VectorReaderContext) {
    const n = field.childrenLength();
    const columns = new Array<Vector>(n);
    const fieldNode = state.readNextNode();
    const validity = readValidityBuffer(field, fieldNode, state);
    for (let i = -1, child: Field; ++i < n;) {
        if (child = field.children(i)!) {
            columns[i] = readVector(child, state);
        }
    }
    return new StructVector({ field, fieldNode, validity, columns });
}

export function readBinaryVector(field: Field, state: VectorReaderContext) {
    return new BinaryVector(readBinaryBuffers(field, state));
}

export function readDecimalVector(field: Field, state: VectorReaderContext) {
    const type = field.type(new Decimal())!;
    const { fieldNode, validity, data } = readNumericBuffers(field, state, Uint32Array);
    return new DecimalVector({
        scale: type.scale(),
        precision: type.precision(),
        field, fieldNode, validity, data
    });
}

export function readUtf8Vector(field: Field, state: VectorReaderContext) {
    const { fieldNode, validity, offsets, data } = readBinaryBuffers(field, state);
    return new Utf8Vector({
        field, fieldNode,
        values: new BinaryVector({
            validity, offsets, data
        })
    });
}

export function readFixedSizeListVector(field: Field, state: VectorReaderContext) {
    const type = field.type(new FixedSizeList())!;
    const fieldNode = state.readNextNode();
    const validity = readValidityBuffer(field, fieldNode, state);
    return new FixedSizeListVector({
        field, fieldNode, validity,
        size: type.listSize(),
        values: readVector(field.children(0)!, state)
    });
}

export function readFixedSizeBinaryVector(field: Field, state: VectorReaderContext) {
    const type = field.type(new FixedSizeBinary())!;
    const { fieldNode, validity, data } = readNumericBuffers(field, state, Uint8Array);
    return new FixedSizeListVector({
        size: type.byteWidth(),
        field, fieldNode, validity,
        values: new Uint8Vector({ data })
    });
}

export function readFloatVector(field: Field, state: VectorReaderContext) {
    const type = field.type(new FloatingPoint())!;
    switch (type.precision()) {
        case Precision.HALF:   return new Float16Vector(readNumericBuffers(field, state, Uint16Array));
        case Precision.SINGLE: return new Float32Vector(readNumericBuffers(field, state, Float32Array));
        case Precision.DOUBLE: return new Float64Vector(readNumericBuffers(field, state, Float64Array));
    }
    throw new Error(`Unrecognized FloatingPoint { precision: ${type.precision()} }`);
}

export function readIntVector(field: Field, state: VectorReaderContext) {
    const type = field.type(new Int())!;
    if (type.isSigned()) {
        switch (type.bitWidth()) {
            case  8: return new  Int8Vector(readNumericBuffers(field, state, Int8Array));
            case 16: return new Int16Vector(readNumericBuffers(field, state, Int16Array));
            case 32: return new Int32Vector(readNumericBuffers(field, state, Int32Array));
            case 64: return new Int64Vector(readNumericBuffers(field, state, Int32Array));
        }
    }
    switch (type.bitWidth()) {
        case  8: return new  Uint8Vector(readNumericBuffers(field, state, Uint8Array));
        case 16: return new Uint16Vector(readNumericBuffers(field, state, Uint16Array));
        case 32: return new Uint32Vector(readNumericBuffers(field, state, Uint32Array));
        case 64: return new Uint64Vector(readNumericBuffers(field, state, Uint32Array));
    }
    throw new Error(`Unrecognized Int { isSigned: ${type.isSigned()}, bitWidth: ${type.bitWidth()} }`);
}

function readListBuffers(field: Field, state: VectorReaderContext) {
    const fieldNode = state.readNextNode();
    const validity = readValidityBuffer(field, fieldNode, state);
    const offsets = readDataBuffer(Int32Array, state);
    return { field, fieldNode, validity, offsets };
}

function readBinaryBuffers(field: Field, state: VectorReaderContext) {
    const fieldNode = state.readNextNode();
    const validity = readValidityBuffer(field, fieldNode, state);
    const offsets = readDataBuffer(Int32Array, state);
    const data = readDataBuffer(Uint8Array, state);
    return { field, fieldNode, validity, offsets, data };
}

function readNumericBuffers<T extends TypedArray>(field: Field, state: VectorReaderContext, ArrayConstructor: TypedArrayConstructor<T>) {
    const fieldNode = state.readNextNode();
    const validity = readValidityBuffer(field, fieldNode, state);
    const data = readDataBuffer(ArrayConstructor, state);
    return { field, fieldNode, validity, data };
}

function readDataBuffer<T extends TypedArray>(ArrayConstructor: TypedArrayConstructor<T>, state: VectorReaderContext) {
    return createTypedArray(ArrayConstructor, state.bytes, state.offset, state.readNextBuffer());
}

function readValidityBuffer(field: Field, fieldNode: FieldNode, state: VectorReaderContext) {
    return createValidityArray(field, fieldNode, state.bytes, state.offset, state.readNextBuffer());
}

function createValidityArray(field: Field, fieldNode: FieldNode, bytes: Uint8Array, offset: number, buffer: Buffer) {
    return field.nullable() && fieldNode.nullCount().low > 0 && createTypedArray(Uint8Array, bytes, offset, buffer) || null;
}

function createTypedArray<T extends TypedArray>(ArrayConstructor: TypedArrayConstructor<T>, bytes: Uint8Array, offset: number, buffer: Buffer) {
    return new ArrayConstructor(
        bytes.buffer,
        bytes.byteOffset + offset + buffer.offset().low,
        buffer.length().low / ArrayConstructor.BYTES_PER_ELEMENT
    );
}
