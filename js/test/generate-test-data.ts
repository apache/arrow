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

/* tslint:disable */
const randomatic = require('randomatic');
import { TextEncoder } from 'text-encoding-utf-8';
import { VectorType as V } from '../src/interfaces';

import {
    Data, Vector, Visitor, DataType,
    Table, Schema, Field, RecordBatch,
    Null,
    Bool,
    Int, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64,
    Float, Float16, Float32, Float64,
    Utf8,
    Binary,
    FixedSizeBinary,
    Date_, DateDay, DateMillisecond,
    Timestamp, TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Time, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    Decimal,
    List,
    Struct,
    Union, DenseUnion, SparseUnion,
    Dictionary,
    Interval, IntervalDayTime, IntervalYearMonth,
    FixedSizeList,
    Map_,
    DateUnit, TimeUnit, UnionMode
} from './Arrow';

type TKeys = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32;

interface TestDataVectorGenerator extends Visitor {

    visit<T extends Null>            (type: T, length?: number): GeneratedVector<V<T>>;
    visit<T extends Bool>            (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends Int>             (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends Float>           (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends Utf8>            (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends Binary>          (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends FixedSizeBinary> (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends Date_>           (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends Timestamp>       (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends Time>            (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends Decimal>         (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends Interval>        (type: T, length?: number, nullCount?: number): GeneratedVector<V<T>>;
    visit<T extends List>            (type: T, length?: number, nullCount?: number, child?: Vector): GeneratedVector<V<T>>;
    visit<T extends FixedSizeList>   (type: T, length?: number, nullCount?: number, child?: Vector): GeneratedVector<V<T>>;
    visit<T extends Dictionary>      (type: T, length?: number, nullCount?: number, dictionary?: Vector): GeneratedVector<V<T>>;
    visit<T extends Union>           (type: T, length?: number, nullCount?: number, children?: Vector[]): GeneratedVector<V<T>>;
    visit<T extends Struct>          (type: T, length?: number, nullCount?: number, children?: Vector[]): GeneratedVector<V<T>>;
    visit<T extends Map_>            (type: T, length?: number, nullCount?: number, children?: Vector[]): GeneratedVector<V<T>>;
    visit<T extends DataType>        (type: T, length?: number, ...args: any[]): GeneratedVector<V<T>>;

    visitNull:            typeof generateNull;
    visitBool:            typeof generateBool;
    visitInt:             typeof generateInt;
    visitFloat:           typeof generateFloat;
    visitUtf8:            typeof generateUtf8;
    visitBinary:          typeof generateBinary;
    visitFixedSizeBinary: typeof generateFixedSizeBinary;
    visitDate:            typeof generateDate;
    visitTimestamp:       typeof generateTimestamp;
    visitTime:            typeof generateTime;
    visitDecimal:         typeof generateDecimal;
    visitList:            typeof generateList;
    visitStruct:          typeof generateStruct;
    visitUnion:           typeof generateUnion;
    visitDictionary:      typeof generateDictionary;
    visitInterval:        typeof generateInterval;
    visitFixedSizeList:   typeof generateFixedSizeList;
    visitMap:             typeof generateMap;
}

class TestDataVectorGenerator extends Visitor {}

TestDataVectorGenerator.prototype.visitNull            = generateNull;
TestDataVectorGenerator.prototype.visitBool            = generateBool;
TestDataVectorGenerator.prototype.visitInt             = generateInt;
TestDataVectorGenerator.prototype.visitFloat           = generateFloat;
TestDataVectorGenerator.prototype.visitUtf8            = generateUtf8;
TestDataVectorGenerator.prototype.visitBinary          = generateBinary;
TestDataVectorGenerator.prototype.visitFixedSizeBinary = generateFixedSizeBinary;
TestDataVectorGenerator.prototype.visitDate            = generateDate;
TestDataVectorGenerator.prototype.visitTimestamp       = generateTimestamp;
TestDataVectorGenerator.prototype.visitTime            = generateTime;
TestDataVectorGenerator.prototype.visitDecimal         = generateDecimal;
TestDataVectorGenerator.prototype.visitList            = generateList;
TestDataVectorGenerator.prototype.visitStruct          = generateStruct;
TestDataVectorGenerator.prototype.visitUnion           = generateUnion;
TestDataVectorGenerator.prototype.visitDictionary      = generateDictionary;
TestDataVectorGenerator.prototype.visitInterval        = generateInterval;
TestDataVectorGenerator.prototype.visitFixedSizeList   = generateFixedSizeList;
TestDataVectorGenerator.prototype.visitMap             = generateMap;

const vectorGenerator = new TestDataVectorGenerator();

const defaultListChild = new Field('list[Int32]', new Int32());

const defaultRecordBatchChildren = () => [
    new Field('i32', new Int32()),
    new Field('f32', new Float32()),
    new Field('dict', new Dictionary(new Utf8(), new Int32()))
];

const defaultStructChildren = () => [
    new Field('struct[0]', new Int32()),
    new Field('struct[1]', new Utf8()),
    new Field('struct[2]', new List(new Field('list[DateDay]', new DateDay())))
];

const defaultUnionChildren = () => [
    new Field('union[0]', new Float64()),
    new Field('union[1]', new Dictionary(new Uint32(), new Int32())),
    new Field('union[2]', new Map_(defaultStructChildren()))
];

export interface GeneratedTable {
    table: Table;
    rows: () => any[][];
    cols: () => any[][];
    keys: () => number[][];
    rowBatches: (() => any[][])[];
    colBatches: (() => any[][])[];
    keyBatches: (() => number[][])[];
}

export interface GeneratedRecordBatch {
    recordBatch: RecordBatch;
    rows: () => any[][];
    cols: () => any[][];
    keys: () => number[][];
}

export type GeneratedVector<TVec extends Vector = Vector> = {
    vector: TVec;
    keys?: number[];
    values: () => (TVec['TValue'] | null)[];
};

export const table = (lengths = [100], schema: Schema = new Schema(defaultRecordBatchChildren(), new Map([['foo', 'bar']]))): GeneratedTable => {
    const generated = lengths.map((length) => recordBatch(length, schema));
    const rowBatches = generated.map(({ rows }) => rows);
    const colBatches = generated.map(({ cols }) => cols);
    const keyBatches = generated.map(({ keys }) => keys);
    const rows = memoize(() => rowBatches.reduce((rows: any[][], batch) => [...rows, ...batch()], []));
    const keys = memoize(() => keyBatches.reduce((keys: any[][], batch) => (
        !keys.length ? batch() : keys.map((idxs, i) => [...(idxs || []), ...(batch()[i] || [])])
    ), []));
    const cols = memoize(() => colBatches.reduce((cols: any[][], batch) => (
        !cols.length ? batch() : cols.map((vals, i) => [...vals, ...batch()[i]])
    ), []));

    return { rows, cols, keys, rowBatches, colBatches, keyBatches, table: new Table(schema, generated.map(({ recordBatch }) => recordBatch)) };
};

export const recordBatch = (length = 100, schema: Schema = new Schema(defaultRecordBatchChildren())): GeneratedRecordBatch => {

    const generated = schema.fields.map((f) => vectorGenerator.visit(f.type, length));
    const vecs = generated.map(({ vector }) => vector);

    const keys = memoize(() => generated.map(({ keys }) => keys));
    const cols = memoize(() => generated.map(({ values }) => values()));
    const rows = ((_cols: () => any[][]) => memoize((rows: any[][] = [], cols: any[][] = _cols()) => {
        for (let i = -1; ++i < length; rows[i] = cols.map((vals) => vals[i]));
        return rows;
    }))(cols);

    return { rows, cols, keys, recordBatch: new RecordBatch(schema, length, vecs) };
};

export const null_ = (length = 100) => vectorGenerator.visit(new Null(), length);
export const bool = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Bool(), length, nullCount);
export const int8 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Int8(), length, nullCount);
export const int16 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Int16(), length, nullCount);
export const int32 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Int32(), length, nullCount);
export const int64 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Int64(), length, nullCount);
export const uint8 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Uint8(), length, nullCount);
export const uint16 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Uint16(), length, nullCount);
export const uint32 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Uint32(), length, nullCount);
export const uint64 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Uint64(), length, nullCount);
export const float16 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Float16(), length, nullCount);
export const float32 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Float32(), length, nullCount);
export const float64 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Float64(), length, nullCount);
export const utf8 = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Utf8(), length, nullCount);
export const binary = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new Binary(), length, nullCount);
export const fixedSizeBinary = (length = 100, nullCount = length * 0.2 | 0, byteWidth = 8) => vectorGenerator.visit(new FixedSizeBinary(byteWidth), length, nullCount);
export const dateDay = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new DateDay(), length, nullCount);
export const dateMillisecond = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new DateMillisecond(), length, nullCount);
export const timestampSecond = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new TimestampSecond(), length, nullCount);
export const timestampMillisecond = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new TimestampMillisecond(), length, nullCount);
export const timestampMicrosecond = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new TimestampMicrosecond(), length, nullCount);
export const timestampNanosecond = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new TimestampNanosecond(), length, nullCount);
export const timeSecond = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new TimeSecond(), length, nullCount);
export const timeMillisecond = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new TimeMillisecond(), length, nullCount);
export const timeMicrosecond = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new TimeMicrosecond(), length, nullCount);
export const timeNanosecond = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new TimeNanosecond(), length, nullCount);
export const decimal = (length = 100, nullCount = length * 0.2 | 0, scale = 2, precision = 9) => vectorGenerator.visit(new Decimal(scale, precision), length, nullCount);
export const list = (length = 100, nullCount = length * 0.2 | 0, child = defaultListChild) => vectorGenerator.visit(new List(child), length, nullCount);
export const struct = <T extends { [key: string]: DataType } = any>(length = 100, nullCount = length * 0.2 | 0, children: Field<T[keyof T]>[] = <any> defaultStructChildren()) => vectorGenerator.visit(new Struct<T>(children), length, nullCount);
export const denseUnion = (length = 100, nullCount = length * 0.2 | 0, children: Field[] = defaultUnionChildren()) => vectorGenerator.visit(new DenseUnion(children.map((f) => f.typeId), children), length, nullCount);
export const sparseUnion = (length = 100, nullCount = length * 0.2 | 0, children: Field[] = defaultUnionChildren()) => vectorGenerator.visit(new SparseUnion(children.map((f) => f.typeId), children), length, nullCount);
export const dictionary = <T extends DataType = Utf8, TKey extends TKeys = Int32> (length = 100, nullCount = length * 0.2 | 0, dict: T = <any> new Utf8(), keys: TKey = <any> new Int32()) => vectorGenerator.visit(new Dictionary(dict, keys), length, nullCount);
export const intervalDayTime = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new IntervalDayTime(), length, nullCount);
export const intervalYearMonth = (length = 100, nullCount = length * 0.2 | 0) => vectorGenerator.visit(new IntervalYearMonth(), length, nullCount);
export const fixedSizeList = (length = 100, nullCount = length * 0.2 | 0, listSize = 2, child = defaultListChild) => vectorGenerator.visit(new FixedSizeList(listSize, child), length, nullCount);
export const map = <T extends { [key: string]: DataType } = any>(length = 100, nullCount = length * 0.2 | 0, children: Field<T[keyof T]>[] = <any> defaultStructChildren()) => vectorGenerator.visit(new Map_<T>(children), length, nullCount);

export const vecs = {
    null_, bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float16, float32, float64, utf8, binary, fixedSizeBinary, dateDay, dateMillisecond, timestampSecond, timestampMillisecond, timestampMicrosecond, timestampNanosecond, timeSecond, timeMillisecond, timeMicrosecond, timeNanosecond, decimal, list, struct, denseUnion, sparseUnion, dictionary, intervalDayTime, intervalYearMonth, fixedSizeList, map
} as { [k: string]: (...args: any[]) => any };

function generateNull<T extends Null>(this: TestDataVectorGenerator, type: T, length = 100): GeneratedVector<V<T>> {
    return { values: () => Array.from({ length }, () => null), vector: Vector.new(Data.Null(type, 0, length, 0, null)) };
}

function generateBool<T extends Bool>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const data = createBitmap(length, length / 2 | 0);
    const nullBitmap = createBitmap(length, nullCount);
    const values = memoize(() => {
        const values = [] as (boolean | null)[];
        iterateBitmap(length, nullBitmap, (i, valid) => values[i] = !valid ? null : isValid(data, i));
        return values;
    });
    iterateBitmap(length, nullBitmap, (i, valid) => !valid && (data[i >> 3] &= ~(1 << (i % 8))));

    return { values, vector: Vector.new(Data.Bool(type, 0, length, nullCount, nullBitmap, data)) };
}

function generateInt<T extends Int>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const ArrayType = type.ArrayType;
    const stride = 1 + Number(type.bitWidth > 32);
    const nullBitmap = createBitmap(length, nullCount);
    const data = fillRandom(ArrayType as any, length * stride);
    const values = memoize(() => {
        const values = [] as (number | null)[];
        iterateBitmap(length, nullBitmap, (i, valid) => {
            values[i] = !valid ? null
                : stride === 1 ? data[i]
                : data.subarray(i * stride, (i + 1) * stride);
        });
        return values;
    });
    iterateBitmap(length, nullBitmap, (i, valid) => !valid && (data.set(new Uint8Array(stride), i * stride)));
    return { values, vector: Vector.new(Data.Int(type, 0, length, nullCount, nullBitmap, data)) };
}

function generateFloat<T extends Float>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const ArrayType = type.ArrayType;
    const precision = type.precision;
    const data = fillRandom(ArrayType as any, length);
    const nullBitmap = createBitmap(length, nullCount);
    const values = memoize(() => {
        const values = [] as (number | null)[];
        iterateBitmap(length, nullBitmap, (i, valid) => {
            values[i] = !valid ? null : precision > 0 ? data[i] : (data[i] - 32767) / 32767;
        });
        return values;
    });
    iterateBitmap(length, nullBitmap, (i, valid) => data[i] = !valid ? 0 : data[i] * Math.random());
    return { values, vector: Vector.new(Data.Float(type, 0, length, nullCount, nullBitmap, data)) };
}

function generateUtf8<T extends Utf8>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const nullBitmap = createBitmap(length, nullCount);
    const offsets = createVariableWidthOffsets(length, nullBitmap);
    const values = [...offsets.slice(1)]
        .map((o, i) => isValid(nullBitmap, i) ? o - offsets[i] : null)
        .map((length) => length == null ? null : randomString(length));
    const data = createVariableWidthBytes(length, nullBitmap, offsets, (i) => encodeUtf8(values[i]));
    return { values: () => values, vector: Vector.new(Data.Utf8(type, 0, length, nullCount, nullBitmap, offsets, data)) };
}

function generateBinary<T extends Binary>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const nullBitmap = createBitmap(length, nullCount);
    const offsets = createVariableWidthOffsets(length, nullBitmap);
    const values = [...offsets.slice(1)]
        .map((o, i) => isValid(nullBitmap, i) ? o - offsets[i] : null)
        .map((length) => length == null ? null : randomBytes(length));
    const data = createVariableWidthBytes(length, nullBitmap, offsets, (i) => values[i]!);
    return { values: () => values, vector: Vector.new(Data.Binary(type, 0, length, nullCount, nullBitmap, offsets, data)) };
}

function generateFixedSizeBinary<T extends FixedSizeBinary>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const nullBitmap = createBitmap(length, nullCount);
    const data = fillRandom(Uint8Array, length * type.byteWidth);
    const values = memoize(() => {
        const values = [] as (Uint8Array | null)[];
        iterateBitmap(length, nullBitmap, (i, valid) => {
            values[i] = !valid ? null : data.subarray(i * type.byteWidth, (i + 1) * type.byteWidth);
        });
        return values;
    });
    iterateBitmap(length, nullBitmap, (i, valid) => !valid && data.set(new Uint8Array(type.byteWidth), i * type.byteWidth));
    return { values, vector: Vector.new(Data.FixedSizeBinary(type, 0, length, nullCount, nullBitmap, data)) };
}

function generateDate<T extends Date_>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const values = [] as (number | null)[];
    const nullBitmap = createBitmap(length, nullCount);
    const data = type.unit === DateUnit.DAY
        ? createDate32(length, nullBitmap, values)
        : createDate64(length, nullBitmap, values);
    return {
        values: () => values.map((x) => x == null ? null : new Date(x)),
        vector: Vector.new(Data.Date(type, 0, length, nullCount, nullBitmap, data))
    };
}

function generateTimestamp<T extends Timestamp>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const values = [] as (number | null)[];
    const nullBitmap = createBitmap(length, nullCount);
    const multiple = type.unit === TimeUnit.NANOSECOND ? 1000000000 :
                     type.unit === TimeUnit.MICROSECOND ? 1000000 :
                     type.unit === TimeUnit.MILLISECOND ? 1000 : 1;
    const data = createTimestamp(length, nullBitmap, multiple, values);
    return { values: () => values, vector: Vector.new(Data.Timestamp(type, 0, length, nullCount, nullBitmap, data)) };
}

function generateTime<T extends Time>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const values = [] as (Int32Array | number | null)[];
    const nullBitmap = createBitmap(length, nullCount);
    const multiple = type.unit === TimeUnit.NANOSECOND ? 1000000000 :
                     type.unit === TimeUnit.MICROSECOND ? 1000000 :
                     type.unit === TimeUnit.MILLISECOND ? 1000 : 1;
    const data = type.bitWidth === 32
        ? createTime32(length, nullBitmap, multiple, values as (number | null)[])
        : createTime64(length, nullBitmap, multiple, values as (Int32Array | null)[]);
    return { values: () => values, vector: Vector.new(Data.Time(type, 0, length, nullCount, nullBitmap, data)) };
}

function generateDecimal<T extends Decimal>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const data = fillRandom(Uint32Array, length * 4);
    const nullBitmap = createBitmap(length, nullCount);
    const view = new DataView(data.buffer, 0, data.byteLength);
    const values = memoize(() => {
        const values = [] as (Uint32Array | null)[];
        iterateBitmap(length, nullBitmap, (i, valid) => {
            values[i] = !valid ? null : new Uint32Array(data.buffer, 16 * i, 4);
        });
        return values;
    });
    iterateBitmap(length, nullBitmap, (i, valid) => {
        if (!valid) {
            view.setFloat64(4 * (i + 0), 0, true);
            view.setFloat64(4 * (i + 1), 0, true);
        }
    });
    return { values, vector: Vector.new(Data.Decimal(type, 0, length,  nullCount, nullBitmap, data))};
}

function generateInterval<T extends Interval>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0): GeneratedVector<V<T>> {
    const stride = (1 + type.unit);
    const nullBitmap = createBitmap(length, nullCount);
    const data = fillRandom(Int32Array, length * stride);
    const values = memoize(() => {
        const values = [] as (Int32Array | null)[];
        iterateBitmap(length, nullBitmap, (i: number, valid: boolean) => {
            values[i] = !valid ? null : stride === 2
                ? new Int32Array(data.buffer, 4 * i * stride, stride)
                : new Int32Array([data[i] / 12 | 0, data[i] % 12 | 0]);
        });
        return values;
    });
    iterateBitmap(length, nullBitmap, (i: number, valid: boolean) => {
        !valid && data.set(new Int32Array(stride), i * stride)
    });
    return { values, vector: Vector.new(Data.Interval(type, 0, length, nullCount, nullBitmap, data)) };
}

function generateList<T extends List>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0, child = this.visit(type.children[0].type, length * 3, nullCount * 3)): GeneratedVector<V<T>> {
    const childVec = child.vector;
    const nullBitmap = createBitmap(length, nullCount);
    const stride = childVec.length / (length - nullCount);
    const offsets = createVariableWidthOffsets(length, nullBitmap, childVec.length, stride);
    const values = memoize(() => {
        const childValues = child.values();
        const values: (T['valueType'] | null)[] = [...offsets.slice(1)]
            .map((offset, i) => isValid(nullBitmap, i) ? offset : null)
            .map((o, i) => o == null ? null : childValues.slice(offsets[i], o));
        return values;
    });
    return { values, vector: Vector.new(Data.List(type, 0, length, nullCount, nullBitmap, offsets, childVec)) };
}

function generateFixedSizeList<T extends FixedSizeList>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0, child = this.visit(type.children[0].type, length * type.listSize, nullCount * type.listSize)): GeneratedVector<V<T>> {
    const nullBitmap = createBitmap(length, nullCount);
    const values = memoize(() => {
        const childValues = child.values();
        const values = [] as (T['valueType'] | null)[];
        for (let i = -1, stride = type.listSize; ++i < length;) {
            values[i] = isValid(nullBitmap, i) ? childValues.slice(i * stride, (i + 1) * stride) : null;
        }
        return values;
    });
    return { values, vector: Vector.new(Data.FixedSizeList(type, 0, length, nullCount, nullBitmap, child.vector)) };
}

function generateDictionary<T extends Dictionary>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0, dictionary = this.visit(type.dictionary, length, 0)): GeneratedVector<V<T>> {

    const t = <any> type;
    const currValues = t.dictionaryValues;
    const hasDict = t.dictionaryVector && t.dictionaryVector.length > 0;
    const dict = hasDict ? t.dictionaryVector.concat(dictionary.vector) : dictionary.vector;
    const vals = hasDict ? (() => [...currValues(), ...dictionary.values()]) : dictionary.values;

    const maxIdx = dict.length - 1;
    const keys = new t.indices.ArrayType(length);
    const nullBitmap = createBitmap(length, nullCount);

    const values = memoize(() => {
        const dict = vals();
        const values = [] as (T['TValue'] | null)[];
        iterateBitmap(length, nullBitmap, (i, valid) => {
            values[i] = !valid ? null : dict[keys[i]];
        });
        return values;
    });

    iterateBitmap(length, nullBitmap, (i, valid) => {
        keys[i] = !valid ? 0 : rand() * maxIdx | 0;
    });

    t.dictionaryVector = dict;
    t.dictionaryValues = vals;

    return { values, keys, vector: Vector.new(Data.Dictionary(type, 0, length, nullCount, nullBitmap, keys, dict)) };
}

function generateUnion<T extends Union>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0, children?: GeneratedVector<any>[]): GeneratedVector<V<T>> {

    const numChildren = type.children.length;

    if (!children) {
        if (type.mode === UnionMode.Sparse) {
            children = type.children.map((f) => this.visit(f.type, length, nullCount));
        } else {
            const childLength = Math.ceil(length / numChildren);
            const childNullCount = (nullCount / childLength) | 0;
            children = type.children.map((f) => this.visit(f.type, childLength, childNullCount));
        }
    }

    const typeIds = type.typeIds;
    const typeIdsBuffer = new Int8Array(length);
    const vecs = children.map(({ vector }) => vector);
    const cols = children.map(({ values }) => values);
    const nullBitmap = createBitmap(length, nullCount);
    const typeIdToChildIndex = typeIds.reduce((typeIdToChildIndex, typeId, idx) => {
        return (typeIdToChildIndex[typeId] = idx) && typeIdToChildIndex || typeIdToChildIndex;
    }, Object.create(null) as { [key: number]: number })
    
    if (type.mode === UnionMode.Sparse) {
        const values = memoize(() => {
            const values = [] as any[];
            const childValues = cols.map((x) => x());
            iterateBitmap(length, nullBitmap, (i, valid) => {
                values[i] = !valid ? null : childValues[typeIdToChildIndex[typeIdsBuffer[i]]][i];
            });
            return values;
        });
        iterateBitmap(length, nullBitmap, (i, valid) => {
            typeIdsBuffer[i] = !valid ? 0 : typeIds[rand() * numChildren | 0];
        });
        return { values, vector: Vector.new(Data.Union(type as SparseUnion, 0, length, nullCount, nullBitmap, typeIdsBuffer, vecs)) } as GeneratedVector<V<T>>;
    }

    const offsets = new Int32Array(length);
    const values = memoize(() => {
        const values = [] as any[];
        const childValues = cols.map((x) => x());
        iterateBitmap(length, nullBitmap, (i, valid) => {
            values[i] = !valid ? null : childValues[typeIdToChildIndex[typeIdsBuffer[i]]][offsets[i]];
        });
        return values;
    });
    iterateBitmap(length, nullBitmap, (i, valid) => {
        if (!valid) {
            offsets[i] = 0;
            typeIdsBuffer[i] = 0;
        } else {
            const colIdx = rand() * numChildren | 0;
            offsets[i] = i / numChildren | 0;
            typeIdsBuffer[i] = typeIds[colIdx];
        }
    });
    return { values, vector: Vector.new(Data.Union(type as DenseUnion, 0, length, nullCount, nullBitmap, typeIdsBuffer, offsets, vecs)) } as GeneratedVector<V<T>>;
}

function generateStruct<T extends Struct>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0, children = type.children.map((f) => this.visit(f.type, length, nullCount))): GeneratedVector<V<T>> {
    const vecs = children.map(({ vector }) => vector);
    const cols = children.map(({ values }) => values);
    const nullBitmap = createBitmap(length, nullCount);
    const values = memoize(() => {
        const values = [] as any[];
        const childValues = cols.map((x) => x());
        iterateBitmap(length, nullBitmap, (i, valid) => {
            values[i] = !valid ? null : childValues.map((col) => col[i]);
        });
        return values;
    });
    return { values, vector: Vector.new(Data.Struct(type, 0, length, nullCount, nullBitmap, vecs)) };
}

function generateMap<T extends Map_>(this: TestDataVectorGenerator, type: T, length = 100, nullCount = length * 0.2 | 0, children = type.children.map((f) => this.visit(f.type, length, nullCount))): GeneratedVector<V<T>> {
    const vecs = children.map(({ vector }) => vector);
    const cols = children.map(({ values }) => values);
    const nullBitmap = createBitmap(length, nullCount);
    const values = memoize(() => {
        const values = [] as any[];
        const childValues = cols.map((x) => x());
        const names = type.children.map((f) => f.name);
        iterateBitmap(length, nullBitmap, (i, valid) => {
            values[i] = !valid ? null : childValues.reduce((row, col, j) => ({
                ...row, [names[j]]: col[i]
            }), {});
        });
        return values;
    });
    return { values, vector: Vector.new(Data.Map(type, 0, length, nullCount, nullBitmap, vecs)) };
}

type TypedArrayConstructor =
    (typeof Int8Array) |
    (typeof Int16Array) |
    (typeof Int32Array) |
    (typeof Uint8Array) |
    (typeof Uint16Array) |
    (typeof Uint32Array) |
    (typeof Float32Array) |
    (typeof Float64Array);


const rand = Math.random.bind(Math);
const randomBytes = (length: number) => fillRandom(Uint8Array, length);
const randomString = ((opts) =>
    (length: number) => randomatic('?', length, opts)
)({ chars: `abcdefghijklmnopqrstuvwxyz0123456789_` });

const memoize = (fn: () => any) => ((x?: any) => () => x || (x = fn()))();

const encodeUtf8 = ((encoder) =>
    encoder.encode.bind(encoder) as (input?: string, options?: { stream?: boolean }) => Uint8Array
)(new TextEncoder('utf-8'));

function fillRandom<T extends TypedArrayConstructor>(ArrayType: T, length: number) {
    const BPE = ArrayType.BYTES_PER_ELEMENT;
    const array = new ArrayType(length);
    const max = (2 ** (8 * BPE)) - 1;
    for (let i = -1; ++i < length; array[i] = rand() * max * (rand() > 0.5 ? -1 : 1));
    return array as InstanceType<T>;
}

function isValid(bitmap: Uint8Array, i: number) {
    return (bitmap[i >> 3] & 1 << (i % 8)) !== 0;
}

function iterateBitmap(length: number, bitmap: Uint8Array, fn: (index: number, valid: boolean) => any) {
    let byteIndex = 0, valueIndex = 0;
    for (let bit = 0; length > 0; bit = 0) {
        let byte = bitmap[byteIndex++];
        do {
            fn(valueIndex++, (byte & 1 << bit) !== 0);
        } while (--length > 0 && ++bit < 8);
    }
}

function createBitmap(length: number, nullCount: number) {
    const nulls = Object.create(null) as { [key: number]: boolean };
    const bytes = new Uint8Array((((length >> 3) + 7) & ~7) || 8).fill(255);
    for (let i, j = -1; ++j < nullCount;) {
        while (nulls[i = (rand() * length) | 0]);
        nulls[i] = true;
        bytes[i >> 3] &= ~(1 << (i % 8)); // false
    }
    return bytes;
}

function createVariableWidthOffsets(length: number, nullBitmap: Uint8Array, max = Infinity, stride = 20) {
    const offsets = new Int32Array(length + 1);
    iterateBitmap(length, nullBitmap, (i, valid) => {
        offsets[i + 1] = valid ? Math.min(max, offsets[i] + (rand() * stride | 0)) : offsets[i];
    });
    return offsets;
}

function createVariableWidthBytes(length: number, nullBitmap: Uint8Array, offsets: Int32Array, getBytes: (index: number) => Uint8Array) {
    const bytes = new Uint8Array(offsets[length]);
    iterateBitmap(length, nullBitmap, (i, valid) => {
        valid && bytes.set(getBytes(i), offsets[i]);
    });
    return bytes;
}

function createDate32(length: number, nullBitmap: Uint8Array, values: (number | null)[] = []) {
    const data = new Int32Array(length).fill(Date.now() / 86400000 | 0);
    iterateBitmap(length, nullBitmap, (i, valid) => {
        if (!valid) {
            data[i] = 0;
            values[i] = null;
        } else {
            data[i] = data[i] + (rand() * 10000 * (rand() > 0.5 ? -1 : 1)) | 0;
            values[i] = data[i] * 86400000;
        }
    });
    return data;
}

function createDate64(length: number, nullBitmap: Uint8Array, values: (number | null)[] = []) {
    const data = new Int32Array(length * 2).fill(0);
    const data32 = createDate32(length, nullBitmap, values);
    iterateBitmap(length, nullBitmap, (i, valid) => {
        if (valid) {
            const value = data32[i] * 86400000;
            const hi = (value / 4294967296) | 0;
            const lo = (value - 4294967296 * hi) | 0;
            values[i] = value;
            data[i * 2 + 0] = lo;
            data[i * 2 + 1] = hi;
        }
    });
    return data;
}

function createTimestamp(length: number, nullBitmap: Uint8Array, multiple: number, values: (number | null)[] = []) {
    const mult = 86400 * multiple;
    const data = new Int32Array(length * 2).fill(0);
    const data32 = createDate32(length, nullBitmap, values);
    iterateBitmap(length, nullBitmap, (i, valid) => {
        if (valid) {
            const value = data32[i] * mult;
            const hi = (value / 4294967296) | 0;
            const lo = (value - 4294967296 * hi) | 0;
            data[i * 2 + 0] = lo;
            data[i * 2 + 1] = hi;
        }
    });
    return data;
}

function createTime32(length: number, nullBitmap: Uint8Array, multiple: number, values: (number | null)[] = []) {
    const data = new Int32Array(length).fill(0);
    iterateBitmap(length, nullBitmap, (i, valid) => {
        if (!valid) {
            data[i] = 0;
            values[i] = null;
        } else {
            values[i] = data[i] = ((1000 * rand()) | 0 * multiple) * (rand() > 0.5 ? -1 : 1);
        }
    });
    return data;
}

function createTime64(length: number, nullBitmap: Uint8Array, multiple: number, values: (Int32Array | null)[] = []) {
    const data = new Int32Array(length * 2).fill(0);
    iterateBitmap(length, nullBitmap, (i, valid) => {
        if (!valid) {
            values[i] = null;
        } else {
            const value = (1000 * rand()) | 0 * multiple;
            const hi = (value / 4294967296) | 0;
            const lo = (value - 4294967296 * hi) | 0;
            data[i * 2 + 0] = lo;
            data[i * 2 + 1] = hi;
            values[i] = data.subarray(i * 2, (i + 1) * 2);
        }
    });
    return data;
}
