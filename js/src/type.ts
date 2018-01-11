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

import { Data } from './data';
import * as Schema_ from './fb/Schema';
import { Vector, View } from './vector';
import { flatbuffers } from 'flatbuffers';
import { TypeVisitor, VisitorNode } from './visitor';
import { Field, DictionaryBatch } from './ipc/message';

export import Long = flatbuffers.Long;
export import ArrowType = Schema_.org.apache.arrow.flatbuf.Type;
export import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
export import TimeUnit = Schema_.org.apache.arrow.flatbuf.TimeUnit;
export import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
export import UnionMode = Schema_.org.apache.arrow.flatbuf.UnionMode;
export import VectorType = Schema_.org.apache.arrow.flatbuf.VectorType;
export import IntervalUnit = Schema_.org.apache.arrow.flatbuf.IntervalUnit;

export type TimeBitWidth = 32 | 64;
export type IntBitWidth = 1 | 8 | 16 | 32 | 64;

export type NumericType = Int | Float | Date_ | Time | Interval | Timestamp;
export type FixedSizeType = Int64 |  Uint64 | Decimal | FixedSizeBinary;
export type PrimitiveType = NumericType | FixedSizeType;

export type FlatListType = Utf8 | Binary; // <-- these types have `offset`, `data`, and `validity` buffers
export type FlatType = Bool | PrimitiveType | FlatListType; // <-- these types have `data` and `validity` buffers
export type ListType = List<any> | FixedSizeList<any> | FlatListType; // <-- these types have `offset` and `validity` buffers
export type NestedType = Map_ | Struct | List<any> | FixedSizeList<any> | Union<any>; // <-- these types have `validity` buffer and nested childData

/**
 * *
 * Main data type enumeration:
 * *
 * Data types in this library are all *logical*. They can be expressed as
 * either a primitive physical type (bytes or bits of some fixed size), a
 * nested type consisting of other data types, or another data type (e.g. a
 * timestamp encoded as an int64)
 */
 export const enum Type {
    NONE            =  0,  // The default placeholder type
    Null            =  1,  // A NULL type having no physical storage
    Int             =  2,  // Signed or unsigned 8, 16, 32, or 64-bit little-endian integer
    Float           =  3,  // 2, 4, or 8-byte floating point value
    Binary          =  4,  // Variable-length bytes (no guarantee of UTF8-ness)
    Utf8            =  5,  // UTF8 variable-length string as List<Char>
    Bool            =  6,  // Boolean as 1 bit, LSB bit-packed ordering
    Decimal         =  7,  // Precision-and-scale-based decimal type. Storage type depends on the parameters.
    Date            =  8,  // int32_t days or int64_t milliseconds since the UNIX epoch
    Time            =  9,  // Time as signed 32 or 64-bit integer, representing either seconds, milliseconds, microseconds, or nanoseconds since midnight since midnight
    Timestamp       = 10,  // Exact timestamp encoded with int64 since UNIX epoch (Default unit millisecond)
    Interval        = 11,  // YEAR_MONTH or DAY_TIME interval in SQL style
    List            = 12,  // A list of some logical data type
    Struct          = 13,  // Struct of logical types
    Union           = 14,  // Union of logical types
    FixedSizeBinary = 15,  // Fixed-size binary. Each value occupies the same number of bytes
    FixedSizeList   = 16,  // Fixed-size list. Each value occupies the same number of bytes
    Map             = 17,  // Map of named logical types
    Dictionary      = 'Dictionary',  // Dictionary aka Category type
    DenseUnion      = 'DenseUnion',  // Dense Union of logical types
    SparseUnion     = 'SparseUnion',  // Sparse Union of logical types
}

export interface DataType<TType extends Type = any> {
    readonly TType: TType;
    readonly TArray: any;
    readonly TValue: any;
    readonly ArrayType: any;
}

export abstract class DataType<TType extends Type = any> implements Partial<VisitorNode> {

    public get [Symbol.toStringTag]() { return 'DataType'; }

    static            isNull (x: DataType): x is Null            { return x.TType === Type.Null;            }
    static             isInt (x: DataType): x is Int             { return x.TType === Type.Int;             }
    static           isFloat (x: DataType): x is Float           { return x.TType === Type.Float;           }
    static          isBinary (x: DataType): x is Binary          { return x.TType === Type.Binary;          }
    static            isUtf8 (x: DataType): x is Utf8            { return x.TType === Type.Utf8;            }
    static            isBool (x: DataType): x is Bool            { return x.TType === Type.Bool;            }
    static         isDecimal (x: DataType): x is Decimal         { return x.TType === Type.Decimal;         }
    static            isDate (x: DataType): x is Date_           { return x.TType === Type.Date;            }
    static            isTime (x: DataType): x is Time            { return x.TType === Type.Time;            }
    static       isTimestamp (x: DataType): x is Timestamp       { return x.TType === Type.Timestamp;       }
    static        isInterval (x: DataType): x is Interval        { return x.TType === Type.Interval;        }
    static            isList (x: DataType): x is List            { return x.TType === Type.List;            }
    static          isStruct (x: DataType): x is Struct          { return x.TType === Type.Struct;          }
    static           isUnion (x: DataType): x is Union           { return x.TType === Type.Union;           }
    static      isDenseUnion (x: DataType): x is DenseUnion      { return x.TType === Type.DenseUnion;      }
    static     isSparseUnion (x: DataType): x is SparseUnion     { return x.TType === Type.SparseUnion;     }
    static isFixedSizeBinary (x: DataType): x is FixedSizeBinary { return x.TType === Type.FixedSizeBinary; }
    static   isFixedSizeList (x: DataType): x is FixedSizeList   { return x.TType === Type.FixedSizeList;   }
    static             isMap (x: DataType): x is Map_            { return x.TType === Type.Map;             }
    static      isDictionary (x: DataType): x is Dictionary      { return x.TType === Type.Dictionary;      }

    constructor(public readonly TType: TType,
                public readonly children?: Field[]) {}

    acceptTypeVisitor(visitor: TypeVisitor): any {
        switch (this.TType) {
            case Type.Null:            return DataType.isNull(this)            && visitor.visitNull(this)            || null;
            case Type.Int:             return DataType.isInt(this)             && visitor.visitInt(this)             || null;
            case Type.Float:           return DataType.isFloat(this)           && visitor.visitFloat(this)           || null;
            case Type.Binary:          return DataType.isBinary(this)          && visitor.visitBinary(this)          || null;
            case Type.Utf8:            return DataType.isUtf8(this)            && visitor.visitUtf8(this)            || null;
            case Type.Bool:            return DataType.isBool(this)            && visitor.visitBool(this)            || null;
            case Type.Decimal:         return DataType.isDecimal(this)         && visitor.visitDecimal(this)         || null;
            case Type.Date:            return DataType.isDate(this)            && visitor.visitDate(this)            || null;
            case Type.Time:            return DataType.isTime(this)            && visitor.visitTime(this)            || null;
            case Type.Timestamp:       return DataType.isTimestamp(this)       && visitor.visitTimestamp(this)       || null;
            case Type.Interval:        return DataType.isInterval(this)        && visitor.visitInterval(this)        || null;
            case Type.List:            return DataType.isList(this)            && visitor.visitList(this)            || null;
            case Type.Struct:          return DataType.isStruct(this)          && visitor.visitStruct(this)          || null;
            case Type.Union:           return DataType.isUnion(this)           && visitor.visitUnion(this)           || null;
            case Type.FixedSizeBinary: return DataType.isFixedSizeBinary(this) && visitor.visitFixedSizeBinary(this) || null;
            case Type.FixedSizeList:   return DataType.isFixedSizeList(this)   && visitor.visitFixedSizeList(this)   || null;
            case Type.Map:             return DataType.isMap(this)             && visitor.visitMap(this)             || null;
            case Type.Dictionary:      return DataType.isDictionary(this)      && visitor.visitDictionary(this)      || null;
            default: return null;
        }
    }
}

export interface Null extends DataType<Type.Null> { TArray: void; TValue: null; }
export class Null extends DataType<Type.Null> {
    constructor() { super(Type.Null); }
    public get [Symbol.toStringTag]() { return 'Null'; }
    public toString() { return `Null`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitNull(this);
    }
}

export interface Int<TValueType = any, TArrayType extends IntArray = IntArray> extends DataType<Type.Int> { TArray: TArrayType; TValue: TValueType; }
export class Int<TValueType = any, TArrayType extends IntArray = IntArray> extends DataType<Type.Int> {
    // @ts-ignore
    public readonly ArrayType: TypedArrayConstructor<TArrayType>;
    constructor(public readonly isSigned: boolean,
                public readonly bitWidth: IntBitWidth) {
        super(Type.Int);
    }
    public get [Symbol.toStringTag]() { return 'Int'; }
    public toString() { return `${this.isSigned ? `` : `u`}int${this.bitWidth}`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any { return visitor.visitInt(this); }
}

export class Int8 extends Int<number, Int8Array> {
    constructor() { super(true, 8); }
    public get [Symbol.toStringTag]() { return 'Int8'; }
    public get ArrayType() { return Int8Array; }
}

export class Int16 extends Int<number, Int16Array> {
    constructor() { super(true, 16); }
    public get [Symbol.toStringTag]() { return 'Int16'; }
    public get ArrayType() { return Int16Array; }
}

export class Int32 extends Int<number, Int32Array> {
    constructor() { super(true, 32); }
    public get [Symbol.toStringTag]() { return 'Int32'; }
    public get ArrayType() { return Int32Array; }
}

export class Int64 extends Int<Int32Array, Int32Array> {
    constructor() { super(true, 64); }
    public get [Symbol.toStringTag]() { return 'Int64'; }
    public get ArrayType() { return Int32Array; }
}

export class Uint8 extends Int<number, Uint8Array> {
    constructor() { super(false, 8); }
    public get [Symbol.toStringTag]() { return 'Uint8'; }
    public get ArrayType() { return Uint8Array; }
}

export class Uint16 extends Int<number, Uint16Array> {
    constructor() { super(false, 16); }
    public get [Symbol.toStringTag]() { return 'Uint16'; }
    public get ArrayType() { return Uint16Array; }
}

export class Uint32 extends Int<number, Uint32Array> {
    constructor() { super(false, 32); }
    public get [Symbol.toStringTag]() { return 'Uint32'; }
    public get ArrayType() { return Uint32Array; }
}

export class Uint64 extends Int<Uint32Array, Uint32Array> {
    constructor() { super(false, 64); }
    public get [Symbol.toStringTag]() { return 'Uint64'; }
    public get ArrayType() { return Uint32Array; }
}

export interface Float<TArrayType extends FloatArray = FloatArray> extends DataType<Type.Float> { TArray: TArrayType; TValue: number; }
export class Float<TArrayType extends FloatArray = FloatArray> extends DataType<Type.Float> {
    // @ts-ignore
    public readonly ArrayType: TypedArrayConstructor<TArrayType>;
    public get [Symbol.toStringTag]() { return 'Float'; }
    constructor(public readonly precision: Precision) {
        super(Type.Float);
    }
    public toString() { return `Float precision[${this.precision}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any { return visitor.visitFloat(this); }
}

export class Float16 extends Float<Uint16Array> {
    constructor() { super(Precision.HALF); }
    public get [Symbol.toStringTag]() { return 'Float16'; }
    public get ArrayType() { return  Uint16Array; }
}

export class Float32 extends Float<Float32Array> {
    constructor() { super(Precision.SINGLE); }
    public get [Symbol.toStringTag]() { return 'Float32'; }
    public get ArrayType() { return Float32Array; }
}

export class Float64 extends Float<Float64Array> {
    constructor() { super(Precision.DOUBLE); }
    public get [Symbol.toStringTag]() { return 'Float64'; }
    public get ArrayType() { return Float64Array; }
}

export interface Binary extends DataType<Type.Binary> { TArray: Uint8Array; TValue: Uint8Array; }
export class Binary extends DataType<Type.Binary> {
    constructor() { super(Type.Binary); }
    public get [Symbol.toStringTag]() { return 'Binary'; }
    public toString() { return `Binary`; }
    public get ArrayType() { return Uint8Array; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitBinary(this);
    }
}

export interface Utf8 extends DataType<Type.Utf8> { TArray: Uint8Array; TValue: string; }
export class Utf8 extends DataType<Type.Utf8> {
    constructor() { super(Type.Utf8); }
    public get [Symbol.toStringTag]() { return 'Utf8'; }
    public toString() { return `Utf8`; }
    public get ArrayType() { return Uint8Array; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitUtf8(this);
    }
}

export interface Bool extends DataType<Type.Bool> { TArray: Uint8Array; TValue: boolean; }
export class Bool extends DataType<Type.Bool> {
    constructor() { super(Type.Bool); }
    public get [Symbol.toStringTag]() { return 'Bool'; }
    public toString() { return `Bool`; }
    public get ArrayType() { return Uint8Array; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitBool(this);
    }
}

export interface Decimal extends DataType<Type.Decimal> { TArray: Uint32Array; TValue: Uint32Array; }
export class Decimal extends DataType<Type.Decimal> {
    constructor(public readonly scale: number,
                public readonly precision: number) {
        super(Type.Decimal);
    }
    public get [Symbol.toStringTag]() { return 'Decimal'; }
    public get ArrayType() { return Uint32Array; }
    public toString() { return `Decimal scale[${this.scale}], precision[${this.precision}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitDecimal(this);
    }
}

/* tslint:disable:class-name */
export interface Date_ extends DataType<Type.Date> { TArray: Int32Array; TValue: Date; }
export class Date_ extends DataType<Type.Date> {
    constructor(public readonly unit: DateUnit) { super(Type.Date); }
    public get [Symbol.toStringTag]() { return 'Date_'; }
    public get ArrayType() { return Int32Array; }
    public toString() { return `Date unit[${this.unit}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any { return visitor.visitDate(this); }
}

export interface Time extends DataType<Type.Time> { TArray: Uint32Array; TValue: number; }
export class Time extends DataType<Type.Time> {
    constructor(public readonly unit: TimeUnit,
                public readonly bitWidth: TimeBitWidth) {
        super(Type.Time);
    }
    public get [Symbol.toStringTag]() { return 'Time'; }
    public get ArrayType() { return Int32Array; }
    public toString() { return `Time unit[${this.unit}], bitWidth[${this.bitWidth}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any { return visitor.visitTime(this); }
}

export interface Timestamp extends DataType<Type.Timestamp> { TArray: Int32Array; TValue: number; }
export class Timestamp extends DataType<Type.Timestamp> {
    constructor(public unit: TimeUnit, public timezone?: string | null) {
        super(Type.Timestamp);
    }
    public get [Symbol.toStringTag]() { return 'Timestamp'; }
    public get ArrayType() { return Int32Array; }
    public toString() { return `Timestamp unit[${this.unit}], timezone[${this.timezone}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitTimestamp(this);
    }
}

export interface Interval extends DataType<Type.Interval> { TArray: Int32Array; TValue: Int32Array; }
export class Interval extends DataType<Type.Interval> {
    constructor(public unit: IntervalUnit) {
        super(Type.Interval);
    }
    public get [Symbol.toStringTag]() { return 'Interval'; }
    public get ArrayType() { return Int32Array; }
    public toString() { return `Interval unit[${this.unit}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitInterval(this);
    }
}

export interface List<T extends DataType = any> extends DataType<Type.List>  { TArray: any; TValue: Vector<T>; }
export class List<T extends DataType = any> extends DataType<Type.List> {
    constructor(public children: Field[]) {
        super(Type.List, children);
    }
    public get [Symbol.toStringTag]() { return 'List'; }
    public toString() { return `List`; }
    public get valueType() { return this.children[0].type as T; }
    public get valueField() { return this.children[0] as Field<T>; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitList(this);
    }
}

export interface Struct extends DataType<Type.Struct> { TArray: Uint8Array; TValue: View<any>; }
export class Struct extends DataType<Type.Struct> {
    constructor(public children: Field[]) {
        super(Type.Struct, children);
    }
    public get [Symbol.toStringTag]() { return 'Struct'; }
    public toString() { return `Struct`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitStruct(this);
    }
}

export interface Union<TType extends Type = any> extends DataType<TType> { TArray: Int8Array; TValue: any; }
export class Union<TType extends Type = any> extends DataType<TType> {
    constructor(TType: TType,
                public readonly mode: UnionMode,
                public readonly typeIds: ArrowType[],
                public readonly children: Field[]) {
        super(TType, children);
    }
    public get [Symbol.toStringTag]() { return 'Union'; }
    public get ArrayType() { return Int8Array; }
    public toString() { return `Union mode[${this.mode}] typeIds[${this.typeIds}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any { return visitor.visitUnion(this); }
}

export class DenseUnion extends Union<Type.DenseUnion> {
    constructor(typeIds: ArrowType[], children: Field[]) {
        super(Type.DenseUnion, UnionMode.Dense, typeIds, children);
    }
    public get [Symbol.toStringTag]() { return 'DenseUnion'; }
    public toString() { return `DenseUnion typeIds[${this.typeIds}]`; }
}

export class SparseUnion extends Union<Type.SparseUnion> {
    constructor(typeIds: ArrowType[], children: Field[]) {
        super(Type.SparseUnion, UnionMode.Sparse, typeIds, children);
    }
    public get [Symbol.toStringTag]() { return 'SparseUnion'; }
    public toString() { return `SparseUnion typeIds[${this.typeIds}]`; }
}

export interface FixedSizeBinary extends DataType<Type.FixedSizeBinary> { TArray: Uint8Array; TValue: Uint8Array; }
export class FixedSizeBinary extends DataType<Type.FixedSizeBinary> {
    constructor(public readonly byteWidth: number) {
        super(Type.FixedSizeBinary);
    }
    public get [Symbol.toStringTag]() { return 'FixedSizeBinary'; }
    public get ArrayType() { return Uint8Array; }
    public toString() { return `FixedSizeBinary byteWidth[${this.byteWidth}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any { return visitor.visitFixedSizeBinary(this); }
}

export interface FixedSizeList<T extends DataType = any> extends DataType<Type.FixedSizeList> { TArray: any; TValue: Vector<T>; }
export class FixedSizeList<T extends DataType = any> extends DataType<Type.FixedSizeList> {
    constructor(public readonly listSize: number,
                public readonly children: Field[]) {
        super(Type.FixedSizeList, children);
    }
    public get [Symbol.toStringTag]() { return 'FixedSizeList'; }
    public get valueType() { return this.children[0].type as T; }
    public get valueField() { return this.children[0] as Field<T>; }
    public toString() { return `FixedSizeList listSize[${this.listSize}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any { return visitor.visitFixedSizeList(this); }
}

/* tslint:disable:class-name */
export interface Map_ extends DataType<Type.Map> { TArray: Uint8Array; TValue: View<any>; }
export class Map_ extends DataType<Type.Map> {
    constructor(public readonly keysSorted: boolean,
                public readonly children: Field[]) {
        super(Type.Map, children);
    }
    public get [Symbol.toStringTag]() { return 'Map'; }
    public toString() { return `Map keysSorted[${this.keysSorted}]`; }
    public acceptTypeVisitor(visitor: TypeVisitor): any { return visitor.visitMap(this); }
}

export interface Dictionary<T extends DataType = any> extends DataType<Type.Dictionary> { TArray: T['TArray']; TValue: T['TValue']; }
export class Dictionary<T extends DataType> extends DataType<Type.Dictionary> {
    public readonly id: number;
    public readonly indicies: Data<Int>;
    public readonly isOrdered: boolean;
    public readonly dictionary: Data<T>;
    constructor(dictionary: Data<T>, indicies: Data<Int>, id?: Long | number | null, isOrdered?: boolean | null) {
        super(Type.Dictionary);
        this.indicies = indicies; // a dictionary index defaults to signed 32 bit int if unspecified
        this.dictionary = dictionary;
        this.isOrdered = isOrdered || false;
        this.id = id == null ?
            DictionaryBatch.atomicDictionaryId++ :
            typeof id === 'number' ? id : id.low ;
    }
    public get [Symbol.toStringTag]() { return 'Dictionary'; }
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return visitor.visitDictionary(this);
    }
}
export interface IterableArrayLike<T = any> extends ArrayLike<T>, Iterable<T> {}

export interface TypedArrayConstructor<T extends TypedArray = TypedArray> {
    readonly prototype: T;
    readonly BYTES_PER_ELEMENT: number;
    new (length: number): T;
    new (elements: Iterable<number>): T;
    new (arrayOrArrayBuffer: ArrayLike<number> | ArrayBufferLike): T;
    new (buffer: ArrayBufferLike, byteOffset: number, length?: number): T;
    of(...items: number[]): T;
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): T;
}

export type FloatArray = Uint16Array | Float32Array | Float64Array;
export type IntArray = Int8Array | Int16Array | Int32Array | Uint8Array | Uint16Array | Uint32Array;

export interface TypedArray extends Iterable<number> {
    [index: number]: number;
    readonly length: number;
    readonly byteLength: number;
    readonly byteOffset: number;
    readonly buffer: ArrayBufferLike;
    readonly BYTES_PER_ELEMENT: number;
    [Symbol.toStringTag]: any;
    [Symbol.iterator](): IterableIterator<number>;
    entries(): IterableIterator<[number, number]>;
    keys(): IterableIterator<number>;
    values(): IterableIterator<number>;
    copyWithin(target: number, start: number, end?: number): this;
    every(callbackfn: (value: number, index: number, array: TypedArray) => boolean, thisArg?: any): boolean;
    fill(value: number, start?: number, end?: number): this;
    filter(callbackfn: (value: number, index: number, array: TypedArray) => any, thisArg?: any): TypedArray;
    find(predicate: (value: number, index: number, obj: TypedArray) => boolean, thisArg?: any): number | undefined;
    findIndex(predicate: (value: number, index: number, obj: TypedArray) => boolean, thisArg?: any): number;
    forEach(callbackfn: (value: number, index: number, array: TypedArray) => void, thisArg?: any): void;
    includes(searchElement: number, fromIndex?: number): boolean;
    indexOf(searchElement: number, fromIndex?: number): number;
    join(separator?: string): string;
    lastIndexOf(searchElement: number, fromIndex?: number): number;
    map(callbackfn: (value: number, index: number, array: TypedArray) => number, thisArg?: any): TypedArray;
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: TypedArray) => number): number;
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: TypedArray) => number, initialValue: number): number;
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: TypedArray) => U, initialValue: U): U;
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: TypedArray) => number): number;
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: TypedArray) => number, initialValue: number): number;
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: TypedArray) => U, initialValue: U): U;
    reverse(): TypedArray;
    set(array: ArrayLike<number>, offset?: number): void;
    slice(start?: number, end?: number): TypedArray;
    some(callbackfn: (value: number, index: number, array: TypedArray) => boolean, thisArg?: any): boolean;
    sort(compareFn?: (a: number, b: number) => number): this;
    subarray(begin: number, end?: number): TypedArray;
    toLocaleString(): string;
    toString(): string;
}
