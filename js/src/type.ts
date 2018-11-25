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

import * as Schema_ from './fb/Schema';
import * as Message_ from './fb/Message';
import { Vector, View } from './vector';
import { flatbuffers } from 'flatbuffers';
import { DictionaryBatch } from './ipc/metadata';
import { TypeVisitor, VisitorNode } from './visitor';

export import Long = flatbuffers.Long;
export import ArrowType = Schema_.org.apache.arrow.flatbuf.Type;
export import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
export import TimeUnit = Schema_.org.apache.arrow.flatbuf.TimeUnit;
export import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
export import UnionMode = Schema_.org.apache.arrow.flatbuf.UnionMode;
export import VectorType = Schema_.org.apache.arrow.flatbuf.VectorType;
export import IntervalUnit = Schema_.org.apache.arrow.flatbuf.IntervalUnit;
export import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;
export import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;

function generateDictionaryMap(fields: Field[]) {
    const result: Map<number, Field<Dictionary>> = new Map();
    fields
        .filter((f) => f.type instanceof Dictionary)
        .forEach((f) => {
            if (result.has((f.type as Dictionary).id)) {
                throw new Error(`Cannot create Schema containing two dictionaries with the same ID`);
            }
            result.set((f.type as Dictionary).id, f as Field<Dictionary>);
        });
    return result;
}

export class Schema {
    public static from(vectors: Vector[]) {
        return new Schema(vectors.map((v, i) => new Field('' + i, v.type)));
    }
    // @ts-ignore
    protected _bodyLength: number;
    // @ts-ignore
    protected _headerType: MessageHeader;
    public readonly fields: Field[];
    public readonly version: MetadataVersion;
    public readonly metadata?: Map<string, string>;
    public readonly dictionaries: Map<number, Field<Dictionary>>;
    constructor(fields: Field[],
                metadata?: Map<string, string>,
                version: MetadataVersion = MetadataVersion.V4,
                dictionaries: Map<number, Field<Dictionary>> = generateDictionaryMap(fields)) {
        this.fields = fields;
        this.version = version;
        this.metadata = metadata;
        this.dictionaries = dictionaries;
    }
    public get bodyLength() { return this._bodyLength; }
    public get headerType() { return this._headerType; }
    public select(...fieldNames: string[]): Schema {
        const namesToKeep = fieldNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
        const newDictFields = new Map(), newFields = this.fields.filter((f) => namesToKeep[f.name]);
        this.dictionaries.forEach((f, dictId) => (namesToKeep[f.name]) && newDictFields.set(dictId, f));
        return new Schema(newFields, this.metadata, this.version, newDictFields);
    }
    public static [Symbol.toStringTag] = ((prototype: Schema) => {
        prototype._bodyLength = 0;
        prototype._headerType = MessageHeader.Schema;
        return 'Schema';
    })(Schema.prototype);
}

export class Field<T extends DataType = DataType> {
    public readonly type: T;
    public readonly name: string;
    public readonly nullable: boolean;
    public readonly metadata?: Map<string, string> | null;
    constructor(name: string, type: T, nullable = false, metadata?: Map<string, string> | null) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.metadata = metadata;
    }
    public toString() { return `${this.name}: ${this.type}`; }
    public get typeId(): T['TType'] { return this.type.TType; }
    public get [Symbol.toStringTag](): string { return 'Field'; }
    public get indices(): T | Int<any> {
        return DataType.isDictionary(this.type) ? this.type.indices : this.type;
    }
}

export type TimeBitWidth = 32 | 64;
export type IntBitWidth = 8 | 16 | 32 | 64;

export type NumericType = Int | Float | Date_ | Time | Interval | Timestamp;
export type FixedSizeType = Int64 |  Uint64 | Decimal | FixedSizeBinary;
export type PrimitiveType = NumericType | FixedSizeType;

export type FlatListType = Utf8 | Binary; // <-- these types have `offset`, `data`, and `validity` buffers
export type FlatType = Bool | PrimitiveType | FlatListType; // <-- these types have `data` and `validity` buffers
export type ListType = List<any>; // <-- these types have `offset` and `validity` buffers
export type NestedType = Map_ | Struct | List<any> | FixedSizeList<any> | Union<any>; // <-- these types have `validity` buffer and nested childData
export type SingleNestedType = List<any> | FixedSizeList<any>; // <-- these are nested types that can only have a single child

/**
 * *
 * Main data type enumeration:
 * *
 * Data types in this library are all *logical*. They can be expressed as
 * either a primitive physical type (bytes or bits of some fixed size), a
 * nested type consisting of other data types, or another data type (e.g. a
 * timestamp encoded as an int64)
 */
 export enum Type {
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

    // @ts-ignore
    public [Symbol.toStringTag]: string;

    static            isNull (x: any): x is Null            { return x && x.TType === Type.Null;            }
    static             isInt (x: any): x is Int             { return x && x.TType === Type.Int;             }
    static           isFloat (x: any): x is Float           { return x && x.TType === Type.Float;           }
    static          isBinary (x: any): x is Binary          { return x && x.TType === Type.Binary;          }
    static            isUtf8 (x: any): x is Utf8            { return x && x.TType === Type.Utf8;            }
    static            isBool (x: any): x is Bool            { return x && x.TType === Type.Bool;            }
    static         isDecimal (x: any): x is Decimal         { return x && x.TType === Type.Decimal;         }
    static            isDate (x: any): x is Date_           { return x && x.TType === Type.Date;            }
    static            isTime (x: any): x is Time            { return x && x.TType === Type.Time;            }
    static       isTimestamp (x: any): x is Timestamp       { return x && x.TType === Type.Timestamp;       }
    static        isInterval (x: any): x is Interval        { return x && x.TType === Type.Interval;        }
    static            isList (x: any): x is List            { return x && x.TType === Type.List;            }
    static          isStruct (x: any): x is Struct          { return x && x.TType === Type.Struct;          }
    static           isUnion (x: any): x is Union           { return x && x.TType === Type.Union;           }
    static      isDenseUnion (x: any): x is DenseUnion      { return x && x.TType === Type.DenseUnion;      }
    static     isSparseUnion (x: any): x is SparseUnion     { return x && x.TType === Type.SparseUnion;     }
    static isFixedSizeBinary (x: any): x is FixedSizeBinary { return x && x.TType === Type.FixedSizeBinary; }
    static   isFixedSizeList (x: any): x is FixedSizeList   { return x && x.TType === Type.FixedSizeList;   }
    static             isMap (x: any): x is Map_            { return x && x.TType === Type.Map;             }
    static      isDictionary (x: any): x is Dictionary      { return x && x.TType === Type.Dictionary;      }

    constructor(public readonly TType: TType,
                public readonly children?: Field[]) {}
    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return TypeVisitor.visitTypeInline(visitor, this);
    }
    protected static [Symbol.toStringTag] = ((proto: DataType) => {
        (<any> proto).ArrayType = Array;
        return proto[Symbol.toStringTag] = 'DataType';
    })(DataType.prototype);
}

export interface Null extends DataType<Type.Null> { TArray: void; TValue: null; }
export class Null extends DataType<Type.Null> {
    constructor() {
        super(Type.Null);
    }
    public toString() { return `Null`; }
    protected static [Symbol.toStringTag] = ((proto: Null) => {
        return proto[Symbol.toStringTag] = 'Null';
    })(Null.prototype);
}

export interface Int<TValueType = any, TArrayType extends IntArray = IntArray> extends DataType<Type.Int> { TArray: TArrayType; TValue: TValueType; }
export class Int<TValueType = any, TArrayType extends IntArray = IntArray> extends DataType<Type.Int> {
    constructor(public readonly isSigned: boolean,
                public readonly bitWidth: IntBitWidth) {
        super(Type.Int);
    }
    public get ArrayType(): TypedArrayConstructor<TArrayType> {
        switch (this.bitWidth) {
            case  8: return (this.isSigned ?  Int8Array :  Uint8Array) as any;
            case 16: return (this.isSigned ? Int16Array : Uint16Array) as any;
            case 32: return (this.isSigned ? Int32Array : Uint32Array) as any;
            case 64: return (this.isSigned ? Int32Array : Uint32Array) as any;
        }
        throw new Error(`Unrecognized ${this[Symbol.toStringTag]} type`);
    }
    public toString() { return `${this.isSigned ? `I` : `Ui`}nt${this.bitWidth}`; }
    protected static [Symbol.toStringTag] = ((proto: Int) => {
        return proto[Symbol.toStringTag] = 'Int';
    })(Int.prototype);
}

export class Int8 extends Int<number, Int8Array> { constructor() { super(true, 8); } }
export class Int16 extends Int<number, Int16Array> { constructor() { super(true, 16); } }
export class Int32 extends Int<number, Int32Array> { constructor() { super(true, 32); } }
export class Int64 extends Int<Int32Array, Int32Array> { constructor() { super(true, 64); } }
export class Uint8 extends Int<number, Uint8Array> { constructor() { super(false, 8); } }
export class Uint16 extends Int<number, Uint16Array> { constructor() { super(false, 16); } }
export class Uint32 extends Int<number, Uint32Array> { constructor() { super(false, 32); } }
export class Uint64 extends Int<Uint32Array, Uint32Array> { constructor() { super(false, 64); } }

export interface Float<TArrayType extends FloatArray = FloatArray> extends DataType<Type.Float> { TArray: TArrayType; TValue: number; }
export class Float<TArrayType extends FloatArray = FloatArray> extends DataType<Type.Float> {
    constructor(public readonly precision: Precision) {
        super(Type.Float);
    }
    // @ts-ignore
    public get ArrayType(): TypedArrayConstructor<TArrayType> {
        switch (this.precision) {
            case Precision.HALF: return Uint16Array as any;
            case Precision.SINGLE: return Float32Array as any;
            case Precision.DOUBLE: return Float64Array as any;
        }
        throw new Error(`Unrecognized ${this[Symbol.toStringTag]} type`);
    }
    public toString() { return `Float${(this.precision << 5) || 16}`; }
    protected static [Symbol.toStringTag] = ((proto: Float) => {
        return proto[Symbol.toStringTag] = 'Float';
    })(Float.prototype);
}

export class Float16 extends Float<Uint16Array> { constructor() { super(Precision.HALF); } }
export class Float32 extends Float<Float32Array> { constructor() { super(Precision.SINGLE); } }
export class Float64 extends Float<Float64Array> { constructor() { super(Precision.DOUBLE); } }

export interface Binary extends DataType<Type.Binary> { TArray: Uint8Array; TValue: Uint8Array; }
export class Binary extends DataType<Type.Binary> {
    constructor() {
        super(Type.Binary);
    }
    public toString() { return `Binary`; }
    protected static [Symbol.toStringTag] = ((proto: Binary) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'Binary';
    })(Binary.prototype);
}

export interface Utf8 extends DataType<Type.Utf8> { TArray: Uint8Array; TValue: string; }
export class Utf8 extends DataType<Type.Utf8> {
    constructor() {
        super(Type.Utf8);
    }
    public toString() { return `Utf8`; }
    protected static [Symbol.toStringTag] = ((proto: Utf8) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'Utf8';
    })(Utf8.prototype);
}

export interface Bool extends DataType<Type.Bool> { TArray: Uint8Array; TValue: boolean; }
export class Bool extends DataType<Type.Bool> {
    constructor() {
        super(Type.Bool);
    }
    public toString() { return `Bool`; }
    protected static [Symbol.toStringTag] = ((proto: Bool) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'Bool';
    })(Bool.prototype);
}

export interface Decimal extends DataType<Type.Decimal> { TArray: Uint32Array; TValue: Uint32Array; }
export class Decimal extends DataType<Type.Decimal> {
    constructor(public readonly scale: number,
                public readonly precision: number) {
        super(Type.Decimal);
    }
    public toString() { return `Decimal[${this.precision}e${this.scale > 0 ? `+` : ``}${this.scale}]`; }
    protected static [Symbol.toStringTag] = ((proto: Decimal) => {
        (<any> proto).ArrayType = Uint32Array;
        return proto[Symbol.toStringTag] = 'Decimal';
    })(Decimal.prototype);
}

/* tslint:disable:class-name */
export interface Date_ extends DataType<Type.Date> { TArray: Int32Array; TValue: Date; }
export class Date_ extends DataType<Type.Date> {
    constructor(public readonly unit: DateUnit) {
        super(Type.Date);
    }
    public toString() { return `Date${(this.unit + 1) * 32}<${DateUnit[this.unit]}>`; }
    protected static [Symbol.toStringTag] = ((proto: Date_) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Date';
    })(Date_.prototype);
}

export interface Time extends DataType<Type.Time> { TArray: Uint32Array; TValue: number; }
export class Time extends DataType<Type.Time> {
    constructor(public readonly unit: TimeUnit,
                public readonly bitWidth: TimeBitWidth) {
        super(Type.Time);
    }
    public toString() { return `Time${this.bitWidth}<${TimeUnit[this.unit]}>`; }
    protected static [Symbol.toStringTag] = ((proto: Time) => {
        (<any> proto).ArrayType = Uint32Array;
        return proto[Symbol.toStringTag] = 'Time';
    })(Time.prototype);
}

export interface Timestamp extends DataType<Type.Timestamp> { TArray: Int32Array; TValue: number; }
export class Timestamp extends DataType<Type.Timestamp> {
    constructor(public unit: TimeUnit, public timezone?: string | null) {
        super(Type.Timestamp);
    }
    public toString() { return `Timestamp<${TimeUnit[this.unit]}${this.timezone ? `, ${this.timezone}` : ``}>`; }
    protected static [Symbol.toStringTag] = ((proto: Timestamp) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Timestamp';
    })(Timestamp.prototype);
}

export interface Interval extends DataType<Type.Interval> { TArray: Int32Array; TValue: Int32Array; }
export class Interval extends DataType<Type.Interval> {
    constructor(public unit: IntervalUnit) {
        super(Type.Interval);
    }
    public toString() { return `Interval<${IntervalUnit[this.unit]}>`; }
    protected static [Symbol.toStringTag] = ((proto: Interval) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Interval';
    })(Interval.prototype);
}

export interface List<T extends DataType = any> extends DataType<Type.List>  { TArray: any; TValue: Vector<T>; }
export class List<T extends DataType = any> extends DataType<Type.List> {
    constructor(public children: Field[]) {
        super(Type.List, children);
    }
    public toString() { return `List<${this.valueType}>`; }
    public get ArrayType() { return this.valueType.ArrayType; }
    public get valueType() { return this.children[0].type as T; }
    public get valueField() { return this.children[0] as Field<T>; }
    protected static [Symbol.toStringTag] = ((proto: List) => {
        return proto[Symbol.toStringTag] = 'List';
    })(List.prototype);
}

export type StructData = {[name: string]: DataType}
export type StructValue<T extends StructData> = {
    [P in keyof T]: T[P]['TValue'];
}
export interface Struct<T extends StructData = StructData> extends DataType<Type.Struct> { TArray: any; TValue: StructValue<T> & View<any>; }
export class Struct<T extends StructData = StructData> extends DataType<Type.Struct> {
    constructor(public children: Field[]) {
        super(Type.Struct, children);
    }
    public toString() { return `Struct<${this.children.map((f) => f.type).join(`, `)}>`; }
    protected static [Symbol.toStringTag] = ((proto: Struct) => {
        return proto[Symbol.toStringTag] = 'Struct';
    })(Struct.prototype);
}

export interface Union<TType extends Type = any> extends DataType<TType> { TArray: Int8Array; TValue: any; }
export class Union<TType extends Type = any> extends DataType<TType> {
    constructor(public readonly mode: UnionMode,
                public readonly typeIds: ArrowType[],
                public readonly children: Field[]) {
        super(<TType> Type.Union, children);
    }
    public toString() { return `${this[Symbol.toStringTag]}<${
        this.children.map((x) => `${x.type}`).join(` | `)
    }>`; }
    protected static [Symbol.toStringTag] = ((proto: Union) => {
        (<any> proto).ArrayType = Int8Array;
        return proto[Symbol.toStringTag] = 'Union';
    })(Union.prototype);
}

export class DenseUnion extends Union<Type.DenseUnion> {
    constructor(typeIds: ArrowType[], children: Field[]) {
        super(UnionMode.Dense, typeIds, children);
    }
    protected static [Symbol.toStringTag] = ((proto: DenseUnion) => {
        return proto[Symbol.toStringTag] = 'DenseUnion';
    })(DenseUnion.prototype);
}

export class SparseUnion extends Union<Type.SparseUnion> {
    constructor(typeIds: ArrowType[], children: Field[]) {
        super(UnionMode.Sparse, typeIds, children);
    }
    protected static [Symbol.toStringTag] = ((proto: SparseUnion) => {
        return proto[Symbol.toStringTag] = 'SparseUnion';
    })(SparseUnion.prototype);
}

export interface FixedSizeBinary extends DataType<Type.FixedSizeBinary> { TArray: Uint8Array; TValue: Uint8Array; }
export class FixedSizeBinary extends DataType<Type.FixedSizeBinary> {
    constructor(public readonly byteWidth: number) {
        super(Type.FixedSizeBinary);
    }
    public toString() { return `FixedSizeBinary[${this.byteWidth}]`; }
    protected static [Symbol.toStringTag] = ((proto: FixedSizeBinary) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'FixedSizeBinary';
    })(FixedSizeBinary.prototype);
}

export interface FixedSizeList<T extends DataType = any> extends DataType<Type.FixedSizeList> { TArray: any; TValue: Vector<T>; }
export class FixedSizeList<T extends DataType = any> extends DataType<Type.FixedSizeList> {
    constructor(public readonly listSize: number,
                public readonly children: Field[]) {
        super(Type.FixedSizeList, children);
    }
    public get ArrayType() { return this.valueType.ArrayType; }
    public get valueType() { return this.children[0].type as T; }
    public get valueField() { return this.children[0] as Field<T>; }
    public toString() { return `FixedSizeList[${this.listSize}]<${this.valueType}>`; }
    protected static [Symbol.toStringTag] = ((proto: FixedSizeList) => {
        return proto[Symbol.toStringTag] = 'FixedSizeList';
    })(FixedSizeList.prototype);
}

/* tslint:disable:class-name */
export interface Map_ extends DataType<Type.Map> { TArray: Uint8Array; TValue: View<any>; }
export class Map_ extends DataType<Type.Map> {
    constructor(public readonly keysSorted: boolean,
                public readonly children: Field[]) {
        super(Type.Map, children);
    }
    public toString() { return `Map<${this.children.join(`, `)}>`; }
    protected static [Symbol.toStringTag] = ((proto: Map_) => {
        return proto[Symbol.toStringTag] = 'Map_';
    })(Map_.prototype);
}

export interface Dictionary<T extends DataType = any> extends DataType<Type.Dictionary> { TArray: T['TArray']; TValue: T['TValue']; }
export class Dictionary<T extends DataType> extends DataType<Type.Dictionary> {
    public readonly id: number;
    public readonly dictionary: T;
    public readonly indices: Int<any>;
    public readonly isOrdered: boolean;
    constructor(dictionary: T, indices: Int<any>, id?: Long | number | null, isOrdered?: boolean | null) {
        super(Type.Dictionary);
        this.indices = indices;
        this.dictionary = dictionary;
        this.isOrdered = isOrdered || false;
        this.id = id == null ? DictionaryBatch.getId() : typeof id === 'number' ? id : id.low;
    }
    public get ArrayType() { return this.dictionary.ArrayType; }
    public toString() { return `Dictionary<${this.indices}, ${this.dictionary}>`; }
    protected static [Symbol.toStringTag] = ((proto: Dictionary) => {
        return proto[Symbol.toStringTag] = 'Dictionary';
    })(Dictionary.prototype);
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
