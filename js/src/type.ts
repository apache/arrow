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

/* tslint:disable:class-name */

import { Field } from './schema';
import { Vector } from './vector';
import { flatbuffers } from 'flatbuffers';
import { ArrayBufferViewConstructor } from './interfaces';
import { Vector as VType, TypeToDataType } from './interfaces';
import { instance as comparer } from './visitor/typecomparator';

import Long = flatbuffers.Long;
import {
    Type,
    Precision, UnionMode,
    DateUnit, TimeUnit, IntervalUnit
} from './enum';

/** @ignore */
export type TimeBitWidth = 32 | 64;
/** @ignore */
export type IntBitWidth = 8 | 16 | 32 | 64;
/** @ignore */
export type IsSigned = { 'true': true; 'false': false };
/** @ignore */
export type RowLike<T extends { [key: string]: DataType }> =
      { readonly length: number }
    & ( Iterable<T[keyof T]['TValue']> )
    & { [P in keyof T]: T[P]['TValue'] }
    & { get<K extends keyof T>(key: K): T[K]['TValue']; }
    ;

export interface DataType<TType extends Type = Type> {
    readonly TType: TType;
    readonly TArray: any;
    readonly TValue: any;
    readonly ArrayType: any;
}

export class DataType<TType extends Type = Type, TChildren extends { [key: string]: DataType } = any> {

    // @ts-ignore
    public [Symbol.toStringTag]: string;

    /** @nocollapse */ static            isNull (x: any): x is Null            { return x && x.typeId === Type.Null;            }
    /** @nocollapse */ static             isInt (x: any): x is Int_            { return x && x.typeId === Type.Int;             }
    /** @nocollapse */ static           isFloat (x: any): x is Float           { return x && x.typeId === Type.Float;           }
    /** @nocollapse */ static          isBinary (x: any): x is Binary          { return x && x.typeId === Type.Binary;          }
    /** @nocollapse */ static            isUtf8 (x: any): x is Utf8            { return x && x.typeId === Type.Utf8;            }
    /** @nocollapse */ static            isBool (x: any): x is Bool            { return x && x.typeId === Type.Bool;            }
    /** @nocollapse */ static         isDecimal (x: any): x is Decimal         { return x && x.typeId === Type.Decimal;         }
    /** @nocollapse */ static            isDate (x: any): x is Date_           { return x && x.typeId === Type.Date;            }
    /** @nocollapse */ static            isTime (x: any): x is Time_           { return x && x.typeId === Type.Time;            }
    /** @nocollapse */ static       isTimestamp (x: any): x is Timestamp_      { return x && x.typeId === Type.Timestamp;       }
    /** @nocollapse */ static        isInterval (x: any): x is Interval_       { return x && x.typeId === Type.Interval;        }
    /** @nocollapse */ static            isList (x: any): x is List            { return x && x.typeId === Type.List;            }
    /** @nocollapse */ static          isStruct (x: any): x is Struct          { return x && x.typeId === Type.Struct;          }
    /** @nocollapse */ static           isUnion (x: any): x is Union_          { return x && x.typeId === Type.Union;           }
    /** @nocollapse */ static isFixedSizeBinary (x: any): x is FixedSizeBinary { return x && x.typeId === Type.FixedSizeBinary; }
    /** @nocollapse */ static   isFixedSizeList (x: any): x is FixedSizeList   { return x && x.typeId === Type.FixedSizeList;   }
    /** @nocollapse */ static             isMap (x: any): x is Map_            { return x && x.typeId === Type.Map;             }
    /** @nocollapse */ static      isDictionary (x: any): x is Dictionary      { return x && x.typeId === Type.Dictionary;      }

    public get children() { return this._children; }
    public get typeId(): TType { return <any> Type.NONE; }

    constructor(protected _children?: Field<TChildren[keyof TChildren]>[]) {}

    public compareTo(other: DataType): other is TypeToDataType<TType> {
        return comparer.visit(this, other);
    }

    protected static [Symbol.toStringTag] = ((proto: DataType) => {
        (<any> proto).ArrayType = Array;
        return proto[Symbol.toStringTag] = 'DataType';
    })(DataType.prototype);
}

export interface Null extends DataType<Type.Null> { TArray: void; TValue: null; }
export class Null extends DataType<Type.Null> {
    public toString() { return `Null`; }
    public get typeId() { return Type.Null as Type.Null; }
    protected static [Symbol.toStringTag] = ((proto: Null) => {
        return proto[Symbol.toStringTag] = 'Null';
    })(Null.prototype);
}

/** @ignore */
type Ints = Type.Int | Type.Int8 | Type.Int16 | Type.Int32 | Type.Int64 | Type.Uint8 | Type.Uint16 | Type.Uint32 | Type.Uint64;
/** @ignore */
type IType = {
    [Type.Int   ]: { bitWidth: IntBitWidth; isSigned: true | false; TArray: IntArray;    TValue: number | Int32Array | Uint32Array; };
    [Type.Int8  ]: { bitWidth:           8; isSigned: true;         TArray: Int8Array;   TValue: number;            };
    [Type.Int16 ]: { bitWidth:          16; isSigned: true;         TArray: Int16Array;  TValue: number;            };
    [Type.Int32 ]: { bitWidth:          32; isSigned: true;         TArray: Int32Array;  TValue: number;            };
    [Type.Int64 ]: { bitWidth:          64; isSigned: true;         TArray: Int32Array;  TValue: Int32Array;        };
    [Type.Uint8 ]: { bitWidth:           8; isSigned: false;        TArray: Uint8Array;  TValue: number;            };
    [Type.Uint16]: { bitWidth:          16; isSigned: false;        TArray: Uint16Array; TValue: number;            };
    [Type.Uint32]: { bitWidth:          32; isSigned: false;        TArray: Uint32Array; TValue: number;            };
    [Type.Uint64]: { bitWidth:          64; isSigned: false;        TArray: Uint32Array; TValue: Uint32Array;       };
};

interface Int_<T extends Ints = Ints> extends DataType<T> { TArray: IType[T]['TArray']; TValue: IType[T]['TValue']; }
class Int_<T extends Ints = Ints> extends DataType<T> {
    constructor(protected _isSigned: IType[T]['isSigned'],
                protected _bitWidth: IType[T]['bitWidth']) {
        super();
    }
    public get typeId() { return Type.Int as T; }
    public get isSigned() { return this._isSigned; }
    public get bitWidth() { return this._bitWidth; }
    public get ArrayType(): ArrayBufferViewConstructor<IType[T]['TArray']> {
        switch (this._bitWidth) {
            case  8: return (this._isSigned ?  Int8Array :  Uint8Array) as any;
            case 16: return (this._isSigned ? Int16Array : Uint16Array) as any;
            case 32: return (this._isSigned ? Int32Array : Uint32Array) as any;
            case 64: return (this._isSigned ? Int32Array : Uint32Array) as any;
        }
        throw new Error(`Unrecognized ${this[Symbol.toStringTag]} type`);
    }
    public toString() { return `${this._isSigned ? `I` : `Ui`}nt${this._bitWidth}`; }
    protected static [Symbol.toStringTag] = ((proto: Int_) => {
        return proto[Symbol.toStringTag] = 'Int';
    })(Int_.prototype);
}

export { Int_ as Int };

export class Int8 extends Int_<Type.Int8> { constructor() { super(true, 8); } }
export class Int16 extends Int_<Type.Int16> { constructor() { super(true, 16); } }
export class Int32 extends Int_<Type.Int32> { constructor() { super(true, 32); } }
export class Int64 extends Int_<Type.Int64> { constructor() { super(true, 64); } }
export class Uint8 extends Int_<Type.Uint8> { constructor() { super(false, 8); } }
export class Uint16 extends Int_<Type.Uint16> { constructor() { super(false, 16); } }
export class Uint32 extends Int_<Type.Uint32> { constructor() { super(false, 32); } }
export class Uint64 extends Int_<Type.Uint64> { constructor() { super(false, 64); } }

/** @ignore */
type Floats = Type.Float | Type.Float16 | Type.Float32 | Type.Float64;
/** @ignore */
type FType = {
    [Type.Float  ]: { precision: Precision;        TArray: FloatArray;    TValue: number; };
    [Type.Float16]: { precision: Precision.HALF;   TArray: Uint16Array;   TValue: number; };
    [Type.Float32]: { precision: Precision.SINGLE; TArray: Float32Array;  TValue: number; };
    [Type.Float64]: { precision: Precision.DOUBLE; TArray: Float64Array;  TValue: number; };
};

export interface Float<T extends Floats = Floats> extends DataType<T> { TArray: FType[T]['TArray']; TValue: number; }
export class Float<T extends Floats = Floats> extends DataType<T> {
    constructor(protected _precision: Precision) {
        super();
    }
    public get typeId() { return Type.Float as T; }
    public get precision() { return this._precision; }
    public get ArrayType(): ArrayBufferViewConstructor<FType[T]['TArray']> {
        switch (this.precision) {
            case Precision.HALF: return Uint16Array;
            case Precision.SINGLE: return Float32Array;
            case Precision.DOUBLE: return Float64Array;
        }
        throw new Error(`Unrecognized ${this[Symbol.toStringTag]} type`);
    }
    public toString() { return `Float${(this.precision << 5) || 16}`; }
    protected static [Symbol.toStringTag] = ((proto: Float) => {
        return proto[Symbol.toStringTag] = 'Float';
    })(Float.prototype);
}

export class Float16 extends Float<Type.Float16> { constructor() { super(Precision.HALF); } }
export class Float32 extends Float<Type.Float32> { constructor() { super(Precision.SINGLE); } }
export class Float64 extends Float<Type.Float64> { constructor() { super(Precision.DOUBLE); } }

export interface Binary extends DataType<Type.Binary> { TArray: Uint8Array; TValue: Uint8Array; }
export class Binary extends DataType<Type.Binary> {
    constructor() {
        super();
    }
    public get typeId() { return Type.Binary as Type.Binary; }
    public toString() { return `Binary`; }
    protected static [Symbol.toStringTag] = ((proto: Binary) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'Binary';
    })(Binary.prototype);
}

export interface Utf8 extends DataType<Type.Utf8> { TArray: Uint8Array; TValue: string; ArrayType: typeof Uint8Array; }
export class Utf8 extends DataType<Type.Utf8> {
    constructor() {
        super();
    }
    public get typeId() { return Type.Utf8 as Type.Utf8; }
    public toString() { return `Utf8`; }
    protected static [Symbol.toStringTag] = ((proto: Utf8) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'Utf8';
    })(Utf8.prototype);
}

export interface Bool extends DataType<Type.Bool> { TArray: Uint8Array; TValue: boolean; ArrayType: typeof Uint8Array; }
export class Bool extends DataType<Type.Bool> {
    constructor() {
        super();
    }
    public get typeId() { return Type.Bool as Type.Bool; }
    public toString() { return `Bool`; }
    protected static [Symbol.toStringTag] = ((proto: Bool) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'Bool';
    })(Bool.prototype);
}

export interface Decimal extends DataType<Type.Decimal> { TArray: Uint32Array; TValue: Uint32Array; ArrayType: typeof Uint32Array; }
export class Decimal extends DataType<Type.Decimal> {
    constructor(protected _scale: number,
                protected _precision: number) {
        super();
    }
    public get typeId() { return Type.Decimal as Type.Decimal; }
    public get scale() { return this._scale; }
    public get precision() { return this._precision; }
    public toString() { return `Decimal[${this._precision}e${this._scale > 0 ? `+` : ``}${this._scale}]`; }
    protected static [Symbol.toStringTag] = ((proto: Decimal) => {
        (<any> proto).ArrayType = Uint32Array;
        return proto[Symbol.toStringTag] = 'Decimal';
    })(Decimal.prototype);
}

/** @ignore */
export type Dates = Type.Date | Type.DateDay | Type.DateMillisecond;
export interface Date_<T extends Dates = Dates> extends DataType<T> { TArray: Int32Array; TValue: Date; ArrayType: typeof Int32Array; }
export class Date_<T extends Dates = Dates> extends DataType<T> {
    constructor(protected _unit: DateUnit) {
        super();
    }
    public get typeId() { return Type.Date as T; }
    public get unit() { return this._unit; }
    public toString() { return `Date${(this._unit + 1) * 32}<${DateUnit[this._unit]}>`; }
    protected static [Symbol.toStringTag] = ((proto: Date_) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Date';
    })(Date_.prototype);
}

export class DateDay extends Date_<Type.DateDay> { constructor() { super(DateUnit.DAY); } }
export class DateMillisecond extends Date_<Type.DateMillisecond> { constructor() { super(DateUnit.MILLISECOND); } }

/** @ignore */
type Times = Type.Time | Type.TimeSecond | Type.TimeMillisecond | Type.TimeMicrosecond | Type.TimeNanosecond;
/** @ignore */
type TimesType = {
    [Type.Time           ]: { unit: TimeUnit;             TValue: number | Int32Array };
    [Type.TimeSecond     ]: { unit: TimeUnit.SECOND;      TValue: number;             };
    [Type.TimeMillisecond]: { unit: TimeUnit.MILLISECOND; TValue: number;             };
    [Type.TimeMicrosecond]: { unit: TimeUnit.MICROSECOND; TValue: Int32Array;         };
    [Type.TimeNanosecond ]: { unit: TimeUnit.NANOSECOND;  TValue: Int32Array;         };
};

interface Time_<T extends Times = Times> extends DataType<T> { TArray: Int32Array; TValue: TimesType[T]['TValue']; ArrayType: typeof Int32Array; }
class Time_<T extends Times = Times> extends DataType<T> {
    constructor(protected _unit: TimesType[T]['unit'],
                protected _bitWidth: TimeBitWidth) {
        super();
    }
    public get typeId() { return Type.Time as T; }
    public get unit() { return this._unit; }
    public get bitWidth() { return this._bitWidth; }
    public toString() { return `Time${this._bitWidth}<${TimeUnit[this._unit]}>`; }
    protected static [Symbol.toStringTag] = ((proto: Time_) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Time';
    })(Time_.prototype);
}

export { Time_ as Time };

export class TimeSecond extends Time_<Type.TimeSecond> { constructor() { super(TimeUnit.SECOND, 32); } }
export class TimeMillisecond extends Time_<Type.TimeMillisecond> { constructor() { super(TimeUnit.MILLISECOND, 32); } }
export class TimeMicrosecond extends Time_<Type.TimeMicrosecond> { constructor() { super(TimeUnit.MICROSECOND, 64); } }
export class TimeNanosecond extends Time_<Type.TimeNanosecond> { constructor() { super(TimeUnit.NANOSECOND, 64); } }

/** @ignore */
type Timestamps = Type.Timestamp | Type.TimestampSecond | Type.TimestampMillisecond | Type.TimestampMicrosecond | Type.TimestampNanosecond;
interface Timestamp_<T extends Timestamps = Timestamps> extends DataType<T> { TArray: Int32Array; TValue: number; ArrayType: typeof Int32Array; }
class Timestamp_<T extends Timestamps = Timestamps> extends DataType<T> {
    constructor(protected _unit: TimeUnit, protected _timezone?: string | null) {
        super();
    }
    public get typeId() { return Type.Timestamp as T; }
    public get unit() { return this._unit; }
    public get timezone() { return this._timezone; }
    public toString() { return `Timestamp<${TimeUnit[this._unit]}${this._timezone ? `, ${this._timezone}` : ``}>`; }
    protected static [Symbol.toStringTag] = ((proto: Timestamp_) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Timestamp';
    })(Timestamp_.prototype);
}

export { Timestamp_ as Timestamp };

export class TimestampSecond extends Timestamp_<Type.TimestampSecond> { constructor(timezone?: string | null) { super(TimeUnit.SECOND, timezone); } }
export class TimestampMillisecond extends Timestamp_<Type.TimestampMillisecond> { constructor(timezone?: string | null) { super(TimeUnit.MILLISECOND, timezone); } }
export class TimestampMicrosecond extends Timestamp_<Type.TimestampMicrosecond> { constructor(timezone?: string | null) { super(TimeUnit.MICROSECOND, timezone); } }
export class TimestampNanosecond extends Timestamp_<Type.TimestampNanosecond> { constructor(timezone?: string | null) { super(TimeUnit.NANOSECOND, timezone); } }

/** @ignore */
type Intervals = Type.Interval | Type.IntervalDayTime | Type.IntervalYearMonth;
interface Interval_<T extends Intervals = Intervals> extends DataType<T> { TArray: Int32Array; TValue: Int32Array; ArrayType: typeof Int32Array; }
class Interval_<T extends Intervals = Intervals> extends DataType<T> {
    constructor(protected _unit: IntervalUnit) {
        super();
    }
    public get typeId() { return Type.Interval as T; }
    public get unit() { return this._unit; }
    public toString() { return `Interval<${IntervalUnit[this._unit]}>`; }
    protected static [Symbol.toStringTag] = ((proto: Interval_) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Interval';
    })(Interval_.prototype);
}

export { Interval_ as Interval };

export class IntervalDayTime extends Interval_<Type.IntervalDayTime> { constructor() { super(IntervalUnit.DAY_TIME); } }
export class IntervalYearMonth extends Interval_<Type.IntervalYearMonth> { constructor() { super(IntervalUnit.YEAR_MONTH); } }

export interface List<T extends DataType = any> extends DataType<Type.List, { [0]: T }>  { TArray: IterableArrayLike<T>; TValue: VType<T>; }
export class List<T extends DataType = any> extends DataType<Type.List, { [0]: T }> {
    constructor(child: Field<T>) {
        super([child]);
    }
    public get typeId() { return Type.List as Type.List; }
    // @ts-ignore
    protected _children: Field<T>[];
    public toString() { return `List<${this.valueType}>`; }
    public get children() { return this._children; }
    public get valueType(): T { return this._children[0].type as T; }
    public get valueField(): Field<T> { return this._children[0] as Field<T>; }
    public get ArrayType(): T['ArrayType'] { return this.valueType.ArrayType; }
    protected static [Symbol.toStringTag] = ((proto: List) => {
        return proto[Symbol.toStringTag] = 'List';
    })(List.prototype);
}

export interface Struct<T extends { [key: string]: DataType } = any> extends DataType<Type.Struct> { TArray: IterableArrayLike<RowLike<T>>; TValue: RowLike<T>; dataTypes: T; }
export class Struct<T extends { [key: string]: DataType } = any> extends DataType<Type.Struct, T> {
    constructor(protected _children: Field<T[keyof T]>[]) {
        super(_children);
    }
    public get typeId() { return Type.Struct as Type.Struct; }
    public get children() { return this._children; }
    public toString() { return `Struct<[${this._children.map((f) => f.type).join(`, `)}]>`; }
    protected static [Symbol.toStringTag] = ((proto: Struct) => {
        return proto[Symbol.toStringTag] = 'Struct';
    })(Struct.prototype);
}

/** @ignore */
type Unions = Type.Union | Type.DenseUnion | Type.SparseUnion;
interface Union_<T extends Unions = Unions> extends DataType<T> { TArray: Int32Array; TValue: any[]; }
class Union_<T extends Unions = Unions> extends DataType<T> {
    protected _typeIds: Int32Array;
    protected _children: Field<any>[];
    protected _typeIdToChildIndex: { [key: number]: number };
    constructor(protected _mode: UnionMode, _typeIds: number[] | Int32Array, _children: Field<any>[]) {
        super(_children);
        this._children = _children;
        this._typeIds = _typeIds = Int32Array.from(_typeIds);
        this._typeIdToChildIndex = _typeIds.reduce((typeIdToChildIndex, typeId, idx) => {
            return (typeIdToChildIndex[typeId] = idx) && typeIdToChildIndex || typeIdToChildIndex;
        }, Object.create(null) as { [key: number]: number });
    }
    public get typeId() { return Type.Union as T; }
    public get mode() { return this._mode; }
    public get typeIds() { return this._typeIds; }
    public get children() { return this._children; }
    public get typeIdToChildIndex() { return this._typeIdToChildIndex; }
    public toString() { return `${this[Symbol.toStringTag]}<${
        this._children.map((x) => `${x.type}`).join(` | `)
    }>`; }
    protected static [Symbol.toStringTag] = ((proto: Union_) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Union';
    })(Union_.prototype);
}

export { Union_ as Union };

export class DenseUnion extends Union_<Type.DenseUnion> {
    constructor(typeIds: number[] | Int32Array, children: Field[]) {
        super(UnionMode.Dense, typeIds, children);
    }
}

export class SparseUnion extends Union_<Type.SparseUnion> {
    constructor(typeIds: number[] | Int32Array, children: Field[]) {
        super(UnionMode.Sparse, typeIds, children);
    }
}

export interface FixedSizeBinary extends DataType<Type.FixedSizeBinary> { TArray: Uint8Array; TValue: Uint8Array; ArrayType: typeof Uint8Array; }
export class FixedSizeBinary extends DataType<Type.FixedSizeBinary> {
    constructor(protected _byteWidth: number) {
        super();
    }
    public get typeId() { return Type.FixedSizeBinary as Type.FixedSizeBinary; }
    public get byteWidth() { return this._byteWidth; }
    public toString() { return `FixedSizeBinary[${this.byteWidth}]`; }
    protected static [Symbol.toStringTag] = ((proto: FixedSizeBinary) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'FixedSizeBinary';
    })(FixedSizeBinary.prototype);
}

export interface FixedSizeList<T extends DataType = any> extends DataType<Type.FixedSizeList> { TArray: IterableArrayLike<T['TArray']>; TValue: VType<T>; }
export class FixedSizeList<T extends DataType = any> extends DataType<Type.FixedSizeList, { [0]: T }> {
    constructor(protected _listSize: number, child: Field<T>) {
        super([child]);
    }
    public get typeId() { return Type.FixedSizeList as Type.FixedSizeList; }
    // @ts-ignore
    protected _children: Field<T>[];
    public get listSize() { return this._listSize; }
    public get children() { return this._children; }
    public get valueType(): T { return this.children[0].type as T; }
    public get valueField(): Field<T> { return this.children[0] as Field<T>; }
    public get ArrayType(): T['ArrayType'] { return this.valueType.ArrayType; }
    public toString() { return `FixedSizeList[${this._listSize}]<${this.valueType}>`; }
    protected static [Symbol.toStringTag] = ((proto: FixedSizeList) => {
        return proto[Symbol.toStringTag] = 'FixedSizeList';
    })(FixedSizeList.prototype);
}

export interface Map_<T extends { [key: string]: DataType } = any> extends DataType<Type.Map> { TArray: Uint8Array; TValue: RowLike<T>; dataTypes: T; }
export class Map_<T extends { [key: string]: DataType } = any> extends DataType<Type.Map, T> {
    constructor(protected _children: Field<T[keyof T]>[],
                protected _keysSorted: boolean = false) {
        super(_children);
    }
    public get typeId() { return Type.Map as Type.Map; }
    public get children() { return this._children; }
    public get keysSorted() { return this._keysSorted; }
    public toString() { return `Map<{${this._children.map((f) => `${f.name}:${f.type}`).join(`, `)}}>`; }
    protected static [Symbol.toStringTag] = ((proto: Map_) => {
        return proto[Symbol.toStringTag] = 'Map_';
    })(Map_.prototype);
}

/** @ignore */
const getId = ((atomicDictionaryId) => () => ++atomicDictionaryId)(-1);

/** @ignore */
export type TKeys = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32;

export interface Dictionary<T extends DataType = any, TKey extends TKeys = TKeys> extends DataType<Type.Dictionary> { TArray: TKey['TArray']; TValue: T['TValue']; }
export class Dictionary<T extends DataType = any, TKey extends TKeys = TKeys> extends DataType<Type.Dictionary> {
    protected _id: number;
    protected _indices: TKey;
    protected _dictionary: T;
    protected _isOrdered: boolean;
    protected _dictionaryVector: Vector<T>;
    public set dictionaryVector(v) { this._dictionaryVector = v; }
    public get dictionaryVector() { return this._dictionaryVector; }
    constructor(dictionary: T, indices: TKey, id?: Long | number | null, isOrdered?: boolean | null, dictionaryVector?: Vector<T>) {
        super();
        this._indices = indices;
        this._dictionary = dictionary;
        this._isOrdered = isOrdered || false;
        this._dictionaryVector = dictionaryVector!;
        this._id = id == null ? getId() : typeof id === 'number' ? id : id.low;
    }
    public get typeId() { return Type.Dictionary as Type.Dictionary; }
    public get id() { return this._id; }
    public get indices() { return this._indices; }
    public get dictionary() { return this._dictionary; }
    public get isOrdered() { return this._isOrdered; }
    public set children(_: T['children']) {}
    public get children() { return this.dictionary.children; }
    public get valueType(): T { return this.dictionary as T; }
    public get ArrayType(): T['ArrayType'] { return this.dictionary.ArrayType; }
    public toString() { return `Dictionary<${this.indices}, ${this.dictionary}>`; }
    protected static [Symbol.toStringTag] = ((proto: Dictionary) => {
        return proto[Symbol.toStringTag] = 'Dictionary';
    })(Dictionary.prototype);
}

/** @ignore */
export interface IterableArrayLike<T = any> extends ArrayLike<T>, Iterable<T> {}
/** @ignore */
export type FloatArray = Uint16Array | Float32Array | Float64Array;
/** @ignore */
export type IntArray = Int8Array | Int16Array | Int32Array | Uint8Array | Uint16Array | Uint32Array;
