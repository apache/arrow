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

import { VectorLike } from './vector';
import { VectorType, TypedArray, TypedArrayConstructor } from './type';
import { DataType, FlatType, ListType, NestedType, Map_, DenseUnion, SparseUnion } from './type';

export type Data<T extends DataType> = DataTypes<T>[T['TType']] & BaseData<T>;
export interface DataTypes<T extends DataType> {
/*                [Type.NONE]*/  0: BaseData<T>;
/*                [Type.Null]*/  1: FlatData<T>;
/*                 [Type.Int]*/  2: FlatData<T>;
/*               [Type.Float]*/  3: FlatData<T>;
/*              [Type.Binary]*/  4: ListData<T>;
/*                [Type.Utf8]*/  5: ListData<T>;
/*                [Type.Bool]*/  6: FlatData<T>;
/*             [Type.Decimal]*/  7: FlatData<T>;
/*                [Type.Date]*/  8: FlatData<T>;
/*                [Type.Time]*/  9: FlatData<T>;
/*           [Type.Timestamp]*/ 10: FlatData<T>;
/*            [Type.Interval]*/ 11: FlatData<T>;
/*                [Type.List]*/ 12: ListData<T>;
/*              [Type.Struct]*/ 13: FlatData<T>;
/*               [Type.Union]*/ 14: UnionData;
/*     [Type.FixedSizeBinary]*/ 15: FlatData<T>;
/*       [Type.FixedSizeList]*/ 16: ListData<T>;
/*                 [Type.Map]*/ 17: NestedData<Map_>;
/*  [Type.DenseUnion]*/ DenseUnion: DenseUnionData;
/*[Type.SparseUnion]*/ SparseUnion: SparseUnionData;
}
// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Array::null_count is called the
// first time, the null count will be computed. See ARROW-33
export type kUnknownNullCount = -1;
export const kUnknownNullCount = -1;

export class BaseData<T extends DataType = DataType> implements VectorLike {
    protected _type: T;
    protected _length: number;
    // @ts-ignore
    protected _childData: BaseData[];
    protected _nullCount: number | kUnknownNullCount;
    protected /*  [VectorType.OFFSET]:*/ 0?: Int32Array;
    protected /*    [VectorType.DATA]:*/ 1?: T['TArray'];
    protected /*[VectorType.VALIDITY]:*/ 2?: Uint8Array;
    protected /*    [VectorType.TYPE]:*/ 3?: Int8Array;
    constructor(type: T, length: number, nullCount?: number) {
        this._type = type;
        this._length = Math.max(length || 0, 0);
        this._nullCount = Math.max(nullCount || 0, -1);
    }
    public get type() { return this._type; }
    public get length() { return this._length; }
    public get typeId() { return this._type.TType; }
    public get childData() { return this._childData; }
    public get nullCount() { return this._nullCount; }
    public get nullBitmap() { return this[VectorType.VALIDITY]; }
    public clone(length = this._length, nullCount = this._nullCount) {
        return new BaseData<T>(this._type, length, nullCount) as this;
    }
    public slice(offset: number, length: number) {
        return length <= 0 ? this : this.sliceInternal(this.clone(
            length, +(this._nullCount === 0) - 1
        ), offset, length);
    }
    protected sliceInternal(clone: this, offset: number, length: number) {
        let arr: any;
        // If typeIds exist, slice the typeIds buffer
        (arr = this[VectorType.TYPE]) && (clone[VectorType.TYPE] = this.sliceData(arr, offset, length));
        // If a null bitmap exists, slice the null bitmap
        (arr = this[VectorType.VALIDITY]) && (clone[VectorType.VALIDITY] = this.sliceNullBitmap(arr, offset, length));
        // If offsets exist, only slice the offsets buffer
        (arr = this[VectorType.OFFSET]) && (clone[VectorType.OFFSET] = this.sliceOffsets(arr, offset, length)) ||
            // Otherwise if no offsets, slice the data buffer
            (arr = this[VectorType.DATA]) && (clone[VectorType.DATA] = this.sliceData(arr, offset, length));
        return clone;
    }
    protected sliceData(data: T['TArray'] & TypedArray, offset: number, length: number) {
        return data.subarray(offset, offset + length);
    }
    protected sliceOffsets(valueOffsets: Int32Array, offset: number, length: number) {
        return valueOffsets.subarray(offset, offset + length + 1);
    }
    protected sliceNullBitmap(nullBitmap: Uint8Array, offset: number, length: number) {
        return length >= 8
            ? nullBitmap.subarray(offset >> 3, ((offset + length) >> 3))
            : nullBitmap.subarray(offset >> 3, ((offset + length) >> 3) + 1);
    }
}

export class FlatData<T extends FlatType> extends BaseData<T> {
    public /*    [VectorType.DATA]:*/ 1: T['TArray'];
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    public get values() { return this[VectorType.DATA]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array, data: Iterable<number>, nullCount?: number) {
        super(type, length, nullCount);
        this[VectorType.DATA] = toTypedArray(this.ArrayType, data);
        this[VectorType.VALIDITY] = toTypedArray(Uint8Array, nullBitmap);
    }
    public get ArrayType(): T['ArrayType'] { return this._type.ArrayType; }
    public clone(length = this._length, nullCount = this._nullCount) {
        return new FlatData<T>(
            this._type, length, this[VectorType.VALIDITY],
            this[VectorType.DATA], nullCount
        ) as this;
    }
}

export class ListData<T extends ListType> extends BaseData<T> {
    public /*  [VectorType.OFFSET]:*/ 0: Int32Array;
    public /*    [VectorType.DATA]:*/ 1: T['TArray'];
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    public get values() { return this[VectorType.DATA]; }
    public get valueOffsets() { return this[VectorType.OFFSET]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array, data: T['TArray'], valueOffsets: Iterable<number>, nullCount?: number) {
        super(type, length, nullCount);
        this[VectorType.DATA] = data;
        this[VectorType.OFFSET] = toTypedArray(Int32Array, valueOffsets);
        this[VectorType.VALIDITY] = toTypedArray(Uint8Array, nullBitmap);
    }
    public clone(length = this._length, nullCount = this._nullCount) {
        return new ListData<T>(
            this._type, length, this[VectorType.VALIDITY],
            this[VectorType.DATA], this[VectorType.OFFSET],
            nullCount
        ) as this;
    }
}

export class NestedData<T extends NestedType = NestedType> extends BaseData<T> {
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    constructor(type: T, length: number, nullBitmap: Uint8Array, childData: BaseData[], nullCount?: number) {
        super(type, length, nullCount);
        this._childData = childData;
        this[VectorType.VALIDITY] = nullBitmap;
    }
    public clone(length = this._length, nullCount = this._nullCount) {
        return new NestedData<T>(
            this._type, length,
            this[VectorType.VALIDITY],
            this._childData, nullCount
        ) as this;
    }
    protected sliceInternal(clone: this, offset: number, length: number) {
        if (!this[VectorType.OFFSET]) {
            clone._childData = this._childData.map((child) => child.slice(offset, length));
        }
        return super.sliceInternal(clone, offset, length);
    }
}

export class UnionData<T extends (DenseUnion | SparseUnion) = any> extends NestedData<T> {
    public /*    [VectorType.TYPE]:*/ 3: T['TArray'];
    public get typeIds() { return this[VectorType.TYPE]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array, typeIds: Iterable<number>, childData: BaseData[], nullCount?: number) {
        super(type, length, nullBitmap, childData, nullCount);
        this[VectorType.TYPE] = toTypedArray(Int8Array, typeIds);
    }
    public clone(length = this._length, nullCount = this._nullCount) {
        return new UnionData<T>(
            this._type, length, this[VectorType.VALIDITY],
            this[VectorType.TYPE], this._childData, nullCount
        ) as this;
    }
}

export class DenseUnionData extends UnionData<DenseUnion> {
    public /*  [VectorType.OFFSET]:*/ 0: Int32Array;
    public get valueOffsets() { return this[VectorType.OFFSET]; }
    constructor(type: DenseUnion, length: number, nullBitmap: Uint8Array, typeIds: Iterable<number>, valueOffsets: Iterable<number>, childData: BaseData[], nullCount?: number) {
        super(type, length, nullBitmap, typeIds, childData, nullCount);
        this[VectorType.OFFSET] = toTypedArray(Int32Array, valueOffsets);
    }
    public clone(length = this._length, nullCount = this._nullCount) {
        return new DenseUnionData(
            this._type, length, this[VectorType.VALIDITY],
            this[VectorType.TYPE], this[VectorType.OFFSET],
            this._childData, nullCount
        ) as this;
    }
}

export class SparseUnionData extends UnionData<SparseUnion> {
    constructor(type: SparseUnion, length: number, nullBitmap: Uint8Array, typeIds: Iterable<number>, childData: BaseData[], nullCount?: number) {
        super(type, length, nullBitmap, typeIds, childData, nullCount);
    }
    public clone(length = this._length, nullCount = this._nullCount) {
        return new SparseUnionData(
            this._type, length, this[VectorType.VALIDITY],
            this[VectorType.TYPE], this._childData, nullCount
        ) as this;
    }
}

function toTypedArray<T extends TypedArray>(ArrayType: TypedArrayConstructor<T>, values?: T | ArrayLike<number> | Iterable<number> | null): T {
    return values instanceof ArrayType ? values
         : !values || !ArrayBuffer.isView(values) ? ArrayType.from(values || [])
         : new ArrayType(values.buffer, values.byteOffset, values.byteLength / ArrayType.BYTES_PER_ELEMENT);
}
