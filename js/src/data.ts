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

import { popcnt_bit_range } from './util/bit';
import { VectorLike, Vector } from './vector';
import { Int, Bool, FlatListType, List, Struct, Map_ } from './type';
import { VectorType, TypedArray, TypedArrayConstructor, Dictionary } from './type';
import { DataType, FlatType, ListType, NestedType, SingleNestedType, DenseUnion, SparseUnion } from './type';

export function toTypedArray<T extends TypedArray>(ArrayType: TypedArrayConstructor<T>, values?: T | ArrayLike<number> | Iterable<number> | null): T {
    if (!ArrayType && ArrayBuffer.isView(values)) { return values; }
    return values instanceof ArrayType ? values
         : !values || !ArrayBuffer.isView(values) ? ArrayType.from(values || [])
         : new ArrayType(values.buffer, values.byteOffset, values.byteLength / ArrayType.BYTES_PER_ELEMENT);
}

export type Data<T extends DataType> = DataTypes<T>[T['TType']] & BaseData<T>;
export interface DataTypes<T extends DataType> {
/*                [Type.NONE]*/  0: BaseData<T>;
/*                [Type.Null]*/  1: FlatData<T>;
/*                 [Type.Int]*/  2: FlatData<T>;
/*               [Type.Float]*/  3: FlatData<T>;
/*              [Type.Binary]*/  4: FlatListData<T>;
/*                [Type.Utf8]*/  5: FlatListData<T>;
/*                [Type.Bool]*/  6: BoolData;
/*             [Type.Decimal]*/  7: FlatData<T>;
/*                [Type.Date]*/  8: FlatData<T>;
/*                [Type.Time]*/  9: FlatData<T>;
/*           [Type.Timestamp]*/ 10: FlatData<T>;
/*            [Type.Interval]*/ 11: FlatData<T>;
/*                [Type.List]*/ 12: ListData<List<T>>;
/*              [Type.Struct]*/ 13: NestedData<Struct>;
/*               [Type.Union]*/ 14: UnionData;
/*     [Type.FixedSizeBinary]*/ 15: FlatData<T>;
/*       [Type.FixedSizeList]*/ 16: SingleNestedData<any>;
/*                 [Type.Map]*/ 17: NestedData<Map_>;
/*  [Type.DenseUnion]*/ DenseUnion: DenseUnionData;
/*[Type.SparseUnion]*/ SparseUnion: SparseUnionData;
/*[  Type.Dictionary]*/ Dictionary: DictionaryData<any>;
}
// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Array::null_count is called the
// first time, the null count will be computed. See ARROW-33
export type kUnknownNullCount = -1;
export const kUnknownNullCount = -1;

export class BaseData<T extends DataType = DataType> implements VectorLike {
    public type: T;
    public length: number;
    public offset: number;
    // @ts-ignore
    public childData: Data<any>[];
    protected _nullCount: number | kUnknownNullCount;
    protected /*  [VectorType.OFFSET]:*/ 0?: Int32Array;
    protected /*    [VectorType.DATA]:*/ 1?: T['TArray'];
    protected /*[VectorType.VALIDITY]:*/ 2?: Uint8Array;
    protected /*    [VectorType.TYPE]:*/ 3?: Int8Array;
    constructor(type: T, length: number, offset?: number, nullCount?: number) {
        this.type = type;
        this.length = Math.floor(Math.max(length || 0, 0));
        this.offset = Math.floor(Math.max(offset || 0, 0));
        this._nullCount = Math.floor(Math.max(nullCount || 0, -1));
    }
    public get typeId() { return this.type.TType; }
    public get nullBitmap() { return this[VectorType.VALIDITY]; }
    public get nullCount() {
        let nullCount = this._nullCount;
        let nullBitmap: Uint8Array | undefined;
        if (nullCount === -1 && (nullBitmap = this[VectorType.VALIDITY])) {
            this._nullCount = nullCount = this.length - popcnt_bit_range(nullBitmap, this.offset, this.offset + this.length);
        }
        return nullCount;
    }
    public clone<R extends T>(type: R, length = this.length, offset = this.offset, nullCount = this._nullCount): Data<R> {
        return new BaseData(type, length, offset, nullCount) as any;
    }
    public slice(offset: number, length: number) {
        return length <= 0 ? this : this.sliceInternal(this.clone(
            this.type, length, this.offset + offset, +(this._nullCount === 0) - 1
        ) as any, offset, length);
    }
    protected sliceInternal(clone: this, offset: number, length: number) {
        let arr: any;
        // If typeIds exist, slice the typeIds buffer
        (arr = this[VectorType.TYPE]) && (clone[VectorType.TYPE] = this.sliceData(arr, offset, length));
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
}

export class FlatData<T extends FlatType> extends BaseData<T> {
    public /*    [VectorType.DATA]:*/ 1: T['TArray'];
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    public get values() { return this[VectorType.DATA]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, data: Iterable<number>, offset?: number, nullCount?: number) {
        super(type, length, offset, nullCount);
        this[VectorType.DATA] = toTypedArray(this.ArrayType, data);
        this[VectorType.VALIDITY] = toTypedArray(Uint8Array, nullBitmap);
    }
    public get ArrayType(): T['ArrayType'] { return this.type.ArrayType; }
    public clone<R extends T>(type: R, length = this.length, offset = this.offset, nullCount = this._nullCount) {
        return new (this.constructor as any)(type, length, this[VectorType.VALIDITY], this[VectorType.DATA], offset, nullCount) as FlatData<R>;
    }
}

export class BoolData extends FlatData<Bool> {
    protected sliceData(data: Uint8Array) { return data; }
}

export class FlatListData<T extends FlatListType> extends FlatData<T> {
    public /*  [VectorType.OFFSET]:*/ 0: Int32Array;
    public /*    [VectorType.DATA]:*/ 1: T['TArray'];
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    public get values() { return this[VectorType.DATA]; }
    public get valueOffsets() { return this[VectorType.OFFSET]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, valueOffsets: Iterable<number>, data: T['TArray'], offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, data, offset, nullCount);
        this[VectorType.OFFSET] = toTypedArray(Int32Array, valueOffsets);
    }
    public clone<R extends T>(type: R, length = this.length, offset = this.offset, nullCount = this._nullCount) {
        return new FlatListData(type, length, this[VectorType.VALIDITY], this[VectorType.OFFSET], this[VectorType.DATA], offset, nullCount) as FlatListData<R>;
    }
}

export class DictionaryData<T extends DataType> extends BaseData<Dictionary<T>> {
    protected _dictionary: Vector<T>;
    protected _indices: Data<Int<any>>;
    public get indices() { return this._indices; }
    public get dictionary() { return this._dictionary; }
    constructor(type: Dictionary<T>, dictionary: Vector<T>, indices: Data<Int<any>>) {
        super(type, indices.length, indices.offset, (indices as any)._nullCount);
        this._indices = indices;
        this._dictionary = dictionary;
    }
    public get nullCount() { return this._indices.nullCount; }
    public get nullBitmap() { return this._indices.nullBitmap; }
    public clone<R extends Dictionary<T>>(type: R, length = this.length, offset = this.offset) {
        const data = this._dictionary.data.clone(type.dictionary as any);
        return new DictionaryData<R>(
            this.type as any,
            this._dictionary.clone(data) as any,
            this._indices.slice(offset - this.offset, length)
        ) as any;
    }
    protected sliceInternal(clone: this, _offset: number, _length: number) {
        clone.length = clone._indices.length;
        clone._nullCount = (clone._indices as any)._nullCount;
        return clone;
    }
}

export class NestedData<T extends NestedType = NestedType> extends BaseData<T> {
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, childData: Data<any>[], offset?: number, nullCount?: number) {
        super(type, length, offset, nullCount);
        this.childData = childData;
        this[VectorType.VALIDITY] = toTypedArray(Uint8Array, nullBitmap);
    }
    public clone<R extends T>(type: R, length = this.length, offset = this.offset, nullCount = this._nullCount): Data<R> {
        return new NestedData<R>(type, length, this[VectorType.VALIDITY], this.childData, offset, nullCount) as any;
    }
    protected sliceInternal(clone: this, offset: number, length: number) {
        if (!this[VectorType.OFFSET]) {
            clone.childData = this.childData.map((child) => child.slice(offset, length));
        }
        return super.sliceInternal(clone, offset, length);
    }
}

export class SingleNestedData<T extends SingleNestedType> extends NestedData<T> {
    protected _valuesData: Data<T>;
    public get values() { return this._valuesData; }
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, valueChildData: Data<T>, offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, [valueChildData], offset, nullCount);
        this._valuesData = valueChildData;
    }
}

export class ListData<T extends ListType> extends SingleNestedData<T> {
    public /*  [VectorType.OFFSET]:*/ 0: Int32Array;
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    public get valueOffsets() { return this[VectorType.OFFSET]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, valueOffsets: Iterable<number>, valueChildData: Data<T>, offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, valueChildData, offset, nullCount);
        this[VectorType.OFFSET] = toTypedArray(Int32Array, valueOffsets);
    }
    public clone<R extends T>(type: R, length = this.length, offset = this.offset, nullCount = this._nullCount): Data<R> {
        return new ListData(type, length, this[VectorType.VALIDITY], this[VectorType.OFFSET], this._valuesData as any, offset, nullCount) as any;
    }
}

export class UnionData<T extends (DenseUnion | SparseUnion) = any> extends NestedData<T> {
    public /*    [VectorType.TYPE]:*/ 3: T['TArray'];
    public get typeIds() { return this[VectorType.TYPE]; }
    public readonly typeIdToChildIndex: { [key: number]: number };
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, typeIds: Iterable<number>, childData: Data<any>[], offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, childData, offset, nullCount);
        this[VectorType.TYPE] = toTypedArray(Int8Array, typeIds);
        this.typeIdToChildIndex = type.typeIds.reduce((typeIdToChildIndex, typeId, idx) => {
            return (typeIdToChildIndex[typeId] = idx) && typeIdToChildIndex || typeIdToChildIndex;
        }, Object.create(null) as { [key: number]: number });
    }
    public clone<R extends T>(type: R, length = this.length, offset = this.offset, nullCount = this._nullCount): Data<R> {
        return new UnionData<R>(type, length, this[VectorType.VALIDITY], this[VectorType.TYPE], this.childData, offset, nullCount) as any;
    }
}

export class SparseUnionData extends UnionData<SparseUnion> {
    constructor(type: SparseUnion, length: number, nullBitmap: Uint8Array | null | undefined, typeIds: Iterable<number>, childData: Data<any>[], offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, typeIds, childData, offset, nullCount);
    }
    public clone<R extends SparseUnion>(type: R, length = this.length, offset = this.offset, nullCount = this._nullCount): Data<R> {
        return new SparseUnionData(
            type,
            length,
            this[VectorType.VALIDITY],
            this[VectorType.TYPE],
            this.childData,
            offset, nullCount
        ) as any;
    }
}

export class DenseUnionData extends UnionData<DenseUnion> {
    public /*  [VectorType.OFFSET]:*/ 0: Int32Array;
    public get valueOffsets() { return this[VectorType.OFFSET]; }
    constructor(type: DenseUnion, length: number, nullBitmap: Uint8Array | null | undefined, typeIds: Iterable<number>, valueOffsets: Iterable<number>, childData: Data<any>[], offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, typeIds, childData, offset, nullCount);
        this[VectorType.OFFSET] = toTypedArray(Int32Array, valueOffsets);
    }
    public clone<R extends DenseUnion>(type: R, length = this.length, offset = this.offset, nullCount = this._nullCount): Data<R> {
        return new DenseUnionData(
            type,
            length,
            this[VectorType.VALIDITY],
            this[VectorType.TYPE],
            this[VectorType.OFFSET],
            this.childData,
            offset, nullCount
        ) as any;
    }
}

export class ChunkedData<T extends DataType> extends BaseData<T> {
    // @ts-ignore
    protected _chunkData: Data<T>[];
    protected _chunkVectors: Vector<T>[];
    protected _chunkOffsets: Uint32Array;
    public get chunkVectors() { return this._chunkVectors; }
    public get chunkOffsets() { return this._chunkOffsets; }
    public get chunkData() {
        return this._chunkData || (
               this._chunkData = this._chunkVectors.map(({ data }) => data));
    }
    constructor(type: T, length: number, chunkVectors: Vector<T>[], offset?: number, nullCount?: number, chunkOffsets?: Uint32Array) {
        super(type, length, offset, nullCount);
        this._chunkVectors = chunkVectors;
        this._chunkOffsets = chunkOffsets || ChunkedData.computeOffsets(chunkVectors);
    }
    public get nullCount() {
        let nullCount = this._nullCount;
        if (nullCount === -1) {
            this._nullCount = nullCount = this._chunkVectors.reduce((x, c) => x + c.nullCount, 0);
        }
        return nullCount;
    }
    public clone<R extends T>(type: R, length = this.length, offset = this.offset, nullCount = this._nullCount): Data<R> {
        return new ChunkedData(
            type, length,
            this._chunkVectors.map((vec) => vec.clone(vec.data.clone(type))) as any,
            offset, nullCount, this._chunkOffsets
        ) as any;
    }
    protected sliceInternal(clone: this, offset: number, length: number) {
        const chunks = this._chunkVectors;
        const offsets = this._chunkOffsets;
        const chunkSlices: Vector<T>[] = [];
        for (let childIndex = -1, numChildren = chunks.length; ++childIndex < numChildren;) {
            const child = chunks[childIndex];
            const childLength = child.length;
            const childOffset = offsets[childIndex];
            // If the child is to the right of the slice boundary, exclude
            if (childOffset >= offset + length) { continue; }
            // If the child is to the left of of the slice boundary, exclude
            if (offset >= childOffset + childLength) { continue; }
            // If the child is between both left and right boundaries, include w/o slicing
            if (childOffset >= offset && (childOffset + childLength) <= offset + length) {
                chunkSlices.push(child);
                continue;
            }
            // If the child overlaps one of the slice boundaries, include that slice
            const begin = Math.max(0, offset - childOffset);
            const end = begin + Math.min(childLength - begin, (offset + length) - childOffset);
            chunkSlices.push(child.slice(begin, end));
        }
        clone._chunkVectors = chunkSlices;
        clone._chunkOffsets = ChunkedData.computeOffsets(chunkSlices);
        return clone;
    }
    static computeOffsets<T extends DataType>(childVectors: Vector<T>[]) {
        const childOffsets = new Uint32Array(childVectors.length + 1);
        for (let index = 0, length = childOffsets.length, childOffset = childOffsets[0] = 0; ++index < length;) {
            childOffsets[index] = (childOffset += childVectors[index - 1].length);
        }
        return childOffsets;
    }
}
