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

import { Vector } from './vector';
import { truncateBitmap } from './util/bit';
import { popcnt_bit_range } from './util/bit';
import { DataType, SparseUnion, DenseUnion } from './type';
import { VectorType as BufferType, UnionMode, Type } from './enum';
import { toArrayBufferView, toUint8Array, toInt32Array } from './util/buffer';
import {
    Dictionary,
    Null, Int, Float,
    Binary, Bool, Utf8, Decimal,
    Date_, Time, Timestamp, Interval,
    List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
} from './type';

// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Vector.nullCount is called the
// first time, the null count will be computed. See ARROW-33
/** @ignore */ export type kUnknownNullCount = -1;
/** @ignore */ export const kUnknownNullCount = -1;

/** @ignore */ export type NullBuffer = Uint8Array | null | undefined;
/** @ignore */ export type TypeIdsBuffer = Int8Array  | ArrayLike<number> | Iterable<number>;
/** @ignore */ export type ValueOffsetsBuffer = Int32Array  | ArrayLike<number> | Iterable<number>;
/** @ignore */ export type DataBuffer<T extends DataType> = T['TArray'] | ArrayLike<number> | Iterable<number>;

/** @ignore */
export interface Buffers<T extends DataType> {
      [BufferType.OFFSET]: Int32Array;
        [BufferType.DATA]: T['TArray'];
    [BufferType.VALIDITY]: Uint8Array;
        [BufferType.TYPE]: T['TArray'];
}

/** @ignore */
export interface Data<T extends DataType = DataType> {
    readonly TType: T['TType'];
    readonly TArray: T['TArray'];
    readonly TValue: T['TValue'];
}

/** @ignore */
export class Data<T extends DataType = DataType> {

    public readonly type: T;
    public readonly length: number;
    public readonly offset: number;
    public readonly stride: number;
    public readonly childData: Data[];
    public readonly values: Buffers<T>[BufferType.DATA];
    public readonly typeIds: Buffers<T>[BufferType.TYPE];
    // @ts-ignore
    public readonly nullBitmap: Buffers<T>[BufferType.VALIDITY];
    // @ts-ignore
    public readonly valueOffsets: Buffers<T>[BufferType.OFFSET];

    public get ArrayType() { return this.type.ArrayType; }
    public get typeId(): T['TType'] { return this.type.typeId; }
    public get buffers() {
        return [this.valueOffsets, this.values, this.nullBitmap, this.typeIds] as Buffers<T>;
    }

    protected _nullCount: number | kUnknownNullCount;

    public get nullCount() {
        let nullCount = this._nullCount;
        let nullBitmap: Uint8Array | undefined;
        if (nullCount <= kUnknownNullCount && (nullBitmap = this.nullBitmap)) {
            this._nullCount = nullCount = this.length - popcnt_bit_range(nullBitmap, this.offset, this.offset + this.length);
        }
        return nullCount;
    }

    constructor(type: T, offset: number, length: number, nullCount?: number, buffers?: Partial<Buffers<T>> | Data<T>, childData?: (Data | Vector)[]) {
        this.type = type;
        this.offset = Math.floor(Math.max(offset || 0, 0));
        this.length = Math.floor(Math.max(length || 0, 0));
        this._nullCount = Math.floor(Math.max(nullCount || 0, -1));
        this.childData = (childData || []).map((x) => x instanceof Data ? x : x.data) as Data[];
        let buffer: Buffers<T>[keyof Buffers<T>];
        if (buffers instanceof Data) {
            this.stride = buffers.stride;
            this.values = buffers.values;
            this.typeIds = buffers.typeIds;
            this.nullBitmap = buffers.nullBitmap;
            this.valueOffsets = buffers.valueOffsets;
        } else {
            if (buffers) {
                (buffer = (buffers as Buffers<T>)[0]) && (this.valueOffsets = buffer);
                (buffer = (buffers as Buffers<T>)[1]) && (this.values = buffer);
                (buffer = (buffers as Buffers<T>)[2]) && (this.nullBitmap = buffer);
                (buffer = (buffers as Buffers<T>)[3]) && (this.typeIds = buffer);
            }
            const t: any = type;
            switch (type.typeId) {
                case Type.Decimal: this.stride = 4; break;
                case Type.Timestamp: this.stride = 2; break;
                case Type.Date: this.stride = 1 + (t as Date_).unit; break;
                case Type.Interval: this.stride = 1 + (t as Interval).unit; break;
                case Type.Int: this.stride = 1 + +((t as Int).bitWidth > 32); break;
                case Type.Time: this.stride = 1 + +((t as Time).bitWidth > 32); break;
                case Type.FixedSizeList: this.stride = (t as FixedSizeList).listSize; break;
                case Type.FixedSizeBinary: this.stride = (t as FixedSizeBinary).byteWidth; break;
                default: this.stride = 1;
            }
        }
    }

    public clone<R extends DataType>(type: R, offset = this.offset, length = this.length, nullCount = this._nullCount, buffers: Buffers<R> = <any> this, childData: (Data | Vector)[] = this.childData) {
        return new Data(type, offset, length, nullCount, buffers, childData);
    }

    public slice(offset: number, length: number): Data<T> {
        // +true === 1, +false === 0, so this means
        // we keep nullCount at 0 if it's already 0,
        // otherwise set to the invalidated flag -1
        const { stride, typeId, childData } = this;
        const nullCount = +(this._nullCount === 0) - 1;
        const childStride = typeId === 16 /* FixedSizeList */ ? stride : 1;
        const buffers = this._sliceBuffers(offset, length, stride, typeId);
        return this.clone<T>(this.type, this.offset + offset, length, nullCount, buffers,
            // Don't slice children if we have value offsets (the variable-width types)
            (!childData.length || this.valueOffsets) ? childData : this._sliceChildren(childData, childStride * offset, childStride * length));
    }

    public _changeLengthAndBackfillNullBitmap(newLength: number): Data<T> {
        const { length, nullCount } = this;
        // start initialized with 0s (nulls), then fill from 0 to length with 1s (not null)
        const bitmap = new Uint8Array(((newLength + 63) & ~63) >> 3).fill(255, 0, length >> 3);
        // set all the bits in the last byte (up to bit `length - length % 8`) to 1 (not null)
        bitmap[length >> 3] = (1 << (length - (length & ~7))) - 1;
        // if we have a nullBitmap, truncate + slice and set it over the pre-filled 1s
        if (nullCount > 0) {
            bitmap.set(truncateBitmap(this.offset, length, this.nullBitmap), 0);
        }
        const buffers = this.buffers;
        buffers[BufferType.VALIDITY] = bitmap;
        return this.clone(this.type, 0, newLength, nullCount + (newLength - length), buffers);
    }

    protected _sliceBuffers(offset: number, length: number, stride: number, typeId: T['TType']): Buffers<T> {
        let arr: any, { buffers } = this;
        // If typeIds exist, slice the typeIds buffer
        (arr = buffers[BufferType.TYPE]) && (buffers[BufferType.TYPE] = arr.subarray(offset, offset + length));
        // If offsets exist, only slice the offsets buffer
        (arr = buffers[BufferType.OFFSET]) && (buffers[BufferType.OFFSET] = arr.subarray(offset, offset + length + 1)) ||
        // Otherwise if no offsets, slice the data buffer. Don't slice the data vector for Booleans, since the offset goes by bits not bytes
        (arr = buffers[BufferType.DATA]) && (buffers[BufferType.DATA] = typeId === 6 ? arr : arr.subarray(stride * offset, stride * (offset + length)));
        return buffers;
    }

    protected _sliceChildren(childData: Data[], offset: number, length: number): Data[] {
        return childData.map((child) => child.slice(offset, length));
    }

    //
    // Convenience methods for creating Data instances for each of the Arrow Vector types
    //
    /** @nocollapse */
    public static new<T extends DataType>(type: T, offset: number, length: number, nullCount?: number, buffers?: Partial<Buffers<T>> | Data<T>, childData?: (Data | Vector)[]): Data<T> {
        if (buffers instanceof Data) { buffers = buffers.buffers; } else if (!buffers) { buffers = [] as Partial<Buffers<T>>; }
        switch (type.typeId) {
            case Type.Null:            return <unknown> Data.Null(            <unknown> type as Null,            offset, length, nullCount || 0, buffers[2]) as Data<T>;
            case Type.Int:             return <unknown> Data.Int(             <unknown> type as Int,             offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.Dictionary:      return <unknown> Data.Dictionary(      <unknown> type as Dictionary,      offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.Float:           return <unknown> Data.Float(           <unknown> type as Float,           offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.Bool:            return <unknown> Data.Bool(            <unknown> type as Bool,            offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.Decimal:         return <unknown> Data.Decimal(         <unknown> type as Decimal,         offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.Date:            return <unknown> Data.Date(            <unknown> type as Date_,           offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.Time:            return <unknown> Data.Time(            <unknown> type as Time,            offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.Timestamp:       return <unknown> Data.Timestamp(       <unknown> type as Timestamp,       offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.Interval:        return <unknown> Data.Interval(        <unknown> type as Interval,        offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.FixedSizeBinary: return <unknown> Data.FixedSizeBinary( <unknown> type as FixedSizeBinary, offset, length, nullCount || 0, buffers[2], buffers[1] || []) as Data<T>;
            case Type.Binary:          return <unknown> Data.Binary(          <unknown> type as Binary,          offset, length, nullCount || 0, buffers[2], buffers[0] || [], buffers[1] || []) as Data<T>;
            case Type.Utf8:            return <unknown> Data.Utf8(            <unknown> type as Utf8,            offset, length, nullCount || 0, buffers[2], buffers[0] || [], buffers[1] || []) as Data<T>;
            case Type.List:            return <unknown> Data.List(            <unknown> type as List,            offset, length, nullCount || 0, buffers[2], buffers[0] || [], (childData || [])[0]) as Data<T>;
            case Type.FixedSizeList:   return <unknown> Data.FixedSizeList(   <unknown> type as FixedSizeList,   offset, length, nullCount || 0, buffers[2], (childData || [])[0]) as Data<T>;
            case Type.Struct:          return <unknown> Data.Struct(          <unknown> type as Struct,          offset, length, nullCount || 0, buffers[2], childData || []) as Data<T>;
            case Type.Map:             return <unknown> Data.Map(             <unknown> type as Map_,            offset, length, nullCount || 0, buffers[2], childData || []) as Data<T>;
            case Type.Union:           return <unknown> Data.Union(           <unknown> type as Union,           offset, length, nullCount || 0, buffers[2], buffers[3] || [], buffers[1] || childData, childData) as Data<T>;
        }
        throw new Error(`Unrecognized typeId ${type.typeId}`);
    }

    /** @nocollapse */
    public static Null<T extends Null>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, _data?: NullBuffer) {
        return new Data(type, offset, length, nullCount, [undefined, undefined, toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Int<T extends Int>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView(type.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Dictionary<T extends Dictionary>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView<T['TArray']>(type.indices.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Float<T extends Float>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView(type.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Bool<T extends Bool>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView(type.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Decimal<T extends Decimal>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView(type.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Date<T extends Date_>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView(type.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Time<T extends Time>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView(type.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Timestamp<T extends Timestamp>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView(type.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Interval<T extends Interval>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView(type.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static FixedSizeBinary<T extends FixedSizeBinary>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, [undefined, toArrayBufferView(type.ArrayType, data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Binary<T extends Binary>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, valueOffsets: ValueOffsetsBuffer, data: Uint8Array) {
        return new Data(type, offset, length, nullCount, [toInt32Array(valueOffsets), toUint8Array(data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static Utf8<T extends Utf8>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, valueOffsets: ValueOffsetsBuffer, data: Uint8Array) {
        return new Data(type, offset, length, nullCount, [toInt32Array(valueOffsets), toUint8Array(data), toUint8Array(nullBitmap)]);
    }
    /** @nocollapse */
    public static List<T extends List>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, valueOffsets: ValueOffsetsBuffer, child: Data<T['valueType']> | Vector<T['valueType']>) {
        return new Data(type, offset, length, nullCount, [toInt32Array(valueOffsets), undefined, toUint8Array(nullBitmap)], [child]);
    }
    /** @nocollapse */
    public static FixedSizeList<T extends FixedSizeList>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, child: Data | Vector) {
        return new Data(type, offset, length, nullCount, [undefined, undefined, toUint8Array(nullBitmap)], [child]);
    }
    /** @nocollapse */
    public static Struct<T extends Struct>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, children: (Data | Vector)[]) {
        return new Data(type, offset, length, nullCount, [undefined, undefined, toUint8Array(nullBitmap)], children);
    }
    /** @nocollapse */
    public static Map<T extends Map_>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, children: (Data | Vector)[]) {
        return new Data(type, offset, length, nullCount, [undefined, undefined, toUint8Array(nullBitmap)], children);
    }
    public static Union<T extends SparseUnion>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, typeIds: TypeIdsBuffer, children: (Data | Vector)[], _?: any): Data<T>;
    public static Union<T extends DenseUnion>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, typeIds: TypeIdsBuffer, valueOffsets: ValueOffsetsBuffer, children: (Data | Vector)[]): Data<T>;
    public static Union<T extends Union>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, typeIds: TypeIdsBuffer, valueOffsetsOrChildren: ValueOffsetsBuffer | (Data | Vector)[], children?: (Data | Vector)[]): Data<T>;
    /** @nocollapse */
    public static Union<T extends Union>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, typeIds: TypeIdsBuffer, valueOffsetsOrChildren: ValueOffsetsBuffer | (Data | Vector)[], children?: (Data | Vector)[]) {
        const buffers = <unknown> [
            undefined, undefined,
            toUint8Array(nullBitmap),
            toArrayBufferView(type.ArrayType, typeIds)
        ] as Partial<Buffers<T>>;
        if (type.mode === UnionMode.Sparse) {
            return new Data(type, offset, length, nullCount, buffers, valueOffsetsOrChildren as (Data | Vector)[]);
        }
        buffers[BufferType.OFFSET] = toInt32Array(<ValueOffsetsBuffer> valueOffsetsOrChildren);
        return new Data(type, offset, length, nullCount, buffers, children);
    }
}

((Data.prototype as any).childData = Object.freeze([]));
