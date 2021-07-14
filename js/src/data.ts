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
import { BufferType, Type } from './enum';
import { DataType, strideForType } from './type';
import { popcnt_bit_range, truncateBitmap } from './util/bit';
import { toBigInt64Array, toBigUint64Array } from './util/buffer';

// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Vector.nullCount is called the
// first time, the null count will be computed. See ARROW-33
/** @ignore */ export type kUnknownNullCount = -1;
/** @ignore */ export const kUnknownNullCount = -1;

/** @ignore */ export type NullBuffer = Uint8Array | null | undefined;
/** @ignore */ export type TypeIdsBuffer = Int8Array  | ArrayLike<number> | Iterable<number> | undefined;
/** @ignore */ export type ValueOffsetsBuffer = Int32Array  | ArrayLike<number> | Iterable<number> | undefined;
/** @ignore */ export type DataBuffer<T extends DataType> = T['TArray'] | ArrayLike<number> | Iterable<number> | undefined;

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
    public readonly children: Data[];

    /**
     * The dictionary for this Vector, if any. Only used for Dictionary type.
     */
    public dictionary?: Vector;

    public readonly values!: Buffers<T>[BufferType.DATA];
    public readonly typeIds!: Buffers<T>[BufferType.TYPE];
    public readonly nullBitmap!: Buffers<T>[BufferType.VALIDITY];
    public readonly valueOffsets!: Buffers<T>[BufferType.OFFSET];

    public get values64() {
        switch (this.type.typeId) {
            case Type.Int64: return toBigInt64Array(this.values);
            case Type.Uint64: return toBigUint64Array(this.values);
        }
        return null;
    }

    public get typeId(): T['TType'] { return this.type.typeId; }
    public get ArrayType(): T['ArrayType'] { return this.type.ArrayType; }
    public get buffers() {
        return [this.valueOffsets, this.values, this.nullBitmap, this.typeIds] as Buffers<T>;
    }
    public get byteLength(): number {
        let byteLength = 0;
        const { valueOffsets, values, nullBitmap, typeIds } = this;
        valueOffsets && (byteLength += valueOffsets.byteLength);
        values       && (byteLength += values.byteLength);
        nullBitmap   && (byteLength += nullBitmap.byteLength);
        typeIds      && (byteLength += typeIds.byteLength);
        return this.children.reduce((byteLength, child) => byteLength + child.byteLength, byteLength);
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

    constructor(type: T, offset: number, length: number, nullCount?: number, buffers?: Partial<Buffers<T>> | Data<T>, children: Data[] = [], dictionary?: Vector) {
        this.type = type;
        this.children = children;
        this.dictionary = dictionary;
        this.offset = Math.floor(Math.max(offset || 0, 0));
        this.length = Math.floor(Math.max(length || 0, 0));
        this._nullCount = Math.floor(Math.max(nullCount || 0, -1));
        let buffer: Buffers<T>[keyof Buffers<T>];
        if (buffers instanceof Data) {
            this.stride = buffers.stride;
            this.values = buffers.values;
            this.typeIds = buffers.typeIds;
            this.nullBitmap = buffers.nullBitmap;
            this.valueOffsets = buffers.valueOffsets;
        } else {
            this.stride = strideForType(type);
            if (buffers) {
                (buffer = (buffers as Buffers<T>)[0]) && (this.valueOffsets = buffer);
                (buffer = (buffers as Buffers<T>)[1]) && (this.values = buffer);
                (buffer = (buffers as Buffers<T>)[2]) && (this.nullBitmap = buffer);
                (buffer = (buffers as Buffers<T>)[3]) && (this.typeIds = buffer);
            }
        }
    }

    public getValid(index: number) {
        if (this.nullCount > 0) {
            const pos = this.offset + index;
            const val = this.nullBitmap[pos >> 3];
            return (val & (1 << (pos % 8))) !== 0;
        }
        return true;
    }

    public setValid(index: number, value: boolean) {
        // If no null bitmap, initialize one on the fly
        if (!this.nullBitmap) {
            const { nullBitmap } = this._changeLengthAndBackfillNullBitmap(this.length);
            Object.assign(this, { nullBitmap, _nullCount: 0 });
        }
        const { nullBitmap, offset } = this;
        const pos = (offset + index) >> 3;
        const bit = (offset + index) % 8;
        const val = nullBitmap[pos] >> bit;
        // If `val` is truthy and the current bit is 0, flip it to 1 and increment `_nullCount`.
        // If `val` is falsey and the current bit is 1, flip it to 0 and decrement `_nullCount`.
        value ? val === 0 && ((nullBitmap[pos] |=  (1 << bit)), (this._nullCount = this.nullCount + 1))
              : val !== 0 && ((nullBitmap[pos] &= ~(1 << bit)), (this._nullCount = this.nullCount - 1));
        return value;
    }

    public clone<R extends DataType>(type: R, offset = this.offset, length = this.length, nullCount = this._nullCount, buffers: Buffers<R> = <any> this, children: Data[] = this.children) {
        return new Data(type, offset, length, nullCount, buffers, children, this.dictionary);
    }

    public slice(offset: number, length: number): Data<T> {
        const { stride, typeId, children } = this;
        // +true === 1, +false === 0, so this means
        // we keep nullCount at 0 if it's already 0,
        // otherwise set to the invalidated flag -1
        const nullCount = +(this._nullCount === 0) - 1;
        const childStride = typeId === 16 /* FixedSizeList */ ? stride : 1;
        const buffers = this._sliceBuffers(offset, length, stride, typeId);
        return this.clone<T>(this.type, this.offset + offset, length, nullCount, buffers,
            // Don't slice children if we have value offsets (the variable-width types)
            (!children.length || this.valueOffsets) ? children : this._sliceChildren(children, childStride * offset, childStride * length));
    }

    public _changeLengthAndBackfillNullBitmap(newLength: number): Data<T> {
        if (this.typeId === Type.Null) {
            return this.clone(this.type, 0, newLength, 0);
        }
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
        let arr: any;
        const { buffers } = this;
        // If typeIds exist, slice the typeIds buffer
        (arr = buffers[BufferType.TYPE]) && (buffers[BufferType.TYPE] = arr.subarray(offset, offset + length));
        // If offsets exist, only slice the offsets buffer
        (arr = buffers[BufferType.OFFSET]) && (buffers[BufferType.OFFSET] = arr.subarray(offset, offset + length + 1)) ||
        // Otherwise if no offsets, slice the data buffer. Don't slice the data vector for Booleans, since the offset goes by bits not bytes
        (arr = buffers[BufferType.DATA]) && (buffers[BufferType.DATA] = typeId === 6 ? arr : arr.subarray(stride * offset, stride * (offset + length)));
        return buffers;
    }

    protected _sliceChildren(children: Data[], offset: number, length: number): Data[] {
        return children.map((child) => child.slice(offset, length));
    }
}

(Data.prototype as any).children = Object.freeze([]);

export { makeData } from './visitor/data';
