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

import { DataType } from './type';
import { Vector } from './vector';
import { popcnt_bit_range } from './util/bit';
import { toArrayBufferView } from './util/buffer';
import { VectorType as BufferType, UnionMode } from './enum';
import {
    Dictionary,
    Null, Int, Float,
    Binary, Bool, Utf8, Decimal,
    Date_, Time, Timestamp, Interval,
    List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
} from './type';

// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Array::null_count is called the
// first time, the null count will be computed. See ARROW-33
/** @ignore */ export type kUnknownNullCount = -1;
/** @ignore */ export const kUnknownNullCount = -1;

/** @ignore */ export type NullBuffer = Uint8Array | null | undefined;
/** @ignore */ export type TypeIdsBuffer = Int8Array  | ArrayLike<number> | Iterable<number>;
/** @ignore */ export type ValueOffsetsBuffer = Int32Array  | ArrayLike<number> | Iterable<number>;
/** @ignore */ export type DataBuffer<T extends DataType> = T['TArray'] | ArrayLike<number> | Iterable<number>;

/** @ignore */
export interface Buffers<T extends DataType> {
      [BufferType.OFFSET]?: Int32Array;
        [BufferType.DATA]?: T['TArray'];
    [BufferType.VALIDITY]?: Uint8Array;
        [BufferType.TYPE]?: T['TArray'];
}

export interface Data<T extends DataType = DataType> {
    readonly TType: T['TType'];
    readonly TArray: T['TArray'];
    readonly TValue: T['TValue'];
}

/** @ignore */
export class Data<T extends DataType = DataType> {

    protected _type: T;
    protected _length: number;
    protected _offset: number;

    // @ts-ignore
    protected _childData: Data[];
    protected _buffers = [] as Buffers<T>;
    protected _nullCount: number | kUnknownNullCount;

    public get type() { return this._type; }
    public get length() { return this._length; }
    public get offset() { return this._offset; }
    public get typeId() { return this._type.typeId; }
    public get childData() { return this._childData; }

    public get ArrayType() { return this._type.ArrayType; }

    public get buffers() { return this._buffers; }
    public get values() { return this._buffers[BufferType.DATA]!; }
    public get typeIds() { return this._buffers[BufferType.TYPE]!; }
    public get nullBitmap() { return this._buffers[BufferType.VALIDITY]!; }
    public get valueOffsets() { return this._buffers[BufferType.OFFSET]!; }
    public get nullCount() {
        let nullCount = this._nullCount;
        let nullBitmap: Uint8Array | undefined;
        if (nullCount === kUnknownNullCount && (nullBitmap = this._buffers[BufferType.VALIDITY])) {
            this._nullCount = nullCount = this._length - popcnt_bit_range(nullBitmap, this._offset, this._offset + this._length);
        }
        return nullCount;
    }

    constructor(type: T, offset: number, length: number, nullCount?: number, buffers?: Buffers<T>, childData?: (Data | Vector)[]) {
        this._type = type;
        this._offset = Math.floor(Math.max(offset || 0, 0));
        this._length = Math.floor(Math.max(length || 0, 0));
        this._buffers = Object.assign([], buffers) as Buffers<T>;
        this._nullCount = Math.floor(Math.max(nullCount || 0, -1));
        this._childData = (childData || []).map((x) => x instanceof Data ? x : x.data) as Data[];
    }

    public clone<R extends DataType>(type: R, offset = this._offset, length = this._length, nullCount = this._nullCount, buffers: Buffers<R> = <any> this._buffers, childData: (Data | Vector)[] = this._childData) {
        return new Data(type, offset, length, nullCount, buffers, childData);
    }

    public slice(offset: number, length: number): Data<T> {
        // +true === 1, +false === 0, so this means
        // we keep nullCount at 0 if it's already 0,
        // otherwise set to the invalidated flag -1
        const nullCount = +(this._nullCount === 0) - 1;
        const buffers = this.sliceBuffers(offset, length);
        const childData = this.sliceChildren(offset, length);
        return this.clone<T>(this._type, this._offset + offset, length, nullCount, buffers, childData);
    }

    protected sliceBuffers(offset: number, length: number): Buffers<T> {
        let arr: any, buffers = Object.assign([], this._buffers) as Buffers<T>;
        // If typeIds exist, slice the typeIds buffer
        (arr = buffers[BufferType.TYPE]) && (buffers[BufferType.TYPE] = this.sliceData(arr, offset, length));
        // If offsets exist, only slice the offsets buffer
        (arr = buffers[BufferType.OFFSET]) && (buffers[BufferType.OFFSET] = this.sliceOffsets(arr, offset, length)) ||
            // Otherwise if no offsets, slice the data buffer
            (arr = buffers[BufferType.DATA]) && (buffers[BufferType.DATA] = this.sliceData(arr, offset, length));
        return buffers;
    }

    protected sliceChildren(offset: number, length: number): Data[] {
        // Only slice children if this isn't variable width data
        if (!this._buffers[BufferType.OFFSET]) {
            return this._childData.map((child) => child.slice(offset, length));
        }
        return this._childData;
    }

    protected sliceData(data: T['TArray'] & ArrayBufferView, offset: number, length: number) {
        // Don't slice the data vector for Booleans, since the offset goes by bits not bytes
        return this._type.typeId === 6 ? data : data.subarray(offset, offset + length);
    }

    protected sliceOffsets(valueOffsets: Int32Array, offset: number, length: number) {
        return valueOffsets.subarray(offset, offset + length + 1);
    }

    //
    // Convenience methods for creating Data instances for each of the Arrow Vector types
    //
    /** @nocollapse */
    public static Null<T extends Null>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap)
        });
    }
    /** @nocollapse */
    public static Int<T extends Int>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView(type.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static Dictionary<T extends Dictionary>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView<T['TArray']>(type.indices.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static Float<T extends Float>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView(type.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static Bool<T extends Bool>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView(type.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static Decimal<T extends Decimal>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView(type.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static Date<T extends Date_>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView(type.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static Time<T extends Time>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView(type.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static Timestamp<T extends Timestamp>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView(type.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static Interval<T extends Interval>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView(type.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static FixedSizeBinary<T extends FixedSizeBinary>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.DATA]: toArrayBufferView(type.ArrayType, data)
        });
    }
    /** @nocollapse */
    public static Binary<T extends Binary>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, valueOffsets: ValueOffsetsBuffer, data: Uint8Array) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.OFFSET]: toArrayBufferView(Int32Array, valueOffsets),
            [BufferType.DATA]: toArrayBufferView(Uint8Array, data)
        });
    }
    /** @nocollapse */
    public static Utf8<T extends Utf8>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, valueOffsets: ValueOffsetsBuffer, data: Uint8Array) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.OFFSET]: toArrayBufferView(Int32Array, valueOffsets),
            [BufferType.DATA]: toArrayBufferView(Uint8Array, data)
        });
    }
    /** @nocollapse */
    public static List<T extends List>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, valueOffsets: ValueOffsetsBuffer, childData: Data | Vector) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.OFFSET]: toArrayBufferView(Int32Array, valueOffsets)
        }, [childData]);
    }
    /** @nocollapse */
    public static FixedSizeList<T extends FixedSizeList>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, childData: Data | Vector) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap)
        }, [childData]);
    }
    /** @nocollapse */
    public static Struct<T extends Struct>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, childData: (Data | Vector)[]) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap)
        }, childData);
    }
    /** @nocollapse */
    public static Map<T extends Map_>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, childData: (Data | Vector)[]) {
        return new Data(type, offset, length, nullCount, {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap)
        }, childData);
    }
    /** @nocollapse */
    public static Union<T extends Union>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, typeIds: TypeIdsBuffer, valueOffsetsOrChildData: ValueOffsetsBuffer | (Data | Vector)[], childData?: (Data | Vector)[]) {
        const buffers = {
            [BufferType.VALIDITY]: toArrayBufferView(Uint8Array, nullBitmap),
            [BufferType.TYPE]: toArrayBufferView(type.ArrayType, typeIds)
        } as any;
        if (type.mode === UnionMode.Sparse) {
            return new Data(type, offset, length, nullCount, buffers, valueOffsetsOrChildData as (Data | Vector)[]);
        }
        buffers[BufferType.OFFSET] = toArrayBufferView(Int32Array, <ValueOffsetsBuffer> valueOffsetsOrChildData);
        return new Data(type, offset, length, nullCount, buffers, childData);
    }
}
