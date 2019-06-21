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

import { memcpy } from '../util/buffer';
import { BigInt64Array, BigUint64Array } from '../util/compat';
import {
    TypedArray, TypedArrayConstructor,
    BigIntArray, BigIntArrayConstructor
} from '../interfaces';

/** @ignore */ type WideArray<T extends BigIntArray> = T extends BigIntArray ? Int32Array : Uint32Array;
/** @ignore */ type DataValue<T> = T extends TypedArray ? number : T extends BigIntArray ? WideValue<T> : T;
/** @ignore */ type WideValue<T extends BigIntArray> = T extends BigIntArray ? bigint | Int32Array | Uint32Array : never;
/** @ignore */ type ArrayCtor<T extends TypedArray | BigIntArray> =
    T extends TypedArray  ? TypedArrayConstructor<T>  :
    T extends BigIntArray ? BigIntArrayConstructor<T> :
    any;

/** @ignore */
const roundLengthUpToNearest64Bytes = (len: number, BPE: number) => ((((len * BPE) + 63) & ~63) || 64) / BPE;
/** @ignore */
const sliceOrExtendArray = <T extends TypedArray | BigIntArray>(arr: T, len = 0) => (
    arr.length >= len ? arr.subarray(0, len) : memcpy(new (arr.constructor as any)(len), arr, 0)
) as T;

/** @ignore */
export interface BufferBuilder<T extends TypedArray | BigIntArray = any, TValue = DataValue<T>> {
    readonly offset: number;
}

/** @ignore */
export class BufferBuilder<T extends TypedArray | BigIntArray = any, TValue = DataValue<T>> {

    constructor(buffer: T, stride = 1) {
        this.buffer = buffer;
        this.stride = stride;
        this.BYTES_PER_ELEMENT = buffer.BYTES_PER_ELEMENT;
        this.ArrayType = buffer.constructor as ArrayCtor<T>;
        this._resize(this.length = buffer.length / stride | 0);
    }

    public buffer: T;
    public length: number;
    public readonly stride: number;
    public readonly ArrayType: ArrayCtor<T>;
    public readonly BYTES_PER_ELEMENT: number;

    public get byteLength() { return this.length * this.stride * this.BYTES_PER_ELEMENT | 0; }
    public get reservedLength() { return this.buffer.length / this.stride; }
    public get reservedByteLength() { return this.buffer.byteLength; }

    // @ts-ignore
    public set(index: number, value: TValue) { return this; }
    public append(value: TValue) { return this.set(this.length, value); }
    public reserve(extra: number) {
        if (extra > 0) {
            this.length += extra;
            const stride = this.stride;
            const length = this.length * stride;
            const reserved = this.buffer.length;
            if (length >= reserved) {
                this._resize(reserved === 0
                    ? roundLengthUpToNearest64Bytes(length * 1, this.BYTES_PER_ELEMENT)
                    : roundLengthUpToNearest64Bytes(length * 2, this.BYTES_PER_ELEMENT)
                );
            }
        }
        return this;
    }
    public flush(length = this.length) {
        length = roundLengthUpToNearest64Bytes(length * this.stride, this.BYTES_PER_ELEMENT);
        const array = sliceOrExtendArray<T>(this.buffer, length);
        this.clear();
        return array;
    }
    public clear() {
        this.length = 0;
        this._resize(0);
        return this;
    }
    protected _resize(newLength: number) {
        return this.buffer = <T> memcpy(new this.ArrayType(newLength), this.buffer);
    }
}

(BufferBuilder.prototype as any).offset = 0;

/** @ignore */
export class DataBufferBuilder<T extends TypedArray> extends BufferBuilder<T, number> {
    public last() { return this.get(this.length - 1); }
    public get(index: number) { return this.buffer[index]; }
    public set(index: number, value: number) {
        this.reserve(index - this.length + 1);
        this.buffer[index] = value;
        return this;
    }
}

/** @ignore */
export class BitmapBufferBuilder extends DataBufferBuilder<Uint8Array> {

    constructor(data = new Uint8Array(0)) { super(data, 1 / 8); }

    public numValid = 0;
    public get numInvalid() { return this.length - this.numValid; }
    public get(idx: number) { return this.buffer[idx >> 3] >> idx % 8 & 1; }
    public set(idx: number, val: number) {
        const { buffer } = this.reserve(idx - this.length + 1);
        const byte = idx >> 3, bit = idx % 8, cur = buffer[byte] >> bit & 1;
        // If `val` is truthy and the current bit is 0, flip it to 1 and increment `numValid`.
        // If `val` is falsey and the current bit is 1, flip it to 0 and decrement `numValid`.
        val ? cur === 0 && ((buffer[byte] |=  (1 << bit)), ++this.numValid)
            : cur === 1 && ((buffer[byte] &= ~(1 << bit)), --this.numValid);
        return this;
    }
    public clear() {
        this.numValid = 0;
        return super.clear();
    }
}

/** @ignore */
export class OffsetsBufferBuilder extends DataBufferBuilder<Int32Array> {
    constructor(data = new Int32Array(1)) { super(data, 1); }
    public append(value: number) {
        return this.set(this.length - 1, value);
    }
    public set(index: number, value: number) {
        const offset = this.length - 1;
        const buffer = this.reserve(index - offset + 1).buffer;
        if (offset < index++) {
            buffer.fill(buffer[offset], offset, index);
        }
        buffer[index] = buffer[index - 1] + value;
        return this;
    }
    public flush(length = this.length - 1) {
        if (length > this.length) {
            this.set(length - 1, 0);
        }
        return super.flush(length + 1);
    }
}

/** @ignore */
export class WideBufferBuilder<T extends BigIntArray> extends BufferBuilder<WideArray<T>, DataValue<T>> {
    // @ts-ignore
    public buffer64: T;
    // @ts-ignore
    constructor(buffer: T, stride: number) {
        const ArrayType = buffer instanceof BigInt64Array ? Int32Array : Uint32Array;
        super(new ArrayType(buffer.buffer, buffer.byteOffset, buffer.byteLength / 4) as WideArray<T>, stride);
    }
    public get ArrayType64(): BigIntArrayConstructor<T> {
        return this.buffer instanceof Int32Array ? BigInt64Array : BigUint64Array as any;
    }
    public set(index: number, value: DataValue<T>) {
        this.reserve(index - this.length + 1);
        switch (typeof value) {
            case 'bigint': this.buffer64[index] = value; break;
            case 'number': this.buffer[index * this.stride] = value; break;
            default: this.buffer.set(value as TypedArray, index * this.stride);
        }
        return this;
    }
    protected _resize(newLength: number) {
        const data = super._resize(newLength);
        const length = data.byteLength / (this.BYTES_PER_ELEMENT * this.stride);
        this.buffer64 = new this.ArrayType64(data.buffer, data.byteOffset, length);
        return data;
    }
}
