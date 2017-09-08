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
import { flatbuffers } from 'flatbuffers';

import Long = flatbuffers.Long;

export type VArray<T = any> = {
    [k: number]: T; length: number;
    constructor: VArrayCtor<VArray<T>>;
};

export type VArrayCtor<VArray> = {
    readonly prototype: VArray;
    BYTES_PER_ELEMENT?: number;
    new(...args: any[]): VArray;
};

export class VirtualVector<T, TArrayType = VArray<T>> extends Vector<T> {
    protected lists: TArrayType[];
    protected _arrayType: VArrayCtor<TArrayType>;
    public get arrayType() { return this._arrayType; }
    constructor(...lists: TArrayType[]) {
        super();
        this.lists = lists.filter(Boolean);
    }
    get(index: number): T {
        /* inlined `findVirtual` impl */
        let rows, length, lists = this.lists;
        for (let batch = -1;
            (rows = lists[++batch]) &&
            (length = rows.length) <= index &&
            0 <= (index -= length);) {}
        return rows && -1 < index ? rows[index] : null;
    }
    protected range(from: number, total: number, batch?: number) {
        /* inlined `findVirtual` impl */
        let rows, local = from, length;
        let { lists, _arrayType } = this;
        for (batch = (batch || 0) - 1;
            (rows = lists[++batch]) &&
            (length = rows.length) <= local &&
            0 <= (local -= length);) {}
        if (rows && local > -1) {
            let index = 0, listsLength = lists.length;
            let set: any = Array.isArray(rows) ? arraySet : typedArraySet;
            let slice = _arrayType['prototype']['subarray'] || _arrayType['prototype']['slice'];
            let source = slice.call(rows, local, local + total), target = source;
            // Perf optimization: if the first slice contains all the values we're looking for,
            // we don't have to copy values to a target Array. If we're slicing a TypedArray,
            // this is a significant improvement as we avoid the memcpy 🎉
            if (source.length < total) {
                target = new _arrayType(total);
                while ((index = set(source, target, index)) < total) {
                    rows = lists[batch = ((batch + 1) % listsLength)];
                    source = slice.call(rows, 0, Math.min(rows.length, total - index));
                }
            }
            return target as any;
        }
        return new _arrayType(0);
    }
    *[Symbol.iterator]() {
        let index = -1, { lists, length } = this;
        for (let outer = -1, n = lists.length; ++outer < n;) {
            let list = lists[outer] as any;
            for (let inner = -1, k = list.length; ++index < length && ++inner < k;) {
                yield list[inner];
            }
        }
    }
}

export type ValidityArgs = Vector<boolean> | Uint8Array;
export class BitVector extends VirtualVector<boolean, Uint8Array> {
    static constant: Vector<boolean> = new (class ValidVector extends Vector<boolean> {
        get() { return true; }
        *[Symbol.iterator]() {
            do { yield true; } while (true);
        }
    })();
    static from(src: any) {
        return src instanceof BitVector   ? src
             : src === BitVector.constant ? src
             : src instanceof Uint8Array       ? new BitVector(src)
             : src instanceof Array            ? new BitVector(BitVector.pack(src))
             : src instanceof Vector           ? new BitVector(BitVector.pack(src))
                                               : BitVector.constant as Vector<any>;
    }
    static pack(values: Iterable<any>) {
        let xs = [], n, i = 0;
        let bit = 0, byte = 0;
        for (const value of values) {
            value && (byte |= 1 << bit);
            if (++bit === 8) {
                xs[i++] = byte;
                byte = bit = 0;
            }
        }
        if (i === 0 || bit > 0) { xs[i++] = byte; }
        if (i % 8 && (n = n = i + 8 - i % 8)) {
            do { xs[i] = 0; } while (++i < n);
        }
        return new Uint8Array(xs);
    }
    constructor(...lists: Uint8Array[]) {
        super(...lists);
        this.length = this.lists.reduce((l, xs) => l + xs['length'], 0);
    }
    get(index: number) {
        /* inlined `findVirtual` impl */
        let rows, length, lists = this.lists;
        for (let batch = -1;
            (rows = lists[++batch]) &&
            (length = rows.length) <= index &&
            0 <= (index -= length);) {}
        return !(!rows || index < 0 || (rows[index >> 3 | 0] & 1 << index % 8) === 0);
    }
    set(index: number, value: boolean) {
        /* inlined `findVirtual` impl */
        let rows, length, lists = this.lists;
        for (let batch = -1;
            (rows = lists[++batch]) &&
            (length = rows.length) <= index &&
            0 <= (index -= length);) {}
        if (rows && index > -1) {
            value
                ? (rows[index >> 3 | 0] |=  (1 << (index % 8)))
                : (rows[index >> 3 | 0] &= ~(1 << (index % 8)));
        }
    }
    concat(vector: BitVector) {
        return new BitVector(...this.lists, ...vector.lists);
    }
    *[Symbol.iterator]() {
        for (const byte of super[Symbol.iterator]()) {
            for (let i = -1; ++i < 8;) {
                yield (byte & 1 << i) !== 0;
            }
        }
    }
}

export class TypedVector<T, TArrayType> extends VirtualVector<T, TArrayType> {
    constructor(validity: ValidityArgs, ...lists: TArrayType[]) {
        super(...lists);
        validity && (this.validity = BitVector.from(validity));
    }
    concat(vector: TypedVector<T, TArrayType>) {
        return (this.constructor as typeof TypedVector).from(this,
            this.length + vector.length,
            this.validity.concat(vector.validity),
            ...this.lists, ...vector.lists
        );
    }
}

export class DateVector extends TypedVector<Date, Uint32Array> {
    get(index: number) {
        return !this.validity.get(index) ? null : new Date(
            Math.pow(2, 32) *
                <any> super.get(2 * index + 1) +
                <any> super.get(2 * index)
        );
    }
    *[Symbol.iterator]() {
        let v, low, high;
        let it = super[Symbol.iterator]();
        let iv = this.validity[Symbol.iterator]();
        while (!(v = iv.next()).done && !(low = it.next()).done && !(high = it.next()).done) {
            yield !v.value ? null : new Date(Math.pow(2, 32) * high.value + low.value);
        }
    }
}

export class IndexVector extends TypedVector<number | number[], Int32Array> {
    get(index: number, returnWithBatchIndex = false) {
        /* inlined `findVirtual` impl */
        let rows, length, batch = -1, lists = this.lists;
        for (;
            (rows = lists[++batch]) &&
            (length = rows.length) <= index &&
            0 <= (index -= length);) {}
        return !returnWithBatchIndex
            ? (rows && -1 < index ? rows[index + batch] : null) as number
            : (rows && -1 < index ? [rows[index + batch], batch] : [0, -1]) as number[];
    }
    *[Symbol.iterator]() {
        // Alternate between iterating a tuple of [from, batch], and to. The from
        // and to values are relative to the record batch they're defined in, so
        // `ListVectorBase` needs to know the right batch to read.
        let xs = new Int32Array(2), { lists } = this;
        for (let i = -1, n = lists.length; ++i < n;) {
            let list = lists[i] as any;
            for (let j = -1, k = list.length - 1; ++j < k;) {
                xs[1] = i;
                xs[0] = list[j];
                yield xs;
                yield list[j + 1];
            }
        }
    }
}

export class ByteVector<TList> extends TypedVector<number, TList> {
    get(index: number) {
        return this.validity.get(index) ? super.get(index) : null;
    }
    *[Symbol.iterator]() {
        let v, r, { validity } = this;
        let it = super[Symbol.iterator]();
        // fast path the case of no nulls
        if (validity === BitVector.constant) {
            yield* it;
        } else {
            let iv = validity[Symbol.iterator]();
            while (!(v = iv.next()).done && !(r = it.next()).done) {
                yield !v.value ? null : r.value;
            }
        }
    }
}

export class LongVector<TList> extends TypedVector<Long, TList> {
    get(index: number) {
        return !this.validity.get(index) ? null : new Long(
            <any> super.get(index * 2),     /* low */
            <any> super.get(index * 2 + 1) /* high */
        );
    }
    *[Symbol.iterator]() {
        let v, low, high;
        let it = super[Symbol.iterator]();
        let iv = this.validity[Symbol.iterator]();
        while (!(v = iv.next()).done && !(low = it.next()).done && !(high = it.next()).done) {
            yield !v.value ? null : new Long(low.value, high.value);
        }
    }
}

export class Int8Vector    extends ByteVector<Int8Array>    {}
export class Int16Vector   extends ByteVector<Int16Array>   {}
export class Int32Vector   extends ByteVector<Int32Array>   {}
export class Int64Vector   extends LongVector<Int32Array>   {}
export class Uint8Vector   extends ByteVector<Uint8Array>   {}
export class Uint16Vector  extends ByteVector<Uint16Array>  {}
export class Uint32Vector  extends ByteVector<Uint32Array>  {}
export class Uint64Vector  extends LongVector<Uint32Array>  {}
export class Float32Vector extends ByteVector<Float32Array> {}
export class Float64Vector extends ByteVector<Float64Array> {}

LongVector.prototype.stride = 2;
(Vector.prototype as any).lists = [];
(Vector.prototype as any).validity = BitVector.constant;
(VirtualVector.prototype as any)._arrayType = Array;
(BitVector.prototype as any)._arrayType = Uint8Array;
(Int8Vector.prototype as any)._arrayType = Int8Array;
(Int16Vector.prototype as any)._arrayType = Int16Array;
(Int32Vector.prototype as any)._arrayType = Int32Array;
(Int64Vector.prototype as any)._arrayType = Int32Array;
(Uint8Vector.prototype as any)._arrayType = Uint8Array;
(Uint16Vector.prototype as any)._arrayType = Uint16Array;
(Uint32Vector.prototype as any)._arrayType = Uint32Array;
(Uint64Vector.prototype as any)._arrayType = Uint32Array;
(DateVector.prototype as any)._arrayType = Uint32Array;
(IndexVector.prototype as any)._arrayType = Int32Array;
(Float32Vector.prototype as any)._arrayType = Float32Array;
(Float64Vector.prototype as any)._arrayType = Float64Array;

function arraySet<T>(source: Array<T>, target: Array<T>, index: number) {
    for (let i = 0, n = source.length; i < n;) {
        target[index++] = source[i++];
    }
    return index;
}

function typedArraySet(source: TypedArray, target: TypedArray, index: number) {
    return target.set(source, index) || index + source.length;
}

// Rather than eat the iterator cost, we've inlined this function into the relevant functions
// function* findVirtual<TList>(index: number, lists: TList[], batch?: number) {
//     let rows, length;
//     for (batch = (batch || 0) - 1;
//         (rows = lists[++batch]) &&
//         (length = rows.length) <= index &&
//         0 <= (index -= length);) {}
//     return rows && -1 < index ? yield [rows, index, batch] : null;
// }

export type TypedArrayCtor<T extends TypedArray> = {
    readonly prototype: T;
    readonly BYTES_PER_ELEMENT: number;
    new(length: number): T;
    new(array: ArrayLike<number>): T;
    new(buffer: ArrayBufferLike, byteOffset?: number, length?: number): T;
};

export type FloatArray = Float32Array | Float64Array;
export type IntArray = Int8Array | Int16Array | Int32Array | Uint8ClampedArray | Uint8Array | Uint16Array | Uint32Array;

export type TypedArray = (
            Int8Array        |
            Uint8Array       |
            Int16Array       |
            Int32Array       |
            Uint16Array      |
            Uint32Array      |
            Float32Array     |
            Float64Array     |
            Uint8ClampedArray);
