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

import { toArrayBufferView, ArrayBufferViewInput } from './buffer';

/** @ignore */
type BigNumArray = IntArray | UintArray;
/** @ignore */
type IntArray = Int8Array | Int16Array | Int32Array;
/** @ignore */
type UintArray = Uint8Array | Uint16Array | Uint32Array | Uint8ClampedArray;

/** @ignore */
const BigNumNMixin = {
    toJSON(this: BN<BigNumArray>, ) { return `"${bignumToString(this)}"`; },
    valueOf(this: BN<BigNumArray>, ) { return bignumToNumber(this); },
    toString(this: BN<BigNumArray>, ) { return bignumToString(this); },
    [Symbol.toPrimitive]<T extends BN<BigNumArray>>(this: T, hint: 'string' | 'number' | 'default') {
        if (hint === 'number') { return bignumToNumber(this); }
        /** @suppress {missingRequire} */
        return hint === 'string' || typeof BigInt !== 'function' ?
            bignumToString(this) : BigInt(bignumToString(this));
    }
};

/** @ignore */
const SignedBigNumNMixin: any = Object.assign({}, BigNumNMixin, { signed: true });
/** @ignore */
const UnsignedBigNumNMixin: any = Object.assign({}, BigNumNMixin, { signed: false });

/** @ignore */
export class BN<T extends BigNumArray> {
    public static new<T extends BigNumArray>(input: ArrayBufferViewInput, signed?: boolean): T;
    /** @nocollapse */
    public static new<T extends BigNumArray>(input: ArrayBufferViewInput, signed = (input instanceof Int8Array || input instanceof Int16Array || input instanceof Int32Array)): T {
        return (signed === true) ? BN.signed(input) as T : BN.unsigned(input) as T;
    }
    /** @nocollapse */
    public static signed<T extends IntArray>(input: ArrayBufferViewInput): T {
        const Ctor: any = ArrayBuffer.isView(input) ? <any> input.constructor : Int32Array;
        const { buffer, byteOffset, length } = toArrayBufferView<T>(<any> Ctor, input) as T;
        const bn = new Ctor(buffer, byteOffset, length);
        return Object.assign(bn, SignedBigNumNMixin);
    }
    /** @nocollapse */
    public static unsigned<T extends UintArray>(input: ArrayBufferViewInput): T {
        const Ctor: any = ArrayBuffer.isView(input) ? <any> input.constructor : Uint32Array;
        const { buffer, byteOffset, length } = toArrayBufferView<T>(<any> Ctor, input) as T;
        const bn = new Ctor(buffer, byteOffset, length);
        return Object.assign(bn, UnsignedBigNumNMixin);
    }
    constructor(input: ArrayBufferViewInput, signed = input instanceof Int32Array) {
        return BN.new(input, signed) as any;
    }
}

/** @ignore */
export interface BN<T extends BigNumArray> extends TypedArrayLike<T> {

    new<T extends ArrayBufferViewInput>(buffer: T, signed?: boolean): T;

    readonly signed: boolean;

    [Symbol.toStringTag]:
        'Int8Array'         |
        'Int16Array'        |
        'Int32Array'        |
        'Uint8Array'        |
        'Uint16Array'       |
        'Uint32Array'       |
        'Uint8ClampedArray';

    /**
     * Convert the bytes to their (positive) decimal representation for printing
     */
    toString(): string;
    /**
     * Down-convert the bytes to a 53-bit precision integer. Invoked by JS for
     * arithmatic operators, like `+`. Easy (and unsafe) way to convert BN to
     * number via `+bn_inst`
     */
    valueOf(): number;
    /**
     * Return the JSON representation of the bytes. Must be wrapped in double-quotes,
     * so it's compatible with JSON.stringify().
     */
    toJSON(): string;
    [Symbol.toPrimitive](hint: any): number | string | bigint;
}

/** @ignore */
function bignumToNumber<T extends BN<BigNumArray>>({ buffer, byteOffset, length }: T) {
    let int64 = 0;
    let words = new Uint32Array(buffer, byteOffset, length);
    for (let i = 0, n = words.length; i < n;) {
        int64 += words[i++] + (words[i++] * (i ** 32));
        // int64 += (words[i++] >>> 0) + (words[i++] * (i ** 32));
    }
    return int64;
}

/** @ignore */
function bignumToString<T extends BN<BigNumArray>>({ buffer, byteOffset, length }: T) {

    let string = '', i = -1;
    let base64 = new Uint32Array(2);
    let base32 = new Uint16Array(buffer, byteOffset, length * 2);
    let checks = new Uint32Array((base32 = new Uint16Array(base32).reverse()).buffer);
    let n = base32.length - 1;

    do {
        for (base64[0] = base32[i = 0]; i < n;) {
            base32[i++] = base64[1] = base64[0] / 10;
            base64[0] = ((base64[0] - base64[1] * 10) << 16) + base32[i];
        }
        base32[i] = base64[1] = base64[0] / 10;
        base64[0] = base64[0] - base64[1] * 10;
        string = `${base64[0]}${string}`;
    } while (checks[0] || checks[1] || checks[2] || checks[3]);

    return string ? string : `0`;
}

/** @ignore */
interface TypedArrayLike<T extends BigNumArray> {

    readonly length: number;
    readonly buffer: ArrayBuffer;
    readonly byteLength: number;
    readonly byteOffset: number;
    readonly BYTES_PER_ELEMENT: number;

    includes(searchElement: number, fromIndex?: number | undefined): boolean;
    copyWithin(target: number, start: number, end?: number | undefined): this;
    every(callbackfn: (value: number, index: number, array: T) => boolean, thisArg?: any): boolean;
    fill(value: number, start?: number | undefined, end?: number | undefined): this;
    filter(callbackfn: (value: number, index: number, array: T) => boolean, thisArg?: any): T;
    find(predicate: (value: number, index: number, obj: T) => boolean, thisArg?: any): number | undefined;
    findIndex(predicate: (value: number, index: number, obj: T) => boolean, thisArg?: any): number;
    forEach(callbackfn: (value: number, index: number, array: T) => void, thisArg?: any): void;
    indexOf(searchElement: number, fromIndex?: number | undefined): number;
    join(separator?: string | undefined): string;
    lastIndexOf(searchElement: number, fromIndex?: number | undefined): number;
    map(callbackfn: (value: number, index: number, array: T) => number, thisArg?: any): T;
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: T) => number): number;
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: T) => number, initialValue: number): number;
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: T) => U, initialValue: U): U;
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: T) => number): number;
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: T) => number, initialValue: number): number;
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: T) => U, initialValue: U): U;
    reverse(): T;
    set(array: ArrayLike<number>, offset?: number | undefined): void;
    slice(start?: number | undefined, end?: number | undefined): T;
    some(callbackfn: (value: number, index: number, array: T) => boolean, thisArg?: any): boolean;
    sort(compareFn?: ((a: number, b: number) => number) | undefined): this;
    subarray(begin: number, end?: number | undefined): T;
    toLocaleString(): string;
    entries(): IterableIterator<[number, number]>;
    keys(): IterableIterator<number>;
    values(): IterableIterator<number>;
}
