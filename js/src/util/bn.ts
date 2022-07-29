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

import { ArrayBufferViewInput, toArrayBufferView } from './buffer.js';
import { TypedArray, TypedArrayConstructor } from '../interfaces.js';
import { BigIntArray, BigIntArrayConstructor } from '../interfaces.js';
import { BigIntAvailable, BigInt64Array, BigUint64Array } from './compat.js';

/** @ignore */
export const isArrowBigNumSymbol = Symbol.for('isArrowBigNum');

/** @ignore */ type BigNumArray = IntArray | UintArray;
/** @ignore */ type IntArray = Int8Array | Int16Array | Int32Array;
/** @ignore */ type UintArray = Uint8Array | Uint16Array | Uint32Array | Uint8ClampedArray;

/** @ignore */
function BigNum(this: any, x: any, ...xs: any) {
    if (xs.length === 0) {
        return Object.setPrototypeOf(toArrayBufferView(this['TypedArray'], x), this.constructor.prototype);
    }
    return Object.setPrototypeOf(new this['TypedArray'](x, ...xs), this.constructor.prototype);
}

BigNum.prototype[isArrowBigNumSymbol] = true;
BigNum.prototype.toJSON = function <T extends BN<BigNumArray>>(this: T) { return `"${bignumToString(this)}"`; };
BigNum.prototype.valueOf = function <T extends BN<BigNumArray>>(this: T) { return bignumToNumber(this); };
BigNum.prototype.toString = function <T extends BN<BigNumArray>>(this: T) { return bignumToString(this); };
BigNum.prototype[Symbol.toPrimitive] = function <T extends BN<BigNumArray>>(this: T, hint: 'string' | 'number' | 'default' = 'default') {
    switch (hint) {
        case 'number': return bignumToNumber(this);
        case 'string': return bignumToString(this);
        case 'default': return bignumToBigInt(this);
    }
    // @ts-ignore
    return bignumToString(this);
};

/** @ignore */
type TypedArrayConstructorArgs =
    [number | void] |
    [Iterable<number> | Iterable<bigint>] |
    [ArrayBufferLike, number | void, number | void];

/** @ignore */
function SignedBigNum(this: any, ...args: TypedArrayConstructorArgs) { return BigNum.apply(this, args); }
/** @ignore */
function UnsignedBigNum(this: any, ...args: TypedArrayConstructorArgs) { return BigNum.apply(this, args); }
/** @ignore */
function DecimalBigNum(this: any, ...args: TypedArrayConstructorArgs) { return BigNum.apply(this, args); }

Object.setPrototypeOf(SignedBigNum.prototype, Object.create(Int32Array.prototype));
Object.setPrototypeOf(UnsignedBigNum.prototype, Object.create(Uint32Array.prototype));
Object.setPrototypeOf(DecimalBigNum.prototype, Object.create(Uint32Array.prototype));
Object.assign(SignedBigNum.prototype, BigNum.prototype, { 'constructor': SignedBigNum, 'signed': true, 'TypedArray': Int32Array, 'BigIntArray': BigInt64Array });
Object.assign(UnsignedBigNum.prototype, BigNum.prototype, { 'constructor': UnsignedBigNum, 'signed': false, 'TypedArray': Uint32Array, 'BigIntArray': BigUint64Array });
Object.assign(DecimalBigNum.prototype, BigNum.prototype, { 'constructor': DecimalBigNum, 'signed': true, 'TypedArray': Uint32Array, 'BigIntArray': BigUint64Array });

/** @ignore */
function bignumToNumber<T extends BN<BigNumArray>>(bn: T) {
    const { buffer, byteOffset, length, 'signed': signed } = bn;
    const words = new BigUint64Array(buffer, byteOffset, length);
    const negative = signed && words[words.length - 1] & (BigInt(1) << BigInt(63));
    let number = negative ? BigInt(1) : BigInt(0);
    let i = BigInt(0);
    if (!negative) {
        for (const word of words) {
            number += word * (BigInt(1) << (BigInt(32) * i++));
        }
    } else {
        for (const word of words) {
            number += ~word * (BigInt(1) << (BigInt(32) * i++));
        }
        number *= BigInt(-1);
    }
    return number;
}

/** @ignore */
export let bignumToString: { <T extends BN<BigNumArray>>(a: T): string };
/** @ignore */
export let bignumToBigInt: { <T extends BN<BigNumArray>>(a: T): bigint };

if (!BigIntAvailable) {
    bignumToString = decimalToString;
    bignumToBigInt = <any>bignumToString;
} else {
    bignumToBigInt = (<T extends BN<BigNumArray>>(a: T) => a.byteLength === 8 ? new a['BigIntArray'](a.buffer, a.byteOffset, 1)[0] : <any>decimalToString(a));
    bignumToString = (<T extends BN<BigNumArray>>(a: T) => a.byteLength === 8 ? `${new a['BigIntArray'](a.buffer, a.byteOffset, 1)[0]}` : decimalToString(a));
}

/** @ignore */
function decimalToString<T extends BN<BigNumArray>>(a: T) {
    let digits = '';
    const base64 = new Uint32Array(2);
    let base32 = new Uint16Array(a.buffer, a.byteOffset, a.byteLength / 2);
    const checks = new Uint32Array((base32 = new Uint16Array(base32).reverse()).buffer);
    let i = -1;
    const n = base32.length - 1;
    do {
        for (base64[0] = base32[i = 0]; i < n;) {
            base32[i++] = base64[1] = base64[0] / 10;
            base64[0] = ((base64[0] - base64[1] * 10) << 16) + base32[i];
        }
        base32[i] = base64[1] = base64[0] / 10;
        base64[0] = base64[0] - base64[1] * 10;
        digits = `${base64[0]}${digits}`;
    } while (checks[0] || checks[1] || checks[2] || checks[3]);
    return digits ? digits : `0`;
}

/** @ignore */
export class BN<T extends BigNumArray> {
    /** @nocollapse */
    public static new<T extends BigNumArray>(num: T, isSigned?: boolean): (T & BN<T>) {
        switch (isSigned) {
            case true: return new (<any>SignedBigNum)(num) as (T & BN<T>);
            case false: return new (<any>UnsignedBigNum)(num) as (T & BN<T>);
        }
        switch (num.constructor) {
            case Int8Array:
            case Int16Array:
            case Int32Array:
            case BigInt64Array:
                return new (<any>SignedBigNum)(num) as (T & BN<T>);
        }
        if (num.byteLength === 16) {
            return new (<any>DecimalBigNum)(num) as (T & BN<T>);
        }
        return new (<any>UnsignedBigNum)(num) as (T & BN<T>);
    }
    /** @nocollapse */
    public static signed<T extends IntArray>(num: T): (T & BN<T>) {
        return new (<any>SignedBigNum)(num) as (T & BN<T>);
    }
    /** @nocollapse */
    public static unsigned<T extends UintArray>(num: T): (T & BN<T>) {
        return new (<any>UnsignedBigNum)(num) as (T & BN<T>);
    }
    /** @nocollapse */
    public static decimal<T extends UintArray>(num: T): (T & BN<T>) {
        return new (<any>DecimalBigNum)(num) as (T & BN<T>);
    }
    constructor(num: T, isSigned?: boolean) {
        return BN.new(num, isSigned) as any;
    }
}

/** @ignore */
export interface BN<T extends BigNumArray> extends TypedArrayLike<T> {

    new <T extends ArrayBufferViewInput>(buffer: T, signed?: boolean): T;

    readonly signed: boolean;
    readonly TypedArray: TypedArrayConstructor<TypedArray>;
    readonly BigIntArray: BigIntArrayConstructor<BigIntArray>;

    [Symbol.toStringTag]:
    'Int8Array' |
    'Int16Array' |
    'Int32Array' |
    'Uint8Array' |
    'Uint16Array' |
    'Uint32Array' |
    'Uint8ClampedArray';

    /**
     * Convert the bytes to their (positive) decimal representation for printing
     */
    toString(): string;
    /**
     * Down-convert the bytes to a 53-bit precision integer. Invoked by JS for
     * arithmetic operators, like `+`. Easy (and unsafe) way to convert BN to
     * number via `+bn_inst`
     */
    valueOf(): number;
    /**
     * Return the JSON representation of the bytes. Must be wrapped in double-quotes,
     * so it's compatible with JSON.stringify().
     */
    toJSON(): string;
    [Symbol.toPrimitive](hint?: any): number | string | bigint;
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
