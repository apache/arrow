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

import { Data } from '../data';
import { Vector } from '../vector';
import { Chunked } from './chunked';
import { BaseVector } from './base';
import { VectorBuilderOptions } from './index';
import { vectorFromValuesWithType } from './index';
import { VectorBuilderOptionsAsync } from './index';
import { BigInt64Array, BigUint64Array } from '../util/compat';
import { toBigInt64Array, toBigUint64Array } from '../util/buffer';
import { Int, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, IntArray } from '../type';
import { VectorType as V, TypedArrayConstructor, BigIntArrayConstructor, BigIntArray } from '../interfaces';

/** @ignore */
type IntVectorConstructors =
    typeof IntVector    |
    typeof Int8Vector   |
    typeof Int16Vector  |
    typeof Int32Vector  |
    typeof Uint8Vector  |
    typeof Uint16Vector |
    typeof Uint32Vector |
    typeof Int64Vector  |
    typeof Uint64Vector ;

/** @ignore */
type FromInput<T extends Int, TNull = any> =
    IntArray | BigIntArray              |
    Iterable<T['TValue'] | TNull>       |
    AsyncIterable<T['TValue'] | TNull>  |
    VectorBuilderOptions<T, TNull>      |
    VectorBuilderOptionsAsync<T, TNull> ;

/** @ignore */
type FromArgs<T extends Int, TNull = any> = [FromInput<T, TNull>, boolean?];

/** @ignore */
type IntArrayCtor = TypedArrayConstructor<IntArray> | BigIntArrayConstructor<BigIntArray>;

/** @ignore */
export class IntVector<T extends Int = Int> extends BaseVector<T> {

    // Guaranteed zero-copy variants
    public static from(this: typeof IntVector, input: Int8Array): Int8Vector;
    public static from(this: typeof IntVector, input: Int16Array): Int16Vector;
    public static from(this: typeof IntVector, input: Int32Array): Int32Vector;
    public static from(this: typeof IntVector, input: BigInt64Array): Int64Vector;
    public static from(this: typeof IntVector, input: Int32Array, is64bit: true): Int64Vector;
    public static from(this: typeof IntVector, input: Uint8Array): Uint8Vector;
    public static from(this: typeof IntVector, input: Uint16Array): Uint16Vector;
    public static from(this: typeof IntVector, input: Uint32Array): Uint32Vector;
    public static from(this: typeof IntVector, input: BigUint64Array): Uint64Vector;
    public static from(this: typeof IntVector, input: Uint32Array, is64bit: true): Uint64Vector;

    // Zero-copy if input is a TypedArray of the same type as the
    // Vector that from is called on, otherwise uses the Builders
    public static from<TNull = any>(this: typeof Int8Vector,   input: FromInput<Int8, TNull>): Int8Vector;
    public static from<TNull = any>(this: typeof Int16Vector,  input: FromInput<Int16, TNull>): Int16Vector;
    public static from<TNull = any>(this: typeof Int32Vector,  input: FromInput<Int32, TNull>): Int32Vector;
    public static from<TNull = any>(this: typeof Int64Vector,  input: FromInput<Int64, TNull>): Int64Vector;
    public static from<TNull = any>(this: typeof Uint8Vector,  input: FromInput<Uint8, TNull>): Uint8Vector;
    public static from<TNull = any>(this: typeof Uint16Vector, input: FromInput<Uint16, TNull>): Uint16Vector;
    public static from<TNull = any>(this: typeof Uint32Vector, input: FromInput<Uint32, TNull>): Uint32Vector;
    public static from<TNull = any>(this: typeof Uint64Vector, input: FromInput<Uint64, TNull>): Uint64Vector;

    // Not zero-copy
    public static from<T extends Int, TNull = any>(this: typeof IntVector, input: Iterable<T['TValue'] | TNull>): V<T>;
    public static from<T extends Int, TNull = any>(this: typeof IntVector, input: AsyncIterable<T['TValue'] | TNull>): Promise<V<T>>;
    public static from<T extends Int, TNull = any>(this: typeof IntVector, input: VectorBuilderOptions<T, TNull>): Chunked<T>;
    public static from<T extends Int, TNull = any>(this: typeof IntVector, input: VectorBuilderOptionsAsync<T, TNull>): Promise<Chunked<T>>;
    /** @nocollapse */
    public static from<T extends Int, TNull = any>(this: IntVectorConstructors, ...args: FromArgs<T, TNull>) {

        let [input, is64bit = false] = args;
        let ArrowType = vectorTypeToDataType(this, is64bit);

        if ((input instanceof ArrayBuffer) || ArrayBuffer.isView(input)) {
            let InputType = arrayTypeToDataType(input.constructor as IntArrayCtor, is64bit) || ArrowType;
            // Special case, infer the Arrow DataType from the input if calling the base
            // IntVector.from with a TypedArray, e.g. `IntVector.from(new Int32Array())`
            if (ArrowType === null) {
                ArrowType = InputType;
            }
            // If the DataType inferred from the Vector constructor matches the
            // DataType inferred from the input arguments, return zero-copy view
            if (ArrowType && ArrowType === InputType) {
                let type = new ArrowType();
                let length = input.byteLength / type.ArrayType.BYTES_PER_ELEMENT;
                // If the ArrowType is 64bit but the input type is 32bit pairs, update the logical length
                if (convert32To64Bit(ArrowType, input.constructor)) {
                    length *= 0.5;
                }
                return Vector.new(Data.Int(type, 0, length, 0, null, input as IntArray));
            }
        }

        if (ArrowType) {
            // If the DataType inferred from the Vector constructor is different than
            // the DataType inferred from the input TypedArray, or if input isn't a
            // TypedArray, use the Builders to construct the result Vector
            return vectorFromValuesWithType(() => new ArrowType!() as T, input);
        }

        if ((input instanceof DataView) || (input instanceof ArrayBuffer)) {
            throw new TypeError(`Cannot infer integer type from instance of ${input.constructor.name}`);
        }

        throw new TypeError('Unrecognized IntVector input');
    }
}

/** @ignore */
export class Int8Vector extends IntVector<Int8> {}
/** @ignore */
export class Int16Vector extends IntVector<Int16> {}
/** @ignore */
export class Int32Vector extends IntVector<Int32> {}
/** @ignore */
export class Int64Vector extends IntVector<Int64> {
    public toBigInt64Array() {
        return toBigInt64Array(this.values);
    }
    // @ts-ignore
    private _values64: BigInt64Array;
    public get values64(): BigInt64Array {
        return this._values64 || (this._values64 = this.toBigInt64Array());
    }
}

/** @ignore */
export class Uint8Vector extends IntVector<Uint8> {}
/** @ignore */
export class Uint16Vector extends IntVector<Uint16> {}
/** @ignore */
export class Uint32Vector extends IntVector<Uint32> {}
/** @ignore */
export class Uint64Vector extends IntVector<Uint64> {
    public toBigUint64Array() {
        return toBigUint64Array(this.values);
    }
    // @ts-ignore
    private _values64: BigUint64Array;
    public get values64(): BigUint64Array {
        return this._values64 || (this._values64 = this.toBigUint64Array());
    }
}

const convert32To64Bit = (typeCtor: any, dataCtor: any) => {
    return (typeCtor === Int64 || typeCtor === Uint64) &&
           (dataCtor === Int32Array || dataCtor === Uint32Array);
};

/** @ignore */
const arrayTypeToDataType = (ctor: IntArrayCtor, is64bit: boolean) => {
    switch (ctor) {
        case Int8Array:      return Int8;
        case Int16Array:     return Int16;
        case Int32Array:     return is64bit ? Int64 : Int32;
        case BigInt64Array:  return Int64;
        case Uint8Array:     return Uint8;
        case Uint16Array:    return Uint16;
        case Uint32Array:    return is64bit ? Uint64 : Uint32;
        case BigUint64Array: return Uint64;
        default: return null;
    }
};

/** @ignore */
const vectorTypeToDataType = (ctor: IntVectorConstructors, is64bit: boolean) => {
    switch (ctor) {
        case Int8Vector:   return Int8;
        case Int16Vector:  return Int16;
        case Int32Vector:  return is64bit ? Int64 : Int32;
        case Int64Vector:  return Int64;
        case Uint8Vector:  return Uint8;
        case Uint16Vector: return Uint16;
        case Uint32Vector: return is64bit ? Uint64 : Uint32;
        case Uint64Vector: return Uint64;
        default: return null;
    }
};
