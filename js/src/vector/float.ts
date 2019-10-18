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
import { Float, Float16, Float32, Float64, FloatArray } from '../type';
import { VectorType as V, TypedArrayConstructor } from '../interfaces';

/** @ignore */
type FloatVectorConstructors =
    typeof FloatVector   |
    typeof Float16Vector |
    typeof Float32Vector |
    typeof Float64Vector ;

/** @ignore */
type FromInput<T extends Float, TNull = any> =
    FloatArray                          |
    Iterable<T['TValue'] | TNull>       |
    AsyncIterable<T['TValue'] | TNull>  |
    VectorBuilderOptions<T, TNull>      |
    VectorBuilderOptionsAsync<T, TNull> ;

/** @ignore */
type FloatArrayCtor = TypedArrayConstructor<FloatArray>;

/** @ignore */
export class FloatVector<T extends Float = Float> extends BaseVector<T> {

    // Guaranteed zero-copy variants
    public static from(this: typeof FloatVector, input: Uint16Array): Float16Vector;
    public static from(this: typeof FloatVector, input: Float32Array): Float32Vector;
    public static from(this: typeof FloatVector, input: Float64Array): Float64Vector;

    // Zero-copy if input is a TypedArray of the same type as the
    // Vector that from is called on, otherwise uses the Builders
    public static from<TNull = any>(this: typeof Float16Vector,  input: FromInput<Float16, TNull>): Float16Vector;
    public static from<TNull = any>(this: typeof Float32Vector,  input: FromInput<Float32, TNull>): Float32Vector;
    public static from<TNull = any>(this: typeof Float64Vector,  input: FromInput<Float64, TNull>): Float64Vector;

    // Not zero-copy
    public static from<T extends Float, TNull = any>(this: typeof FloatVector, input: Iterable<T['TValue'] | TNull>): V<T>;
    public static from<T extends Float, TNull = any>(this: typeof FloatVector, input: AsyncIterable<T['TValue'] | TNull>): Promise<V<T>>;
    public static from<T extends Float, TNull = any>(this: typeof FloatVector, input: VectorBuilderOptions<T, TNull>): Chunked<T>;
    public static from<T extends Float, TNull = any>(this: typeof FloatVector, input: VectorBuilderOptionsAsync<T, TNull>): Promise<Chunked<T>>;
    /** @nocollapse */
    public static from<T extends Float, TNull = any>(this: FloatVectorConstructors, input: FromInput<T, TNull>) {

        let ArrowType = vectorTypeToDataType(this);

        if ((input instanceof ArrayBuffer) || ArrayBuffer.isView(input)) {
            let InputType = arrayTypeToDataType(input.constructor as FloatArrayCtor) || ArrowType;
            // Special case, infer the Arrow DataType from the input if calling the base
            // FloatVector.from with a TypedArray, e.g. `FloatVector.from(new Float32Array())`
            if (ArrowType === null) {
                ArrowType = InputType;
            }
            // If the DataType inferred from the Vector constructor matches the
            // DataType inferred from the input arguments, return zero-copy view
            if (ArrowType && ArrowType === InputType) {
                let type = new ArrowType();
                let length = input.byteLength / type.ArrayType.BYTES_PER_ELEMENT;
                // If the ArrowType is Float16 but the input type isn't a Uint16Array,
                // let the Float16Builder handle casting the input values to Uint16s.
                if (!convertTo16Bit(ArrowType, input.constructor)) {
                    return Vector.new(Data.Float(type, 0, length, 0, null, input as FloatArray));
                }
            }
        }

        if (ArrowType) {
            // If the DataType inferred from the Vector constructor is different than
            // the DataType inferred from the input TypedArray, or if input isn't a
            // TypedArray, use the Builders to construct the result Vector
            return vectorFromValuesWithType(() => new ArrowType!() as T, input);
        }

        if ((input instanceof DataView) || (input instanceof ArrayBuffer)) {
            throw new TypeError(`Cannot infer float type from instance of ${input.constructor.name}`);
        }

        throw new TypeError('Unrecognized FloatVector input');
    }
}

/** @ignore */
export class Float16Vector extends FloatVector<Float16> {
    // Since JS doesn't have half floats, `toArray()` returns a zero-copy slice
    // of the underlying Uint16Array data. This behavior ensures we don't incur
    // extra compute or copies if you're calling `toArray()` in order to create
    // a buffer for something like WebGL. Buf if you're using JS and want typed
    // arrays of 4-to-8-byte precision, these methods will enumerate the values
    // and clamp to the desired byte lengths.
    public toFloat32Array() { return new Float32Array(this as Iterable<number>); }
    public toFloat64Array() { return new Float64Array(this as Iterable<number>); }
}

/** @ignore */
export class Float32Vector extends FloatVector<Float32> {}
/** @ignore */
export class Float64Vector extends FloatVector<Float64> {}

const convertTo16Bit = (typeCtor: any, dataCtor: any) => {
    return (typeCtor === Float16) && (dataCtor !== Uint16Array);
};

/** @ignore */
const arrayTypeToDataType = (ctor: FloatArrayCtor) => {
    switch (ctor) {
        case Uint16Array:    return Float16;
        case Float32Array:   return Float32;
        case Float64Array:   return Float64;
        default: return null;
    }
};

/** @ignore */
const vectorTypeToDataType = (ctor: FloatVectorConstructors) => {
    switch (ctor) {
        case Float16Vector: return Float16;
        case Float32Vector: return Float32;
        case Float64Vector: return Float64;
        default: return null;
    }
};
