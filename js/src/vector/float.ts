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
import { BaseVector } from './base';
import { Vector as V } from '../interfaces';
import { Float, Float16, Float32, Float64 } from '../type';
import { toFloat16Array, toFloat32Array, toFloat64Array } from '../util/buffer';

export class FloatVector<T extends Float = Float> extends BaseVector<T> {

    public static from(this: typeof FloatVector, data: Float16['TArray']): Float16Vector;
    public static from(this: typeof FloatVector, data: Float32['TArray']): Float32Vector;
    public static from(this: typeof FloatVector, data: Float64['TArray']): Float64Vector;
    public static from<T extends Float>(this: typeof FloatVector, data: T['TArray']): V<T>;

    public static from(this: typeof Float16Vector, data: Float16['TArray'] | Iterable<number>): Float16Vector;
    public static from(this: typeof Float32Vector, data: Float32['TArray'] | Iterable<number>): Float32Vector;
    public static from(this: typeof Float64Vector, data: Float64['TArray'] | Iterable<number>): Float64Vector;
    /** @nocollapse */
    public static from<T extends Float>(data: T['TArray']) {
        let type: Float | null = null;
        switch (this) {
            case Float16Vector: data = toFloat16Array(data); break;
            case Float32Vector: data = toFloat32Array(data); break;
            case Float64Vector: data = toFloat64Array(data); break;
        }
        switch (data.constructor) {
            case Uint16Array:  type = new Float16(); break;
            case Float32Array: type = new Float32(); break;
            case Float64Array: type = new Float64(); break;
        }
        return type !== null
            ? Vector.new(Data.Float(type, 0, data.length, 0, null, data))
            : (() => { throw new TypeError('Unrecognized FloatVector input'); })();
    }
}

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

export class Float32Vector extends FloatVector<Float32> {}
export class Float64Vector extends FloatVector<Float64> {}
