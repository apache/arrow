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
import { Int, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64 } from '../type';
import {
    toInt8Array, toInt16Array, toInt32Array,
    toUint8Array, toUint16Array, toUint32Array,
    toBigInt64Array, toBigUint64Array
} from '../util/buffer';

export class IntVector<T extends Int = Int> extends BaseVector<T> {

    public static from(this: typeof IntVector, data: Int8Array): Int8Vector;
    public static from(this: typeof IntVector, data: Int16Array): Int16Vector;
    public static from(this: typeof IntVector, data: Int32Array): Int32Vector;
    public static from(this: typeof IntVector, data: Uint8Array): Uint8Vector;
    public static from(this: typeof IntVector, data: Uint16Array): Uint16Vector;
    public static from(this: typeof IntVector, data: Uint32Array): Uint32Vector;

    // @ts-ignore
    public static from(this: typeof IntVector, data: Int32Array, is64: true): Int64Vector;
    public static from(this: typeof IntVector, data: Uint32Array, is64: true): Uint64Vector;
    public static from<T extends Int>(this: typeof IntVector, data: T['TArray']): V<T>;

    public static from(this: typeof Int8Vector,   data: Int8['TArray']   | Iterable<number>): Int8Vector;
    public static from(this: typeof Int16Vector,  data: Int16['TArray']  | Iterable<number>): Int16Vector;
    public static from(this: typeof Int32Vector,  data: Int32['TArray']  | Iterable<number>): Int32Vector;
    public static from(this: typeof Int64Vector,  data: Int32['TArray']  | Iterable<number>): Int64Vector;
    public static from(this: typeof Uint8Vector,  data: Uint8['TArray']  | Iterable<number>): Uint8Vector;
    public static from(this: typeof Uint16Vector, data: Uint16['TArray'] | Iterable<number>): Uint16Vector;
    public static from(this: typeof Uint32Vector, data: Uint32['TArray'] | Iterable<number>): Uint32Vector;
    public static from(this: typeof Uint64Vector, data: Uint32['TArray'] | Iterable<number>): Uint64Vector;

    /** @nocollapse */
    public static from<T extends Int>(data: T['TArray'], is64?: boolean) {
        let length: number = 0;
        let type: Int | null = null;
        switch (this) {
            case Int8Vector:   data = toInt8Array(data);   is64 = false; break;
            case Int16Vector:  data = toInt16Array(data);  is64 = false; break;
            case Int32Vector:  data = toInt32Array(data);  is64 = false; break;
            case Int64Vector:  data = toInt32Array(data);  is64 =  true; break;
            case Uint8Vector:  data = toUint8Array(data);  is64 = false; break;
            case Uint16Vector: data = toUint16Array(data); is64 = false; break;
            case Uint32Vector: data = toUint32Array(data); is64 = false; break;
            case Uint64Vector: data = toUint32Array(data); is64 =  true; break;
        }
        if (is64 === true) {
            length = data.length * 0.5;
            type = data instanceof Int32Array ? new Int64() : new Uint64();
        } else {
            length = data.length;
            switch (data.constructor) {
                case Int8Array:   type = new Int8();   break;
                case Int16Array:  type = new Int16();  break;
                case Int32Array:  type = new Int32();  break;
                case Uint8Array:  type = new Uint8();  break;
                case Uint16Array: type = new Uint16(); break;
                case Uint32Array: type = new Uint32(); break;
            }
        }
        return type !== null
            ? Vector.new(Data.Int(type, 0, length, 0, null, data))
            : (() => { throw new TypeError('Unrecognized IntVector input'); })();
    }
}

export class Int8Vector extends IntVector<Int8> {}
export class Int16Vector extends IntVector<Int16> {}
export class Int32Vector extends IntVector<Int32> {}
export class Int64Vector extends IntVector<Int64> {
    public toBigInt64Array() {
        return toBigInt64Array(this.values);
    }
}

export class Uint8Vector extends IntVector<Uint8> {}
export class Uint16Vector extends IntVector<Uint16> {}
export class Uint32Vector extends IntVector<Uint32> {}
export class Uint64Vector extends IntVector<Uint64> {
    public toBigUint64Array() {
        return toBigUint64Array(this.values);
    }
}

export interface Int64Vector extends IntVector<Int64> {
    indexOf(value: Int64['TValue'] | bigint | null, fromIndex?: number): number;
}

export interface Uint64Vector extends IntVector<Uint64> {
    indexOf(value: Uint64['TValue'] | bigint | null, fromIndex?: number): number;
}
