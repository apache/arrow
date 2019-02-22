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

export class IntVector<T extends Int = Int> extends BaseVector<T> {
    public static from<T extends Int>(data: T['TArray']): V<T>;
    public static from<T extends Int64>(data: T['TArray'], is64: true): V<T>;
    public static from<T extends Uint64>(data: T['TArray'], is64: true): V<T>;
    /** @nocollapse */
    public static from(data: any, is64?: boolean) {
        if (is64 === true) {
            return data instanceof Int32Array
                ? Vector.new(Data.Int(new Int64(), 0, data.length * 0.5, 0, null, data))
                : Vector.new(Data.Int(new Uint64(), 0, data.length * 0.5, 0, null, data));
        }
        switch (data.constructor) {
            case Int8Array: return Vector.new(Data.Int(new Int8(), 0, data.length, 0, null, data));
            case Int16Array: return Vector.new(Data.Int(new Int16(), 0, data.length, 0, null, data));
            case Int32Array: return Vector.new(Data.Int(new Int32(), 0, data.length, 0, null, data));
            case Uint8Array: return Vector.new(Data.Int(new Uint8(), 0, data.length, 0, null, data));
            case Uint16Array: return Vector.new(Data.Int(new Uint16(), 0, data.length, 0, null, data));
            case Uint32Array: return Vector.new(Data.Int(new Uint32(), 0, data.length, 0, null, data));
        }
        throw new TypeError('Unrecognized Int data');
    }
}

export class Int8Vector extends IntVector<Int8> {}
export class Int16Vector extends IntVector<Int16> {}
export class Int32Vector extends IntVector<Int32> {}
export class Int64Vector extends IntVector<Int64> {}
export class Uint8Vector extends IntVector<Uint8> {}
export class Uint16Vector extends IntVector<Uint16> {}
export class Uint32Vector extends IntVector<Uint32> {}
export class Uint64Vector extends IntVector<Uint64> {}
