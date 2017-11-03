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

import { Vector } from '../types';
import { VirtualVector } from './virtual';
import { TypedArray, TypedArrayConstructor } from '../types';

export interface TypedVector<T, TArray extends TypedArray> {
    slice(start?: number, end?: number): TArray;
}

export class TypedVector<T, TArray extends TypedArray> extends Vector<T> {
    readonly data: TArray;
    readonly stride: number;
    readonly length: number;
    constructor(argv: { data: TArray } | TArray) {
        super();
        const data = ArrayBuffer.isView(argv) ? argv : argv.data;
        this.length = ((this.data = data).length / this.stride) | 0;
    }
    get(index: number): T | null {
        return this.data[index] as any;
    }
    concat(...vectors: Vector<T>[]): Vector<T> {
        return new VirtualVector(this.data.constructor as TypedArrayConstructor, this, ...vectors);
    }
    slice(start?: number, end?: number) {
        const { data, stride } = this, from = start! | 0;
        const to = end === undefined ? data.length : Math.max(end | 0, from);
        return data.subarray(Math.min(from, to) * stride | 0, to * stride | 0);
    }
}

(TypedVector.prototype as any).stride = 1;

export class Int8Vector extends TypedVector<number, Int8Array> {}
export class Int16Vector extends TypedVector<number, Int16Array> {}
export class Int32Vector extends TypedVector<number, Int32Array> {}
export class Uint8Vector extends TypedVector<number, Uint8Array> {}
export class Uint16Vector extends TypedVector<number, Uint16Array> {}
export class Uint32Vector extends TypedVector<number, Uint32Array> {}
export class Float32Vector extends TypedVector<number, Float32Array> {}
export class Float64Vector extends TypedVector<number, Float64Array> {}
