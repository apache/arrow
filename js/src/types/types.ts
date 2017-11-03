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

import * as Schema_ from '../format/Schema_generated';
import Type = Schema_.org.apache.arrow.flatbuf.Type;

export interface TypedArrayConstructor<T extends TypedArray = TypedArray> {
    readonly prototype: T;
    readonly BYTES_PER_ELEMENT: number;
    new (length: number): T;
    new (elements: Iterable<number>): T;
    new (arrayOrArrayBuffer: ArrayLike<number> | ArrayBufferLike): T;
    new (buffer: ArrayBufferLike, byteOffset: number, length?: number): T;
}

export interface TypedArray extends Iterable<number> {
    [index: number]: number;
    readonly length: number;
    readonly byteLength: number;
    readonly byteOffset: number;
    readonly buffer: ArrayBufferLike;
    readonly BYTES_PER_ELEMENT: number;
    [Symbol.iterator](): IterableIterator<number>;
    slice(start?: number, end?: number): TypedArray;
    subarray(begin: number, end?: number): TypedArray;
    set(array: ArrayLike<number>, offset?: number): void;
}

export type FloatArray = Float32Array | Float64Array;
export type IntArray = Int8Array | Int16Array | Int32Array;
export type UintArray = Uint8ClampedArray | Uint8Array | Uint16Array | Uint32Array;

export type List<T> = T[] | TypedArray;

export interface Vector<T = any> extends Iterable<T | null> {
    readonly length: number;
    get(index: number): T | null;
    concat(...vectors: Vector<T>[]): Vector<T>;
    slice<R = T[]>(start?: number, end?: number): R;
}

export interface Row<T = any> extends Vector<T> {
    col(key: string): T | null;
}

export interface Column<T = any> extends Vector<T> {
    readonly name: string;
    readonly type: string;
    readonly nullable: boolean;
    readonly nullCount: number;
    readonly metadata: Map<string, string>;
}

export interface Struct<T = any> extends Vector<Row<T>> {
    readonly columns: Column[];
    key(key: number): string | null;
    col(key: string): Column | null;
    select(...columns: string[]): Struct<T>;
    concat(...structs: Vector<Row<T>>[]): Vector<Row<T>>;
}

export class Vector<T = any> implements Vector<T> {
    slice<R = T[]>(start?: number, end?: number): R {
        let { length } = this, from = start! | 0;
        let to = end === undefined ? length : Math.max(end | 0, from);
        let result = new Array<T | null>(to - Math.min(from, to));
        for (let i = -1, n = result.length; ++i < n;) {
            result[i] = this.get(i + from);
        }
        return result as any;
    }
    *[Symbol.iterator]() {
        for (let i = -1, n = this.length; ++i < n;) {
            yield this.get(i);
        }
    }
}

(Vector.prototype as any).name = '';
(Vector.prototype as any).type = Type[0];
(Vector.prototype as any).stride = 1;
(Vector.prototype as any).nullable = !1;
(Vector.prototype as any).nullCount = 0;
(Vector.prototype as any).metadata = new Map();
