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

export type List<T> = T[] | TypedArray;
export type FloatArray = Float32Array | Float64Array;
export type IntArray = Int8Array | Int16Array | Int32Array;
export type UintArray = Uint8ClampedArray | Uint8Array | Uint16Array | Uint32Array;
