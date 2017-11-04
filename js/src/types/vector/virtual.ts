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

import { TypedVector } from './typed';
import { Vector, Column, TypedArray, TypedArrayConstructor } from '../types';

export class VirtualVector<T> implements Column<T> {
    readonly name: string;
    readonly type: string;
    readonly length: number;
    readonly vectors: Vector<T>[];
    readonly offsets: Uint32Array;
    readonly ArrayType: ArrayConstructor | TypedArrayConstructor;
    constructor(ArrayType: ArrayConstructor | TypedArrayConstructor, ...vectors: Vector<T>[]) {
        this.vectors = vectors;
        this.ArrayType = ArrayType;
        this.name = (vectors[0] as any).name;
        this.type = (vectors[0] as any).type;
        this.length = vectors.reduce((sum, vec) => sum + vec.length, 0);
        this.offsets = Uint32Array.from(vectors.reduce((sums, vector, index) => [...sums, vector.length + sums[index]], [0]));
    }
    *[Symbol.iterator]() {
        for (const vector of this.vectors) {
            yield* vector;
        }
    }
    get nullable() {
        return (this.vectors as Column<T>[]).some((vec) => vec.nullable);
    }
    get nullCount() {
        return (this.vectors as Column<T>[]).reduce((sum, v) => sum + v.nullCount | 0, 0);
    }
    get metadata() {
        return new Map<string, string>(
            (this.vectors as Column<T>[]).reduce((entries, v) => [
                ...entries, ...v.metadata.entries()
            ], [] as [string, string][])
        );
    }
    get(index: number) {
        return findIndex(this.offsets, index) ? this.vectors[_vector].get(_offset) : null;
    }
    concat(...vectors: Vector<T>[]) {
        return new VirtualVector(this.ArrayType, ...this.vectors, ...vectors);
    }
    slice(begin?: number, end?: number) {
        const ArrayType = this.ArrayType as any;
        // clamp begin and end values between the virtual length
        clampRange(this.length, begin!, end);
        const from = _from, total = _total;
        // find the start vector index and adjusted value index offset
        if (!findIndex(this.offsets, from)) { return new ArrayType(0); }
        const set = ArrayType === Array ? arraySet : typedArraySet as any;
        let index = _vector, vectors = this.vectors as TypedVector<T, TypedArray>[];
        let vector = vectors[index], source = vector.slice(_offset, _offset + total), target = source;
        // Perf optimization: if the first slice contains all the values we're looking for,
        // we don't have to copy values to a target Array. If we're slicing a TypedArray,
        // this is a significant improvement as we avoid the memcpy 🎉
        if ((source.length / vector.stride | 0) < total) {
            let vectorsLength = vectors.length;
            let count = 0, length = 0, sources = [];
            do {
                sources.push(source);
                length += source.length;
                count += (source.length / vector.stride | 0);
            } while (
                (count  < total) &&
                (vector = vectors[index = (++index % vectorsLength)]) &&
                (source = vector.slice(0, Math.min(vector.length, total - count)))
            );
            target = new ArrayType(length);
            for (let i = -1, j = 0, n = sources.length; ++i < n;) {
                j = set(sources[i], target, j);
            }
        }
        return target;
    }
}

let _from = -1, _total = -1;
function clampRange(length: number, start: number, end?: number) {
    let total = length, from = start || 0;
    let to = end === end && typeof end == 'number' ? end : total;
    if (to < 0) { to = total + to; }
    if (from < 0) { from = total - (from * -1) % total; }
    if (to < from) { from = to; to = start; }
    _from = from;
    _total = !isFinite(total = (to - from)) || total < 0 ? 0 : total;
}

let _offset = -1, _vector = -1;
function findIndex(offsets: Uint32Array, index: number) {
    let offset = 0, left = 0, middle = 0, right = offsets.length - 1;
    while (index < offsets[right] && index >= (offset = offsets[left])) {
        if (left + 1 === right) {
            _vector = left;
            _offset = index - offset;
            return true;
        }
        middle = left + ((right - left) / 2) | 0;
        index >= offsets[middle] ? (left = middle) : (right = middle);
    }
    return false;
}

function arraySet<T>(source: T[], target: T[], index: number) {
    for (let i = 0, n = source.length; i < n;) {
        target[index++] = source[i++];
    }
    return index;
}

function typedArraySet(source: TypedArray, target: TypedArray, index: number) {
    return target.set(source, index) || index + source.length;
}
