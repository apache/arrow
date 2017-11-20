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

import { Vector } from './vector';
import { NumericVector } from './numeric';
import { TypedArray, TypedArrayConstructor } from './types';

export class VirtualVector<T> implements Vector<T> {
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
        return (this.vectors as Vector<T>[]).some((vec) => vec.nullable);
    }
    get nullCount() {
        return (this.vectors as Vector<T>[]).reduce((sum, v) => sum + v.nullCount | 0, 0);
    }
    get metadata() {
        return new Map<string, string>(
            (this.vectors as Vector<T>[]).reduce((entries, v) => [
                ...entries, ...v.metadata.entries()
            ], [] as [string, string][])
        );
    }
    get(index: number) {
        // find the vector index and adjusted value offset (inlined)
        let offsets = this.offsets, offset = 0;
        let left = 0, middle = 0, right = offsets.length - 1;
        while (index < offsets[right] && index >= (offset = offsets[left])) {
            if (left + 1 === right) {
                return this.vectors[left].get(index - offset);
            }
            middle = left + ((right - left) / 2) | 0;
            index >= offsets[middle] ? (left = middle) : (right = middle);
        }
        return null;
    }
    concat(...vectors: Vector<T>[]) {
        return new VirtualVector(this.ArrayType, ...this.vectors, ...vectors);
    }
    slice(begin?: number, end?: number) {

        // clamp begin and end values between the virtual length (inlined)
        // let [from, total] = clampRange(this.length, begin!, end);
        let total = this.length, from = begin! | 0;
        let to = end === end && typeof end == 'number' ? end : total;
        if (to < 0) { to = total + to; }
        if (from < 0) { from = total - (from * -1) % total; }
        if (to < from) { from = to; to = begin! | 0; }
        total = !isFinite(total = (to - from)) || total < 0 ? 0 : total;

        // find the vector index and adjusted value offset (inlined)
        let offsets = this.offsets, ArrayType = this.ArrayType as any;
        let offset = 0, index = 0, middle = 0, right = offsets.length - 1;
        while (from < offsets[right] && from >= (offset = offsets[index])) {
            if (index + 1 === right) {
                from -= offset;
                let set = ArrayType === Array ? arraySet : typedArraySet as any;
                let vectors = this.vectors as any as NumericVector<T, TypedArray>[];
                let vector = vectors[index], source = vector.slice(from, from + total), target = source;
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
            middle = index + ((right - index) / 2) | 0;
            from >= offsets[middle] ? (index = middle) : (right = middle);
        }
        return new ArrayType(0);
    }
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
