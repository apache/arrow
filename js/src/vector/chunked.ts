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

import { ChunkedData } from '../data';
import { View, Vector, NestedVector } from '../vector';
import { DataType, TypedArray, IterableArrayLike } from '../type';

export class ChunkedView<T extends DataType> implements View<T> {
    public chunkVectors: Vector<T>[];
    public chunkOffsets: Uint32Array;
    // @ts-ignore
    protected _children: Vector<any>[];
    constructor(data: ChunkedData<T>) {
        this.chunkVectors = data.chunkVectors;
        this.chunkOffsets = data.chunkOffsets;
    }
    public clone(data: ChunkedData<T>): this {
        return new ChunkedView(data) as this;
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        for (const vector of this.chunkVectors) {
            yield* vector;
        }
    }
    public getChildAt<R extends DataType = DataType>(index: number) {
        return index < 0 ? null
            : (this._children || (this._children = []))[index] ||
              (this._children[index] = Vector.concat<R>(
                  ...(<any> this.chunkVectors as NestedVector<any>[])
                         .map((chunk) => chunk.getChildAt<R>(index))));
    }
    public isValid(index: number): boolean {
        // binary search to find the child vector and value index offset (inlined for speed)
        let offsets = this.chunkOffsets, pos = 0;
        let lhs = 0, mid = 0, rhs = offsets.length - 1;
        while (index < offsets[rhs] && index >= (pos = offsets[lhs])) {
            if (lhs + 1 === rhs) {
                return this.chunkVectors[lhs].isValid(index - pos);
            }
            mid = lhs + ((rhs - lhs) / 2) | 0;
            index >= offsets[mid] ? (lhs = mid) : (rhs = mid);
        }
        return false;
    }
    public get(index: number): T['TValue'] | null {
        // binary search to find the child vector and value index offset (inlined for speed)
        let offsets = this.chunkOffsets, pos = 0;
        let lhs = 0, mid = 0, rhs = offsets.length - 1;
        while (index < offsets[rhs] && index >= (pos = offsets[lhs])) {
            if (lhs + 1 === rhs) {
                return this.chunkVectors[lhs].get(index - pos);
            }
            mid = lhs + ((rhs - lhs) / 2) | 0;
            index >= offsets[mid] ? (lhs = mid) : (rhs = mid);
        }
        return null;
    }
    public set(index: number, value: T['TValue'] | null): void {
        // binary search to find the child vector and value index offset (inlined for speed)
        let offsets = this.chunkOffsets, pos = 0;
        let lhs = 0, mid = 0, rhs = offsets.length - 1;
        while (index < offsets[rhs] && index >= (pos = offsets[lhs])) {
            if (lhs + 1 === rhs) {
                return this.chunkVectors[lhs].set(index - pos, value);
            }
            mid = lhs + ((rhs - lhs) / 2) | 0;
            index >= offsets[mid] ? (lhs = mid) : (rhs = mid);
        }
    }
    public toArray(): IterableArrayLike<T['TValue'] | null> {
        const chunks = this.chunkVectors;
        const numChunks = chunks.length;
        if (numChunks === 1) {
            return chunks[0].toArray();
        }
        let sources = new Array<any>(numChunks);
        let sourcesLen = 0, ArrayType: any = Array;
        for (let index = -1; ++index < numChunks;) {
            let source = chunks[index].toArray();
            sourcesLen += (sources[index] = source).length;
            if (ArrayType !== source.constructor) {
                ArrayType = source.constructor;
            }
        }
        let target = new ArrayType(sourcesLen);
        let setValues = ArrayType === Array ? arraySet : typedArraySet as any;
        for (let index = -1, offset = 0; ++index < numChunks;) {
            offset = setValues(sources[index], target, offset);
        }
        return target;
    }
}

function typedArraySet(source: TypedArray, target: TypedArray, index: number) {
    return target.set(source, index) || index + source.length;
}

function arraySet(source: any[], target: any[], index: number) {
    let dstIdx = index - 1, srcIdx = -1, srcLen = source.length;
    while (++srcIdx < srcLen) {
        target[++dstIdx] = source[srcIdx];
    }
    return dstIdx;
}
