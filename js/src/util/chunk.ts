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
import { DataType } from '../type';
import { instance as iteratorVisitor } from '../visitor/iterator';

/** @ignore */
export class ChunkedIterator<T extends DataType> implements IterableIterator<T['TValue'] | null> {
    private chunkIndex = 0;
    private chunkIterator: IterableIterator<T['TValue'] | null>;

    constructor(
        private chunks: ReadonlyArray<Data<T>>,
    ) {
        this.chunkIterator = this.getChunkIterator();
    }

    next(): IteratorResult<T['TValue'] | null> {
        while (this.chunkIndex < this.chunks.length) {
            const next = this.chunkIterator.next();

            if (!next.done) {
                return next;
            }

            if (++this.chunkIndex < this.chunks.length) {
                this.chunkIterator = this.getChunkIterator();
            }
        }

        return {done: true, value: null};
    }

    getChunkIterator() {
        return iteratorVisitor.visit(this.chunks[this.chunkIndex]);
    }

    [Symbol.iterator]() {
        return this;
    }
}

/** @ignore */
export function computeChunkNullCounts<T extends DataType>(chunks: ReadonlyArray<Data<T>>) {
    return chunks.reduce((nullCount, chunk) => {
        return nullCount + chunk.nullCount;
    }, 0);
}

/** @ignore */
export function computeChunkOffsets<T extends DataType>(chunks: ReadonlyArray<Data<T>>) {
    return chunks.reduce((offsets, chunk, index) => {
        offsets[index + 1] = offsets[index] + chunk.length;
        return offsets;
    }, new Uint32Array(chunks.length + 1));
}

/** @ignore */
export function binarySearch<
    T extends DataType,
    F extends (chunks: ReadonlyArray<Data<T>>, _1: number, _2: number) => any
>(chunks: ReadonlyArray<Data<T>>, offsets: Uint32Array, idx: number, fn: F) {
    let lhs = 0, mid = 0, rhs = offsets.length - 1;
    do {
        if (lhs >= rhs - 1) {
            return (idx < offsets[rhs]) ? fn(chunks, lhs, idx - offsets[lhs]) : null;
        }
        mid = lhs + (((rhs - lhs) * .5) | 0);
        idx < offsets[mid] ? (rhs = mid) : (lhs = mid);
    } while (lhs < rhs);
}

/** @ignore */
export function isChunkedValid<T extends DataType>(data: Data<T>, index: number): boolean {
    return data.getValid(index);
}

/** @ignore */
export function wrapChunkedGet<T extends DataType>(fn: (data: Data<T>, _1: any) => any) {
    return (data: Data<T>, _1: any) => data.getValid(_1) ? fn(data, _1) : null;
}

/** @ignore */
export function wrapChunkedSet<T extends DataType>(fn: (data: Data<T>, _1: any, _2: any) => void) {
    return (data: Data<T>, _1: any, _2: any) => {
        if (data.setValid(_1, _2 != null)) {
            return fn(data, _1, _2);
        }
    };
}

/** @ignore */
export function wrapChunkedCall1<T extends DataType>(fn: (c: Data<T>, _1: number) => any) {
    function chunkedFn(chunks: ReadonlyArray<Data<T>>, i: number, j: number) { return fn(chunks[i], j); }
    return function(this: any, index: number) {
        const data = this.data as ReadonlyArray<Data<T>>;
        switch (data.length) {
            case 0: return undefined;
            case 1: return fn(data[0], index);
            default: return binarySearch(data, this._offsets, index, chunkedFn);
        }
    };
}

/** @ignore */
export function wrapChunkedCall2<T extends DataType>(fn: (c: Data<T>, _1: number, _2: any) => any) {
    let _2: any;
    function chunkedFn(chunks: ReadonlyArray<Data<T>>, i: number, j: number) { return fn(chunks[i], j, _2); }
    return function(this: any, index: number, value: any) {
        const data = this.data as ReadonlyArray<Data<T>>;
        switch (data.length) {
            case 0: return undefined;
            case 1: return fn(data[0], index, value);
            default:{
                _2 = value;
                const result = binarySearch(data, this._offsets, index, chunkedFn);
                _2 = undefined;
                return result;
            }
        }
    };
}

/** @ignore */
export function wrapChunkedIndexOf<T extends DataType>(indexOf: (c: Data<T>, e: T['TValue'], o?: number) => any) {
    let _1: any;
    function chunkedIndexOf(data: ReadonlyArray<Data<T>>, chunkIndex: number, fromIndex: number) {
        let begin = fromIndex, index = 0, total = 0;
        for (let i = chunkIndex - 1, n = data.length; ++i < n;) {
            const chunk = data[i];
            if (~(index = indexOf(chunk, _1, begin))) {
                return total + index;
            }
            begin = 0;
            total += chunk.length;
        }
        return -1;
    }
    return function(this: any, element: T['TValue'], offset?: number) {
        _1 = element;
        const data = this.data as ReadonlyArray<Data<T>>;
        const result = typeof offset !== 'number'
            ? chunkedIndexOf(data, 0, 0)
            : binarySearch(data, this._offsets, offset, chunkedIndexOf);
        _1 = undefined;
        return result;
    };
}
