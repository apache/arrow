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
import { Field } from '../schema';
import { clampRange } from '../util/vector';
import { DataType, Dictionary } from '../type';
import { DictionaryVector } from './dictionary';
import { AbstractVector, Vector } from '../vector';
import { selectChunkArgs } from '../util/args';
import { Clonable, Sliceable, Applicative } from '../vector';

/** @ignore */
type ChunkedDict<T extends DataType> = T extends Dictionary ? T['dictionaryVector'] : null | never;
/** @ignore */
type ChunkedKeys<T extends DataType> = T extends Dictionary ? Vector<T['indices']> | Chunked<T['indices']> : null | never;

/** @ignore */
export type SearchContinuation<T extends Chunked> = (column: T, chunkIndex: number, valueIndex: number) => any;

/** @ignore */
export class Chunked<T extends DataType = any>
    extends AbstractVector<T>
    implements Clonable<Chunked<T>>,
               Sliceable<Chunked<T>>,
               Applicative<T, Chunked<T>> {

    /** @nocollapse */
    public static flatten<T extends DataType>(...vectors: (Vector<T> | Vector<T>[])[]) {
        return selectChunkArgs<Vector<T>>(Vector, vectors);
    }

    /** @nocollapse */
    public static concat<T extends DataType>(...vectors: (Vector<T> | Vector<T>[])[]) {
        const chunks = Chunked.flatten<T>(...vectors);
        return new Chunked<T>(chunks[0].type, chunks);
    }

    protected _type: T;
    protected _length: number;
    protected _chunks: Vector<T>[];
    protected _numChildren: number;
    protected _children?: Chunked[];
    protected _nullCount: number = -1;
    protected _chunkOffsets: Uint32Array;

    constructor(type: T, chunks: Vector<T>[] = [], offsets = calculateOffsets(chunks)) {
        super();
        this._type = type;
        this._chunks = chunks;
        this._chunkOffsets = offsets;
        this._length = offsets[offsets.length - 1];
        this._numChildren = (this._type.children || []).length;
    }

    public get type() { return this._type; }
    public get length() { return this._length; }
    public get chunks() { return this._chunks; }
    public get typeId(): T['TType'] { return this._type.typeId; }
    public get data(): Data<T> {
        return this._chunks[0] ? this._chunks[0].data : <any> null;
    }

    public get ArrayType() { return this._type.ArrayType; }
    public get numChildren() { return this._numChildren; }
    public get stride() { return this._chunks[0] ? this._chunks[0].stride : 1; }
    public get nullCount() {
        let nullCount = this._nullCount;
        if (nullCount < 0) {
            this._nullCount = nullCount = this._chunks.reduce((x, { nullCount }) => x + nullCount, 0);
        }
        return nullCount;
    }

    protected _indices?: ChunkedKeys<T>;
    public get indices(): ChunkedKeys<T> | null {
        if (DataType.isDictionary(this._type)) {
            if (!this._indices) {
                const chunks = (<any> this._chunks) as DictionaryVector<T, any>[];
                this._indices = (chunks.length === 1
                    ? chunks[0].indices
                    : Chunked.concat(...chunks.map((x) => x.indices))) as ChunkedKeys<T>;
            }
            return this._indices;
        }
        return null;
    }
    public get dictionary(): ChunkedDict<T> | null {
        if (DataType.isDictionary(this._type)) {
            return (<any> this._type.dictionaryVector) as ChunkedDict<T>;
        }
        return null;
    }

    public *[Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        for (const chunk of this._chunks) {
            yield* chunk;
        }
    }

    public clone(chunks = this._chunks): Chunked<T> {
        return new Chunked(this._type, chunks);
    }

    public concat(...others: Vector<T>[]): Chunked<T> {
        return this.clone(Chunked.flatten(this, ...others));
    }

    public slice(begin?: number, end?: number): Chunked<T> {
        return clampRange(this, begin, end, this._sliceInternal);
    }

    public getChildAt<R extends DataType = any>(index: number): Chunked<R> | null {

        if (index < 0 || index >= this._numChildren) { return null; }

        let columns = this._children || (this._children = []);
        let child: Chunked<R>, field: Field<R>, chunks: Vector<R>[];

        if (child = columns[index]) { return child; }
        if (field = ((this._type.children || [])[index] as Field<R>)) {
            chunks = this._chunks
                .map((vector) => vector.getChildAt<R>(index))
                .filter((vec): vec is Vector<R> => vec != null);
            if (chunks.length > 0) {
                return (columns[index] = new Chunked<R>(field.type, chunks));
            }
        }

        return null;
    }

    public search(index: number): [number, number] | null;
    public search<N extends SearchContinuation<Chunked<T>>>(index: number, then?: N): ReturnType<N>;
    public search<N extends SearchContinuation<Chunked<T>>>(index: number, then?: N) {
        let idx = index;
        // binary search to find the child vector and value indices
        let offsets = this._chunkOffsets, rhs = offsets.length - 1;
        // return early if out of bounds, or if there's just one child
        if (idx < 0            ) { return null; }
        if (idx >= offsets[rhs]) { return null; }
        if (rhs <= 1           ) { return then ? then(this, 0, idx) : [0, idx]; }
        let lhs = 0, pos = 0, mid = 0;
        do {
            if (lhs + 1 === rhs) {
                return then ? then(this, lhs, idx - pos) : [lhs, idx - pos];
            }
            mid = lhs + ((rhs - lhs) / 2) | 0;
            idx >= offsets[mid] ? (lhs = mid) : (rhs = mid);
        } while (idx < offsets[rhs] && idx >= (pos = offsets[lhs]));
        return null;
    }

    public isValid(index: number): boolean {
        return !!this.search(index, this.isValidInternal);
    }

    public get(index: number): T['TValue'] | null {
        return this.search(index, this.getInternal);
    }

    public set(index: number, value: T['TValue'] | null): void {
        this.search(index, ({ chunks }, i, j) => chunks[i].set(j, value));
    }

    public indexOf(element: T['TValue'], offset?: number): number {
        if (offset && typeof offset === 'number') {
            return this.search(offset, (self, i, j) => this.indexOfInternal(self, i, j, element))!;
        }
        return this.indexOfInternal(this, 0, Math.max(0, offset || 0), element);
    }

    public toArray(): T['TArray'] {
        const { chunks } = this;
        const n = chunks.length;
        let { ArrayType } = this._type;
        if (n <= 0) { return new ArrayType(0); }
        if (n <= 1) { return chunks[0].toArray(); }
        let len = 0, src = new Array(n);
        for (let i = -1; ++i < n;) {
            len += (src[i] = chunks[i].toArray()).length;
        }
        if (ArrayType !== src[0].constructor) {
            ArrayType = src[0].constructor;
        }
        let dst = new (ArrayType as any)(len);
        let set: any = ArrayType === Array ? arraySet : typedSet;
        for (let i = -1, idx = 0; ++i < n;) {
            idx = set(src[i], dst, idx);
        }
        return dst;
    }

    protected getInternal({ _chunks }: Chunked<T>, i: number, j: number) { return _chunks[i].get(j); }
    protected isValidInternal({ _chunks }: Chunked<T>, i: number, j: number) { return _chunks[i].isValid(j); }
    protected indexOfInternal({ _chunks }: Chunked<T>, chunkIndex: number, fromIndex: number, element: T['TValue']) {
        let i = chunkIndex - 1, n = _chunks.length;
        let start = fromIndex, offset = 0, found = -1;
        while (++i < n) {
            if (~(found = _chunks[i].indexOf(element, start))) {
                return offset + found;
            }
            start = 0;
            offset += _chunks[i].length;
        }
        return -1;
    }

    protected _sliceInternal(self: Chunked<T>, begin: number, end: number) {
        const slices: Vector<T>[] = [];
        const { chunks, _chunkOffsets: chunkOffsets } = self;
        for (let i = -1, n = chunks.length; ++i < n;) {
            const chunk = chunks[i];
            const chunkLength = chunk.length;
            const chunkOffset = chunkOffsets[i];
            // If the child is to the right of the slice boundary, we can stop
            if (chunkOffset >= end) { break; }
            // If the child is to the left of of the slice boundary, exclude
            if (begin >= chunkOffset + chunkLength) { continue; }
            // If the child is between both left and right boundaries, include w/o slicing
            if (chunkOffset >= begin && (chunkOffset + chunkLength) <= end) {
                slices.push(chunk);
                continue;
            }
            // If the child overlaps one of the slice boundaries, include that slice
            const from = Math.max(0, begin - chunkOffset);
            const to = Math.min(end - chunkOffset, chunkLength);
            slices.push(chunk.slice(from, to) as Vector<T>);
        }
        return self.clone(slices);
    }
}

/** @ignore */
function calculateOffsets<T extends DataType>(vectors: Vector<T>[]) {
    let offsets = new Uint32Array((vectors || []).length + 1);
    let offset = offsets[0] = 0, length = offsets.length;
    for (let index = 0; ++index < length;) {
        offsets[index] = (offset += vectors[index - 1].length);
    }
    return offsets;
}

/** @ignore */
const typedSet = (src: TypedArray, dst: TypedArray, offset: number) => {
    dst.set(src, offset);
    return (offset + src.length);
};

/** @ignore */
const arraySet = (src: any[], dst: any[], offset: number) => {
    let idx = offset - 1;
    for (let i = -1, n = src.length; ++i < n;) {
        dst[++idx] = src[i];
    }
    return idx;
};

/** @ignore */
interface TypedArray extends ArrayBufferView {
    readonly length: number;
    readonly [n: number]: number;
    set(array: ArrayLike<number>, offset?: number): void;
}
