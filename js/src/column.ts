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

import { Field } from './schema';
import { Vector } from './vector';
import { DataType } from './type';
import { Clonable, Sliceable, Applicative } from './vector';
import { Chunked, SearchContinuation } from './vector/chunked';

export interface Column<T extends DataType = any> {
    typeId: T['TType'];
    concat(...others: Vector<T>[]): Column<T>;
    slice(begin?: number, end?: number): Column<T>;
    clone(chunks?: Vector<T>[], offsets?: Uint32Array): Column<T>;
}

export class Column<T extends DataType = any>
    extends Chunked<T>
    implements Clonable<Column<T>>,
               Sliceable<Column<T>>,
               Applicative<T, Column<T>> {

    constructor(field: Field<T>, vectors: Vector<T>[] = [], offsets?: Uint32Array) {
        vectors = Chunked.flatten(...vectors);
        super(field.type, vectors, offsets);
        this._field = field;
        if (vectors.length === 1 && !(this instanceof SingleChunkColumn)) {
            return new SingleChunkColumn(field, vectors[0], this._chunkOffsets);
        }
    }

    protected _field: Field<T>;
    protected _children?: Column[];

    public get field() { return this._field; }
    public get name() { return this._field.name; }

    public clone(chunks = this._chunks) {
        return new Column(this._field, chunks);
    }

    public getChildAt<R extends DataType = any>(index: number): Column<R> | null {

        if (index < 0 || index >= this.numChildren) { return null; }

        let columns = this._children || (this._children = []);
        let column: Column<R>, field: Field<R>, chunks: Vector<R>[];

        if (column = columns[index]) { return column; }
        if (field = ((this.type.children || [])[index] as Field<R>)) {
            chunks = this._chunks
                .map((vector) => vector.getChildAt<R>(index))
                .filter((vec): vec is Vector<R> => vec != null);
            if (chunks.length > 0) {
                return (columns[index] = new Column<R>(field, chunks));
            }
        }

        return null;
    }
}

class SingleChunkColumn<T extends DataType = any> extends Column<T> {
    protected _chunk: Vector<T>;
    constructor(field: Field<T>, vector: Vector<T>, offsets?: Uint32Array) {
        super(field, [vector], offsets);
        this._chunk = vector;
    }
    public search(index: number): [number, number] | null;
    public search<N extends SearchContinuation<Chunked<T>>>(index: number, then?: N): ReturnType<N>;
    public search<N extends SearchContinuation<Chunked<T>>>(index: number, then?: N) {
        return then ? then(this, 0, index) : [0, index];
    }
    public isValid(index: number): boolean {
        return this._chunk.isValid(index);
    }
    public get(index: number): T['TValue'] | null {
        return this._chunk.get(index);
    }
    public set(index: number, value: T['TValue'] | null): void {
        this._chunk.set(index, value);
    }
    public indexOf(element: T['TValue'], offset?: number): number {
        return this._chunk.indexOf(element, offset);
    }
}
