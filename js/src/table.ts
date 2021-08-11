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

import { Data } from './data';
import { Type } from './enum';
import { Vector } from './vector';
import { Schema } from './schema';
import { DataType, Struct } from './type';
import { compareSchemas } from './visitor/typecomparator';

import {
    ChunkedIterator,
    isChunkedValid,
    computeChunkOffsets,
    computeChunkNullCounts,
    wrapChunkedCall1,
    wrapChunkedCall2,
    wrapChunkedIndexOf,
    sliceChunks,
} from './util/chunk';

import { IndexingProxyHandlerMixin } from './util/proxy';

import { instance as getVisitor } from './visitor/get';
import { instance as setVisitor } from './visitor/set';
import { instance as indexOfVisitor } from './visitor/indexof';
import { instance as toArrayVisitor } from './visitor/toarray';
import { instance as byteLengthVisitor } from './visitor/bytelength';

import { RecordBatch, _InternalEmptyPlaceholderRecordBatch } from './recordbatch';
import { clampRange } from './util/vector';

/** @ignore */
export interface Table<T extends { [key: string]: DataType } = any> {
    ///
    // Virtual properties for the TypeScript compiler.
    // These do not exist at runtime.
    ///
    readonly TType: Struct<T>;
    readonly TArray: Struct<T>['TArray'];
    readonly TValue: Struct<T>['TValue'];

    /**
     * @see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Symbol/isConcatSpreadable
     */
    [Symbol.isConcatSpreadable]: true;
}

export class Table<T extends { [key: string]: DataType } = any> {

    constructor(columns: { [P in keyof T]: Vector<T[P]> });
    constructor(schema: Schema<T>, data?: RecordBatch<T> | RecordBatch<T>[]);
    constructor(schema: Schema<T>, data?: RecordBatch<T> | RecordBatch<T>[], offsets?: Uint32Array);
    constructor(...args: any[]) {

        if (args.length === 0) {
            args = [new Schema([])];
        }

        if (args.length === 1 && !(args[0] instanceof Schema)) {
            const [obj] = args as [{ [P in keyof T]: Vector<T[P]> }];
            const batches = Object.keys(obj).reduce((batches, name: keyof T) => {
                obj[name].data.forEach((data, batchIndex) => {
                    if (!batches[batchIndex]) {
                        batches[batchIndex] = {} as { [P in keyof T]: Data<T[P]> };
                    }
                    batches[batchIndex][name] = data;
                });
                return batches;
            }, new Array<{ [P in keyof T]: Data<T[P]> }>())
            .map((data) => new RecordBatch<T>(data));

            args = [batches[0].schema, batches];
        }

        let [schema, data, offsets] = args;

        if (!(schema instanceof Schema)) {
            throw new TypeError('Table constructor expects a [Schema, RecordBatch[]] pair.');
        }

        this.schema = schema;

        [, data = [new _InternalEmptyPlaceholderRecordBatch(schema)]] = args;

        const batches: RecordBatch<T>[] = Array.isArray(data) ? data : [data];

        batches.forEach((batch: RecordBatch<T>) => {
            if (!(batch instanceof RecordBatch)) {
                throw new TypeError('Table constructor expects a [Schema, RecordBatch[]] pair.');
            }
            if (!compareSchemas(this.schema, batch.schema)) {
                throw new TypeError('Table and all RecordBatch schemas must be equivalent.');
            }
        }, new Struct(schema.fields));

        this.data = batches.map(({ data }) => data);
        this._offsets = offsets ?? computeChunkOffsets(this.data);
    }

    protected _offsets!: Uint32Array;
    protected _nullCount!: number;

    /**
     * @summary Get and set elements by index.
     */
    [index: number]: Struct<T>['TValue'] | null;

    public readonly schema!: Schema<T>;

    /**
     * @summary The contiguous {@link RecordBatch `RecordBatch`} chunks of the Table rows.
     */
    public readonly data!: ReadonlyArray<Data<Struct<T>>>;

    /**
     * @summary The number of columns in this Table.
     */
    public get numCols() { return this.schema.fields.length; }

     /**
      * @summary The number of rows in this Table.
      */
    public get numRows() {
        return this.data.reduce((numRows, data) => numRows + data.length, 0);
    }

    /**
     * @summary The number of null rows in this Table.
     */
     public get nullCount() {
        if (this._nullCount === -1) {
            this._nullCount = computeChunkNullCounts(this.data);
        }
        return this._nullCount;
    }

    /**
     * @summary Check whether an element is null.
     * @param index The index at which to read the validity bitmap.
     */
    // @ts-ignore
    public isValid(index: number): boolean { return false; }

    /**
     * @summary Get an element value by position.
     * @param index The index of the element to read.
     */
    // @ts-ignore
    public get(index: number): T['TValue'] | null { return null; }

    /**
     * @summary Set an element value by position.
     * @param index The index of the element to write.
     * @param value The value to set.
     */
    // @ts-ignore
    public set(index: number, value: T['TValue'] | null): void { return; }

    /**
     * @summary Retrieve the index of the first occurrence of a value in an Vector.
     * @param element The value to locate in the Vector.
     * @param offset The index at which to begin the search. If offset is omitted, the search starts at index 0.
     */
    // @ts-ignore
    public indexOf(element: T['TValue'], offset?: number): number { return -1; }

    /**
     * @summary Get the size in bytes of an element by index.
     * @param index The index at which to get the byteLength.
     */
    // @ts-ignore
    public getByteLength(index: number): number { return 0; }

    /**
     * @summary Iterator for rows in this Table.
     */
    public [Symbol.iterator]() {
        return new ChunkedIterator(this.data);
    }

    /**
     * @summary Return a JavaScript Array of the Table rows.
     * @returns An Array of Table rows.
     */
    public toArray() {
        return this.data.reduce((ary, data) =>
            ary.concat(toArrayVisitor.visit(data)),
            new Array<Struct<T>['TValue']>()
        );
    }

    /**
     * @summary Combines two or more Tables of the same schema.
     * @param others Additional Tables to add to the end of this Tables.
     */
    public concat(...others: Table<T>[]) {
        const schema = this.schema;
        const data = this.data.concat(others.flatMap(({ data }) => data));
        return new Table(schema, data.map((data) => new RecordBatch(schema, data)));
    }

    /**
     * Return a zero-copy sub-section of this Table.
     * @param start The beginning of the specified portion of the Table.
     * @param end The end of the specified portion of the Table. This is exclusive of the element at the index 'end'.
     */
    public slice(begin?: number, end?: number): Table<T> {
        const schema = this.schema;
        [begin, end] = clampRange({ length: this.numRows }, begin, end);
        const data = sliceChunks(this.data, this._offsets, begin, end);
        return new Table(schema, data.map((chunk) => new RecordBatch(schema, chunk)));
    }

    /**
     * @summary Returns a child Vector by name, or null if this Vector has no child with the given name.
     * @param name The name of the child to retrieve.
     */
    public getChild<R extends keyof T['TChildren']>(name: R) {
        return this.getChildAt(this.schema.fields?.findIndex((f) => f.name === name));
    }

    /**
     * @summary Returns a child Vector by index, or null if this Vector has no child at the supplied index.
     * @param index The index of the child to retrieve.
     */
    public getChildAt<R extends DataType = any>(index: number): Vector<R> | null {
        if (index > -1 && index < this.schema.fields.length) {
            return new Vector(this.data.map(({ children }) => children[index] as Data<R>));
        }
        return null;
    }

    // Initialize this static property via an IIFE so bundlers don't tree-shake
    // out this logic, but also so we're still compliant with `"sideEffects": false`
    protected static [Symbol.toStringTag] = ((proto: Table) => {
        (proto as any)._nullCount = -1;
        (proto as any)[Symbol.isConcatSpreadable] = true;
        (proto as any)['isValid'] = wrapChunkedCall1(isChunkedValid);
        (proto as any)['get'] = wrapChunkedCall1(getVisitor.getVisitFn(Type.Struct));
        (proto as any)['set'] = wrapChunkedCall2(setVisitor.getVisitFn(Type.Struct));
        (proto as any)['indexOf'] = wrapChunkedIndexOf(indexOfVisitor.getVisitFn(Type.Struct));
        (proto as any)['getByteLength'] = wrapChunkedCall1(byteLengthVisitor.getVisitFn(Type.Struct));
        Object.setPrototypeOf(proto, new Proxy({}, new IndexingProxyHandlerMixin()));
        return 'Table';
    })(Table.prototype);
}
