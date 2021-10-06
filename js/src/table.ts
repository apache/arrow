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

import { Data, makeData } from './data';
import { Type } from './enum';
import { Vector } from './vector';
import { Field, Schema } from './schema';
import { DataType, Null, Struct } from './type';
import { compareSchemas } from './visitor/typecomparator';
import { distributeVectorsIntoRecordBatches } from './util/recordbatch';

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

import { NumericIndexingProxyHandlerMixin } from './util/proxy';

import { instance as getVisitor } from './visitor/get';
import { instance as setVisitor } from './visitor/set';
import { instance as indexOfVisitor } from './visitor/indexof';
import { instance as toArrayVisitor } from './visitor/toarray';
import { instance as byteLengthVisitor } from './visitor/bytelength';

import { DataProps } from './data';
import { clampRange } from './util/vector';
import { BigIntArray, TypedArray } from './interfaces';
import { RecordBatch, _InternalEmptyPlaceholderRecordBatch } from './recordbatch';

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

    constructor();
    constructor(batches: readonly RecordBatch<T>[]);
    constructor(...columns: { [P in keyof T]: Vector<T[P]> }[]);
    constructor(...columns: { [P in keyof T]: TypedArray | BigIntArray }[]);
    constructor(...columns: { [P in keyof T]: Data<T[P]> | DataProps<T[P]> }[]);
    constructor(schema: Schema<T>, data?: RecordBatch<T> | RecordBatch<T>[]);
    constructor(schema: Schema<T>, data?: RecordBatch<T> | RecordBatch<T>[], offsets?: Uint32Array);
    constructor(...args: any[]) {

        if (args.length === 0) {
            this.batches = [];
            this.schema = new Schema([]);
            this._offsets = new Uint32Array([0]);
            return this;
        }

        let batches: RecordBatch<T>[] = [];
        let schema: Schema<T> | undefined = undefined;
        let offsets: Uint32Array | undefined = undefined;

        if (args[0] instanceof Schema) {
            schema = args.shift() as Schema<T>;
        }
        if (args[args.length - 1] instanceof Uint32Array) {
            offsets = args.pop() as Uint32Array;
        }

        args.flat(Infinity).forEach((x) => {
            if (x instanceof RecordBatch) {
                batches.push(x);
            } else if (x instanceof Data) {
                if (x.type instanceof Struct) {
                    batches.push(new RecordBatch(new Schema(x.type.children), x));
                }
            } else if (x && typeof x === 'object') {
                const keys = Object.keys(x) as (keyof T)[];
                const vecs = keys.map((k) => new Vector(x[k]));
                const s = new Schema(keys.map((k, i) => new Field(String(k), vecs[i].type)));
                [schema, batches] = distributeVectorsIntoRecordBatches(s, vecs);
            }
        });

        schema ??= batches[0]?.schema ?? new Schema([]);

        if (!(schema instanceof Schema)) {
            throw new TypeError('Table constructor expects a [Schema, RecordBatch[]] pair.');
        }

        for (const batch of batches) {
            if (!(batch instanceof RecordBatch)) {
                throw new TypeError('Table constructor expects a [Schema, RecordBatch[]] pair.');
            }
            if (!compareSchemas(schema, batch.schema)) {
                throw new TypeError('Table and inner RecordBatch schemas must be equivalent.');
            }
        }

        this.schema = schema;
        this.batches = batches;
        this._offsets = offsets ?? computeChunkOffsets(this.data);
    }

    declare protected _offsets: Uint32Array;
    declare protected _nullCount: number;

    /**
     * @summary Get and set elements by index.
     */
    [index: number]: Struct<T>['TValue'] | null;

    public readonly schema: Schema<T>;

    /**
     * @summary The contiguous {@link RecordBatch `RecordBatch`} chunks of the Table rows.
     */
    public readonly batches: ReadonlyArray<RecordBatch<T>>;

    /**
     * @summary The contiguous {@link RecordBatch `RecordBatch`} chunks of the Table rows.
     */
    public get data() { return this.batches.map(({ data }) => data); }

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
    public get(index: number): Struct<T>['TValue'] | null { return null; }

    /**
     * @summary Set an element value by position.
     * @param index The index of the element to write.
     * @param value The value to set.
     */
    // @ts-ignore
    public set(index: number, value: Struct<T>['TValue'] | null): void { return; }

    /**
     * @summary Retrieve the index of the first occurrence of a value in an Vector.
     * @param element The value to locate in the Vector.
     * @param offset The index at which to begin the search. If offset is omitted, the search starts at index 0.
     */
    // @ts-ignore
    public indexOf(element: Struct<T>['TValue'], offset?: number): number { return -1; }

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
    public getChild<P extends keyof T>(name: P) {
        return this.getChildAt<T[P]>(this.schema.fields?.findIndex((f) => f.name === name));
    }

    /**
     * @summary Returns a child Vector by index, or null if this Vector has no child at the supplied index.
     * @param index The index of the child to retrieve.
     */
    public getChildAt<R extends DataType = any>(index: number): Vector<R> | null {
        if (index > -1 && index < this.schema.fields.length) {
            return new Vector(this.data.map((data) => data.children[index] as Data<R>));
        }
        return null;
    }

    /**
     * @summary Sets a child Vector by name.
     * @param name The name of the child to overwrite.
     * @returns A new Table with the supplied child for the specified name.
     */
    public setChild<P extends keyof T, R extends DataType>(name: P, child: Vector<R>) {
        return this.setChildAt(this.schema.fields?.findIndex((f) => f.name === name), child) as Table<T & { [K in P]: R }>;
    }

    /**
     * @summary Sets a child Vector by index.
     * @param index The index of the child to overwrite.
     * @returns A new Table with the supplied child at the specified index.
     */
    public setChildAt(index: number, child?: null): Table;
    public setChildAt<R extends DataType = any>(index: number, child: Vector<R>): Table;
    public setChildAt(index: number, child: any) {
        let schema: Schema = this.schema;
        let batches: RecordBatch[] = [...this.batches];
        if (index > -1 && index < this.numCols) {
            if (!child) {
                child = new Vector([makeData({ type: new Null, length: this.numRows })]);
            }
            const fields = schema.fields.slice() as Field<any>[];
            const field = fields[index].clone({ type: child.type });
            const children = this.schema.fields.map((_, i) => this.getChildAt(i)!);
            [fields[index], children[index]] = [field, child];
            [schema, batches] = distributeVectorsIntoRecordBatches(schema, children);
        }
        return new Table(schema, batches);
    }

    /**
     * @summary Construct a new Table containing only specified columns.
     *
     * @param columnNames Names of columns to keep.
     * @returns A new Table of columns matching the specified names.
     */
    public select<K extends keyof T = any>(columnNames: K[]) {
        const nameToIndex = this.schema.fields.reduce((m, f, i) => m.set(f.name as K, i), new Map<K, number>());
        return this.selectAt(columnNames.map((columnName) => nameToIndex.get(columnName)!).filter((x) => x > -1));
    }

    /**
     * @summary Construct a new Table containing only columns at the specified indices.
     *
     * @param columnIndices Indices of columns to keep.
     * @returns A new Table of columns at the specified indices.
     */
    public selectAt<K extends T[keyof T] = any>(columnIndices: number[]) {
        const schema = this.schema.selectAt(columnIndices);
        const data = this.batches.map((batch) => batch.selectAt(columnIndices));
        return new Table<{ [key: string]: K }>(schema, data);
    }

    // Initialize this static property via an IIFE so bundlers don't tree-shake
    // out this logic, but also so we're still compliant with `"sideEffects": false`
    protected static [Symbol.toStringTag] = ((proto: Table) => {
        (proto as any).schema = null;
        (proto as any).batches = [];
        (proto as any)._offsets = new Uint32Array([0]);
        (proto as any)._nullCount = -1;
        (proto as any)[Symbol.isConcatSpreadable] = true;
        (proto as any)['isValid'] = wrapChunkedCall1(isChunkedValid);
        (proto as any)['get'] = wrapChunkedCall1(getVisitor.getVisitFn(Type.Struct));
        (proto as any)['set'] = wrapChunkedCall2(setVisitor.getVisitFn(Type.Struct));
        (proto as any)['indexOf'] = wrapChunkedIndexOf(indexOfVisitor.getVisitFn(Type.Struct));
        (proto as any)['getByteLength'] = wrapChunkedCall1(byteLengthVisitor.getVisitFn(Type.Struct));
        Object.setPrototypeOf(proto, new Proxy({}, new NumericIndexingProxyHandlerMixin<Table>(
            (inst, key) => inst.get(key),
            (inst, key, val) => inst.set(key, val)
        )));
        // Object.setPrototypeOf(proto, new Proxy({}, new IndexingProxyHandlerMixin<Table>(
        //     (inst, key) => inst.get(key),
        //     (inst, key, val) => inst.set(key, val),
        //     (inst, key) => inst.getChild(key),
        //     (inst, key, val) => inst.setChild(key, val),
        // )));
        return 'Table';
    })(Table.prototype);
}
