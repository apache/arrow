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
import { Table } from './table';
import { Vector } from './vector';
import { Visitor } from './visitor';
import { Schema, Field } from './schema';
import { DataType, Struct, Dictionary } from './type';
import { IndexingProxyHandlerMixin } from './util/proxy';

import { instance as getVisitor } from './visitor/get';
import { instance as setVisitor } from './visitor/set';
import { instance as indexOfVisitor } from './visitor/indexof';
import { instance as toArrayVisitor } from './visitor/toarray';
import { instance as iteratorVisitor } from './visitor/iterator';
import { instance as byteLengthVisitor } from './visitor/bytelength';

/** @ignore */
export interface RecordBatch<T extends { [key: string]: DataType } = any> {
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

/** @ignore */
export class RecordBatch<T extends { [key: string]: DataType } = any> {

    constructor(columns: { [P in keyof T]: Data<T[P]> });
    constructor(schema: Schema<T>, data: undefined | Data<Struct<T>>);
    constructor(...args: any[]) {
        switch (args.length) {
            case 2: {
                [this.schema] = args;
                if (!(this.schema instanceof Schema)) {
                    throw new TypeError('RecordBatch constructor expects a [Schema, Data] pair.');
                }
                [,
                    this.data = makeData({
                        type: new Struct<T>(this.schema.fields),
                        children: this.schema.fields.map((f) => makeData({ type: f.type }))
                    })
                ] = args;
                if (!(this.data instanceof Data)) {
                    throw new TypeError('RecordBatch constructor expects a [Schema, Data] pair.');
                }
                [this.schema, this.data] = ensureSameLengthData<T>(this.schema, this.data.children as Data<T[keyof T]>[]);
                break;
            }
            case 1: {
                const [obj] = args;
                const { fields, children, length } = Object.keys(obj).reduce((memo, name, i) => {
                    memo.children[i] = obj[name];
                    memo.length = Math.max(memo.length, obj[name].length);
                    memo.fields[i] = Field.new({ name, type: obj[name].type });
                    return memo;
                }, {
                    length: 0,
                    fields: new Array<Field<T[keyof T]>>(),
                    children: new Array<Data<T[keyof T]>>(),
                });

                const schema = new Schema<T>(fields);
                const data = makeData({ type: new Struct<T>(fields), length, children });
                [this.schema, this.data] = ensureSameLengthData<T>(schema, data.children as Data<T[keyof T]>[], length);
                break;
            }
            default: throw new TypeError('RecordBatch constructor expects an Object mapping names to child Data, or a [Schema, Data] pair.');
        }
    }

    protected _dictionaries?: Map<number, Vector>;

    /**
     * @summary Get and set elements by index.
     */
    [index: number]: Struct<T>['TValue'] | null;

    public readonly schema: Schema<T>;
    public readonly data: Data<Struct<T>>;

    public get dictionaries() {
        return this._dictionaries || (
               this._dictionaries = new DictionaryCollector().visit(this.data).dictionaries);
    }

    /**
     * @summary The number of columns in this RecordBatch.
     */
    public get numCols() { return this.schema.fields.length; }

    /**
     * @summary The number of rows in this RecordBatch.
     */
    public get numRows() { return this.data.length; }

    /**
     * @summary The number of null rows in this RecordBatch.
     */
    public get nullCount() {
        return this.data.nullCount;
    }

    /**
     * @summary Check whether an element is null.
     * @param index The index at which to read the validity bitmap.
     */
    public isValid(index: number) {
        return this.data.getValid(index);
    }

    /**
     * @summary Get a row by position.
     * @param index The index of the element to read.
     */
    public get(index: number) {
        return getVisitor.visit(this.data, index);
    }

    /**
     * @summary Set a row by position.
     * @param index The index of the element to write.
     * @param value The value to set.
     */
    public set(index: number, value: Struct<T>['TValue']) {
        return setVisitor.visit(this.data, index, value);
    }

    /**
     * @summary Retrieve the index of the first occurrence of a row in an RecordBatch.
     * @param element The row to locate in the RecordBatch.
     * @param offset The index at which to begin the search. If offset is omitted, the search starts at index 0.
     */
    public indexOf(element: Struct<T>['TValue'], offset?: number): number {
        return indexOfVisitor.visit(this.data, element, offset);
    }

    /**
     * @summary Get the size (in bytes) of a row by index.
     * @param index The row index for which to compute the byteLength.
     */
    public getByteLength(index: number): number {
        return byteLengthVisitor.visit(this.data, index);
    }

    /**
     * @summary Iterator for rows in this RecordBatch.
     */
    public [Symbol.iterator]() {
        return iteratorVisitor.visit(this.data);
    }

    /**
     * @summary Return a JavaScript Array of the RecordBatch rows.
     * @returns An Array of RecordBatch rows.
     */
    public toArray() {
        return toArrayVisitor.visit(this.data);
    }

    /**
     * @summary Combines two or more RecordBatch of the same schema.
     * @param others Additional RecordBatch to add to the end of this RecordBatch.
     */
    public concat(...others: RecordBatch<T>[]) {
        return new Table(this.schema, [this, ...others]);
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
            return new Vector([this.data.children[index]]) as Vector<R>;
        }
        return null;
    }

    public select<K extends keyof T = any>(...columnNames: K[]) {
        const nameToIndex = this.schema.fields.reduce((m, f, i) => m.set(f.name as K, i), new Map<K, number>());
        return this.selectAt(...columnNames.map((columnName) => nameToIndex.get(columnName)!).filter((x) => x > -1));
    }

    public selectAt<K extends T[keyof T] = any>(...columnIndices: number[]) {
        const schema = this.schema.selectAt(...columnIndices);
        const children = columnIndices.map((i) => this.data.children[i]).filter(Boolean);
        const subset = makeData({ type: new Struct(schema.fields), length: this.numRows, children });
        return new RecordBatch<{ [key: string]: K }>(schema, subset);
    }

    // Initialize this static property via an IIFE so bundlers don't tree-shake
    // out this logic, but also so we're still compliant with `"sideEffects": false`
    protected static [Symbol.toStringTag] = ((proto: RecordBatch) => {

        (proto as any)._nullCount = -1;
        (proto as any)[Symbol.isConcatSpreadable] = true;

        Object.setPrototypeOf(proto, new Proxy({}, new IndexingProxyHandlerMixin()));

        return 'RecordBatch';
    })(RecordBatch.prototype);
}


/** @ignore */
const ensureSameLengthData = (() => {

    return function ensureSameLengthData<T extends { [key: string]: DataType } = any>(
        schema: Schema<T>,
        columns: Data<T[keyof T]>[],
        length = columns.reduce((l, c) => Math.max(l, c.length), 0)
    ) {
        let child: Data<T[keyof T]>;
        let field: Field<T[keyof T]>;
        let columnIndex = -1;
        const numColumns = columns.length;
        const fields = [...schema.fields];
        const children = [] as Data<T[keyof T]>[];
        const nullCount = ((length + 63) & ~63) >> 3;
        while (++columnIndex < numColumns) {
            field = fields[columnIndex];
            child = columns[columnIndex];
            if (!child || child.length !== length) {
                field = field.clone({ nullable: true });
                child = child
                    ? child._changeLengthAndBackfillNullBitmap(length)
                    : makeData({
                        type: field.type,
                        length,
                        nullCount: length,
                        nullBitmap: new Uint8Array(nullCount)
                    });
            }
            fields[columnIndex] = field;
            children[columnIndex] = child;
        }
        return [
            new Schema<T>(fields),
            makeData({ type: new Struct<T>(fields), length, children })
        ] as [Schema<T>, Data<Struct<T>>];
    };
})();


/** @ignore */
class DictionaryCollector extends Visitor {
    public dictionaries = new Map<number, Vector>();
    public visit<T extends DataType>(data: Data<T>) {
        if (DataType.isDictionary(data.type)) {
            return super.visit(data);
        } else {
            data.children.forEach((child) => this.visit(child));
        }
        return this;
    }
    public visitDictionary<T extends Dictionary>(data: Data<T>) {
        const dictionary = data.dictionary;
        if (dictionary && dictionary.length > 0) {
            this.dictionaries.set(data.type.id, dictionary);
        }
        return this;
    }
}

/**
 * An internal class used by the `RecordBatchReader` and `RecordBatchWriter`
 * implementations to differentiate between a stream with valid zero-length
 * RecordBatches, and a stream with a Schema message, but no RecordBatches.
 * @see https://github.com/apache/arrow/pull/4373
 * @ignore
 * @private
 */
/* eslint-disable @typescript-eslint/naming-convention */
export class _InternalEmptyPlaceholderRecordBatch<T extends { [key: string]: DataType } = any> extends RecordBatch<T> {
    constructor(schema: Schema<T>) {
        const children = schema.fields.map((f) => makeData({ type: f.type }));
        const data = makeData({ type: new Struct<T>(schema.fields), nullCount: 0, children });
        super(schema, data);
    }
}
