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
import { Table } from './table';
import { Vector } from './vector';
import { Schema, Field } from './schema';
import { DataType, Struct } from './type';
import { Chunked } from './vector/chunked';
import { StructVector } from './vector/struct';
import { selectFieldArgs } from './util/args';
import { ensureSameLengthData } from './util/recordbatch';
import { Clonable, Sliceable, Applicative } from './vector';

type VectorMap = { [key: string]: Vector };
type Fields<T extends { [key: string]: DataType }> = (keyof T)[] | Field<T[keyof T]>[];
type ChildData<T extends { [key: string]: DataType }> = (Data<T[keyof T]> | Vector<T[keyof T]>)[];

export interface RecordBatch<T extends { [key: string]: DataType } = any> {
    concat(...others: Vector<Struct<T>>[]): Table<T>;
    slice(begin?: number, end?: number): RecordBatch<T>;
    clone(data: Data<Struct<T>>, children?: Vector[]): RecordBatch<T>;
}

export class RecordBatch<T extends { [key: string]: DataType } = any>
    extends StructVector<T>
    implements Clonable<RecordBatch<T>>,
               Sliceable<RecordBatch<T>>,
               Applicative<Struct<T>, Table<T>> {

    public static from<T extends VectorMap = any>(children: T): RecordBatch<{ [P in keyof T]: T[P]['type'] }>;
    public static from<T extends { [key: string]: DataType } = any>(children: ChildData<T>, fields?: Fields<T>): RecordBatch<T>;
    /** @nocollapse */
    public static from(...args: any[]) {
        return RecordBatch.new(args[0], args[1]);
    }

    public static new<T extends VectorMap = any>(children: T): RecordBatch<{ [P in keyof T]: T[P]['type'] }>;
    public static new<T extends { [key: string]: DataType } = any>(children: ChildData<T>, fields?: Fields<T>): RecordBatch<T>;
    /** @nocollapse */
    public static new<T extends { [key: string]: DataType } = any>(...args: any[]) {
        const [fs, xs] = selectFieldArgs<T>(args);
        const vs = xs.filter((x): x is Vector<T[keyof T]> => x instanceof Vector);
        return new RecordBatch(...ensureSameLengthData(new Schema<T>(fs), vs.map((x) => x.data)));
    }

    protected _schema: Schema;

    constructor(schema: Schema<T>, length: number, children: (Data | Vector)[]);
    constructor(schema: Schema<T>, data: Data<Struct<T>>, children?: Vector[]);
    constructor(...args: any[]) {
        let data: Data<Struct<T>>;
        let schema = args[0] as Schema<T>;
        let children: Vector[] | undefined;
        if (args[1] instanceof Data) {
            [, data, children] = (args as [any, Data<Struct<T>>, Vector<T[keyof T]>[]?]);
        } else {
            const fields = schema.fields as Field<T[keyof T]>[];
            const [, length, childData] = args as [any, number, Data<T[keyof T]>[]];
            data = Data.Struct(new Struct<T>(fields), 0, length, 0, null, childData);
        }
        super(data, children);
        this._schema = schema;
    }

    public clone(data: Data<Struct<T>>, children = this._children) {
        return new RecordBatch<T>(this._schema, data, children);
    }

    public concat(...others: Vector<Struct<T>>[]): Table<T> {
        const schema = this._schema, chunks = Chunked.flatten(this, ...others);
        return new Table(schema, chunks.map(({ data }) => new RecordBatch(schema, data)));
    }

    public get schema() { return this._schema; }
    public get numCols() { return this._schema.fields.length; }

    public select<K extends keyof T = any>(...columnNames: K[]) {
        const nameToIndex = this._schema.fields.reduce((m, f, i) => m.set(f.name as K, i), new Map<K, number>());
        return this.selectAt(...columnNames.map((columnName) => nameToIndex.get(columnName)!).filter((x) => x > -1));
    }
    public selectAt<K extends T[keyof T] = any>(...columnIndices: number[]) {
        const schema = this._schema.selectAt(...columnIndices);
        const childData = columnIndices.map((i) => this.data.childData[i]).filter(Boolean);
        return new RecordBatch<{ [key: string]: K }>(schema, this.length, childData);
    }
}

/**
 * @ignore
 * @private
 */
/* tslint:disable:class-name */
export class _InternalEmptyPlaceholderRecordBatch<T extends { [key: string]: DataType } = any> extends RecordBatch<T> {
    constructor(schema: Schema<T>) {
        super(schema, 0, schema.fields.map((f) => Data.new(f.type, 0, 0, 0)));
    }
}
