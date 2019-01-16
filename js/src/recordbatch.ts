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
import { StructVector } from './vector/struct';
import { Vector as VType } from './interfaces';
import { Chunked } from './vector/chunked';
import { Clonable, Sliceable, Applicative } from './vector';

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

    /** @nocollapse */
    public static from<T extends { [key: string]: DataType } = any>(vectors: VType<T[keyof T]>[], names: (keyof T)[] = []) {
        return new RecordBatch(
            Schema.from(vectors, names),
            vectors.reduce((len, vec) => Math.max(len, vec.length), 0),
            vectors
        );
    }

    protected _schema: Schema;

    constructor(schema: Schema<T>, numRows: number, childData: (Data | Vector)[]);
    constructor(schema: Schema<T>, data: Data<Struct<T>>, children?: Vector[]);
    constructor(...args: any[]) {
        let schema = args[0];
        let data: Data<Struct<T>>;
        let children: Vector[] | undefined;
        if (typeof args[1] === 'number') {
            const fields = schema.fields as Field<T[keyof T]>[];
            const [, numRows, childData] = args as [Schema<T>, number, Data[]];
            data = Data.Struct(new Struct<T>(fields), 0, numRows, 0, null, childData);
        } else {
            [, data, children] = (args as [Schema<T>, Data<Struct<T>>, Vector[]?]);
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
        const fields = this._schema.fields;
        const schema = this._schema.select(...columnNames);
        const childNames = columnNames.reduce((xs, x) => (xs[x] = true) && xs, <any> {});
        const childData = this.data.childData.filter((_, i) => childNames[fields[i].name]);
        const structData = Data.Struct(new Struct(schema.fields), 0, this.length, 0, null, childData);
        return new RecordBatch<{ [P in K]: T[P] }>(schema, structData as Data<Struct<{ [P in K]: T[P] }>>);
    }
}
