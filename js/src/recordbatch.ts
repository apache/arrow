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

import { Schema, Struct } from './type';
import { flatbuffers } from 'flatbuffers';
import { View, Vector, StructVector } from './vector';
import { Data, NestedData, ChunkedData } from './data';

import Long = flatbuffers.Long;

export class RecordBatch extends StructVector {
    public static from(vectors: Vector[]) {
        return new RecordBatch(Schema.from(vectors),
            Math.max(...vectors.map((v) => v.length)),
            vectors
        );
    }
    public readonly schema: Schema;
    public readonly numCols: number;
    public readonly numRows: number;
    public readonly columns: Vector<any>[];
    constructor(schema: Schema, data: Data<Struct>, view: View<Struct>);
    constructor(schema: Schema, numRows: Long | number, cols: Data<any> | Vector[]);
    constructor(...args: any[]) {
        if (typeof args[1] !== 'number') {
            const data = args[1] as Data<Struct>;
            super(data, args[2]);
            this.schema = args[0];
            this.numRows = data.length;
            this.numCols = this.schema.fields.length;
            this.columns = data instanceof ChunkedData
                ? data.childVectors
                : data.childData.map((col) => Vector.create(col));
        } else {
            const [schema, numRows, cols] = args;
            const columns: Vector<any>[] = new Array(cols.length);
            const columnsData: Data<any>[] = new Array(cols.length);
            for (let index = -1, length = cols.length; ++index < length;) {
                const col: Data<any> | Vector = cols[index];
                if (col instanceof Vector) {
                    columnsData[index] = (columns[index] = col as Vector).data;
                } else {
                    columns[index] = Vector.create(columnsData[index] = col);
                }
            }
            super(new NestedData(new Struct(schema.fields), numRows, null, columnsData));
            this.schema = schema;
            this.columns = columns;
            this.numRows = numRows;
            this.numCols = schema.fields.length;
        }
    }
    public clone(data: Data<Struct>, view: View<Struct> = this.view.clone(data)): this {
        return new RecordBatch(this.schema, data, view) as this;
    }
    public select(...columnNames: string[]) {
        const fields = this.schema.fields;
        const namesToKeep = columnNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
        return new RecordBatch(
            this.schema.select(...columnNames), this.numRows,
            this.columns.filter((_, index) => namesToKeep[fields[index].name])
        );
    }
}
