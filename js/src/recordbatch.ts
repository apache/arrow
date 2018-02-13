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

import { Schema, Struct, DataType } from './type';
import { flatbuffers } from 'flatbuffers';
import { View, Vector, StructVector } from './vector';
import { Data, NestedData } from './data';

import Long = flatbuffers.Long;

export class RecordBatch extends StructVector {
    public static from(vectors: Vector[]) {
        return new RecordBatch(Schema.from(vectors),
            Math.max(...vectors.map((v) => v.length)),
            vectors
        );
    }
    public readonly schema: Schema;
    public readonly length: number;
    public readonly numCols: number;
    constructor(schema: Schema, data: Data<Struct>, view: View<Struct>);
    constructor(schema: Schema, numRows: Long | number, cols: Data<any> | Vector[]);
    constructor(...args: any[]) {
        if (typeof args[1] !== 'number') {
            const data = args[1] as Data<Struct>;
            super(data, args[2]);
            this.schema = args[0];
            this.length = data.length;
        } else {
            const [schema, numRows, cols] = args;
            const childData: Data<any>[] = new Array(cols.length);
            for (let index = -1, length = cols.length; ++index < length;) {
                const col: Data<any> | Vector = cols[index];
                childData[index] = col instanceof Vector ? col.data : col;
            }
            super(new NestedData(new Struct(schema.fields), numRows, null, childData));
            this.schema = schema;
            this.length = numRows;
        }
        this.numCols = this.schema.fields.length;
    }
    public clone<R extends Struct>(data: Data<R>, view: View<R> = this.view.clone(data)): this {
        return new RecordBatch(this.schema, data as any, view) as any;
    }
    public getChildAt<R extends DataType = DataType>(index: number): Vector<R> | null {
        return index < 0 || index >= this.numCols ? null : super.getChildAt<R>(index);
    }
    public select(...columnNames: string[]) {
        const fields = this.schema.fields;
        const namesToKeep = columnNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
        return new RecordBatch(
            this.schema.select(...columnNames), this.length,
            this.childData.filter((_, i) => namesToKeep[fields[i].name])
        );
    }
}
