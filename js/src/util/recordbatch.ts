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

import { Column } from '../column';
import { Vector } from '../vector';
import { DataType } from '../type';
import { Data, Buffers } from '../data';
import { Schema, Field } from '../schema';
import { Chunked } from '../vector/chunked';
import { RecordBatch } from '../recordbatch';

const noopBuf = new Uint8Array(0);
const nullBufs = (bitmapLength: number) => <unknown> [
    noopBuf, noopBuf, new Uint8Array(bitmapLength), noopBuf
] as Buffers<any>;

/** @ignore */
export function ensureSameLengthData<T extends { [key: string]: DataType } = any>(
    schema: Schema<T>,
    chunks: Data<T[keyof T]>[],
    batchLength = chunks.reduce((l, c) => Math.max(l, c.length), 0)
) {
    let data: Data<T[keyof T]>;
    let field: Field<T[keyof T]>;
    let i = -1, n = chunks.length;
    const fields = [...schema.fields];
    const batchData = [] as Data<T[keyof T]>[];
    const bitmapLength = ((batchLength + 63) & ~63) >> 3;
    while (++i < n) {
        if ((data = chunks[i]) && data.length === batchLength) {
            batchData[i] = data;
        } else {
            (field = fields[i]).nullable || (fields[i] = fields[i].clone({ nullable: true }) as Field<T[keyof T]>);
            batchData[i] = data ? data._changeLengthAndBackfillNullBitmap(batchLength)
                : new Data(field.type, 0, batchLength, batchLength, nullBufs(bitmapLength)) as Data<T[keyof T]>;
        }
    }
    return [new Schema<T>(fields), batchLength, batchData] as [Schema<T>, number, Data<T[keyof T]>[]];
}

/** @ignore */
export function distributeColumnsIntoRecordBatches<T extends { [key: string]: DataType } = any>(columns: Column<T[keyof T]>[]): [Schema<T>, RecordBatch<T>[]] {
    return distributeVectorsIntoRecordBatches<T>(new Schema<T>(columns.map(({ field }) => field)), columns);
}

/** @ignore */
export function distributeVectorsIntoRecordBatches<T extends { [key: string]: DataType } = any>(schema: Schema<T>, vecs: (Vector<T[keyof T]> | Chunked<T[keyof T]>)[]): [Schema<T>, RecordBatch<T>[]] {
    return uniformlyDistributeChunksAcrossRecordBatches<T>(schema, vecs.map((v) => v instanceof Chunked ? v.chunks.map((c) => c.data) : [v.data]));
}

/** @ignore */
function uniformlyDistributeChunksAcrossRecordBatches<T extends { [key: string]: DataType } = any>(schema: Schema<T>, columns: Data<T[keyof T]>[][]): [Schema<T>, RecordBatch<T>[]] {

    const fields = [...schema.fields];
    const batchArgs = [] as [number, Data<T[keyof T]>[]][];
    const memo = { numBatches: columns.reduce((n, c) => Math.max(n, c.length), 0) };
    let sameLength = false, numBatches = 0, batchLength = 0, batchData: Data<T[keyof T]>[];

    while (memo.numBatches-- > 0) {

        [sameLength, batchLength] = columns.reduce((memo, [chunk]) => {
            const [same, batchLength] = memo;
            const chunkLength = chunk ? chunk.length : batchLength;
            isFinite(batchLength) && same && (memo[0] = chunkLength === batchLength);
            memo[1] = Math.min(batchLength, chunkLength);
            return memo;
        }, [true, Number.POSITIVE_INFINITY] as [boolean, number]);

        if (isFinite(batchLength) && !(sameLength && batchLength <= 0)) {
            batchData = distributeChildData(fields, batchLength, columns, memo);
            batchLength > 0 && (batchArgs[numBatches++] = [batchLength, batchData]);
        }
    }
    return [
        schema = new Schema<T>(fields, schema.metadata),
        batchArgs.map((xs) => new RecordBatch(schema, ...xs))
    ];
}

/** @ignore */
function distributeChildData<T extends { [key: string]: DataType } = any>(fields: Field<T[keyof T]>[], batchLength: number, columns: Data<T[keyof T]>[][], memo: { numBatches: number }) {
    let data: Data<T[keyof T]>;
    let field: Field<T[keyof T]>;
    let chunks: Data<T[keyof T]>[];
    let length = 0, i = -1, n = columns.length;
    const batchData = [] as Data<T[keyof T]>[];
    const bitmapLength = ((batchLength + 63) & ~63) >> 3;
    while (++i < n) {
        if ((data = (chunks = columns[i]).shift()!) && ((length = data.length) >= batchLength)) {
            if (length === batchLength) {
                batchData[i] = data;
            } else {
                batchData[i] = data.slice(0, batchLength);
                data = data.slice(batchLength, length - batchLength);
                memo.numBatches = Math.max(memo.numBatches, chunks.push(data));
            }
        } else {
            (field = fields[i]).nullable || (fields[i] = field.clone({ nullable: true }) as Field<T[keyof T]>);
            batchData[i] = data ? data._changeLengthAndBackfillNullBitmap(batchLength)
                : new Data(field.type, 0, batchLength, batchLength, nullBufs(bitmapLength)) as Data<T[keyof T]>;
        }
    }
    return batchData;
}
