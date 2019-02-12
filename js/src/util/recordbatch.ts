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
import { Column } from '../column';
import { Schema } from '../schema';
import { Vector } from '../vector';
import { DataType } from '../type';
import { Chunked } from '../vector/chunked';
import { RecordBatch } from '../recordbatch';

/** @ignore */
export function alignChunkLengths<T extends { [key: string]: DataType; } = any>(schema: Schema, chunks: Data<T[keyof T]>[], length = chunks.reduce((l, c) => Math.max(l, c.length), 0)) {
    const bitmapLength = ((length + 63) & ~63) >> 3;
    return chunks.map((chunk, idx) => {
        const chunkLength = chunk ? chunk.length : 0;
        if (chunkLength === length) { return chunk; }
        const field = schema.fields[idx];
        if (!field.nullable) { schema.fields[idx] = field.clone({ nullable: true }); }
        return chunk
            ? chunk._changeLengthAndBackfillNullBitmap(length)
            : new Data(field.type, 0, length, length, [,, new Uint8Array(bitmapLength)]);
    });
}

/** @ignore */
export function distributeColumnsIntoRecordBatches<T extends { [key: string]: DataType; } = any>(columns: Column<T[keyof T]>[]): [Schema<T>, RecordBatch<T>[]] {
    return distributeVectorsIntoRecordBatches<T>(new Schema<T>(columns.map(({ field }) => field)), columns);
}

/** @ignore */
export function distributeVectorsIntoRecordBatches<T extends { [key: string]: DataType; } = any>(schema: Schema<T>, vecs: (Vector<T[keyof T]> | Chunked<T[keyof T]>)[]): [Schema<T>, RecordBatch<T>[]] {
    return uniformlyDistributeChunksAcrossRecordBatches<T>(schema, vecs.map((v) => v instanceof Chunked ? v.chunks.map((c) => c.data) : [v.data]));
}

/** @ignore */
export function uniformlyDistributeChunksAcrossRecordBatches<T extends { [key: string]: DataType; } = any>(schema: Schema<T>, chunks: Data<T[keyof T]>[][]): [Schema<T>, RecordBatch<T>[]] {

    let recordBatchesLen = 0;
    const recordBatches = [] as RecordBatch<T>[];
    const memo = { numChunks: chunks.reduce((n, c) => Math.max(n, c.length), 0) };

    for (let chunkIndex = -1; ++chunkIndex < memo.numChunks;) {

        const [sameLength, batchLength] = chunks.reduce((memo, chunks) => {
            const chunk = chunks[chunkIndex];
            const [sameLength, batchLength] = memo;
            memo[1] = Math.min(batchLength, chunk ? chunk.length : batchLength);
            sameLength && isFinite(batchLength) && (memo[0] = batchLength === memo[1]);
            return memo;
        }, [true, Number.POSITIVE_INFINITY] as [boolean, number]);

        if (!isFinite(batchLength) || (sameLength && batchLength <= 0)) { continue; }

        recordBatches[recordBatchesLen++] = new RecordBatch(schema, batchLength,
            sameLength ? gatherChunksSameLength(schema, chunkIndex, batchLength, chunks) :
                         gatherChunksDiffLength(schema, chunkIndex, batchLength, chunks, memo));
    }
    return [schema, recordBatches];
}

/** @ignore */
function gatherChunksSameLength(schema: Schema, chunkIndex: number, length: number, chunks: Data[][]) {
    const bitmapLength = ((length + 63) & ~63) >> 3;
    return chunks.map((chunks, idx) => {
        const chunk = chunks[chunkIndex];
        if (chunk) { return chunk; }
        const field = schema.fields[idx];
        if (!field.nullable) { schema.fields[idx] = field.clone({ nullable: true }); }
        return new Data(field.type, 0, length, length, [,, new Uint8Array(bitmapLength)]);
    });
}

/** @ignore */
function gatherChunksDiffLength(schema: Schema, chunkIndex: number, length: number, chunks: Data[][], memo: { numChunks: number }) {
    const bitmapLength = ((length + 63) & ~63) >> 3;
    return chunks.map((chunks, idx) => {
        const chunk = chunks[chunkIndex];
        const chunkLength = chunk ? chunk.length : 0;
        if (chunkLength === length) { return chunk; }
        if (chunkLength > length) {
            memo.numChunks = Math.max(memo.numChunks, chunks.length + 1);
            chunks.splice(chunkIndex + 1, 0, chunk.slice(length, chunkLength - length));
            return chunk.slice(0, length);
        }
        const field = schema.fields[idx];
        if (!field.nullable) { schema.fields[idx] = field.clone({ nullable: true }); }
        return chunk
            ? chunk._changeLengthAndBackfillNullBitmap(length)
            : new Data(field.type, 0, length, length, [,, new Uint8Array(bitmapLength)]);
    });
}
