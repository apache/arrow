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

import '../../jest-extensions';

import {
    Schema,
    RecordBatch,
    RecordBatchReader,
    RecordBatchFileReader,
    RecordBatchStreamReader,
} from '../../Arrow';

export function validateRecordBatchReader<T extends RecordBatchFileReader | RecordBatchStreamReader>(type: 'json' | 'file' | 'stream', numBatches: number, r: T) {
    const reader = r.open();
    expect(reader).toBeInstanceOf(RecordBatchReader);
    expect(type === 'file' ? reader.isFile() : reader.isStream()).toBe(true);
    expect(reader.schema).toBeInstanceOf(Schema);
    validateRecordBatchIterator(numBatches, reader[Symbol.iterator]());
    expect(reader.closed).toBe(reader.autoDestroy);
    return reader;
}

export async function validateAsyncRecordBatchReader<T extends RecordBatchReader>(type: 'json' | 'file' | 'stream', numBatches: number, r: T) {
    const reader = await r.open();
    expect(reader).toBeInstanceOf(RecordBatchReader);
    expect(reader.schema).toBeInstanceOf(Schema);
    expect(type === 'file' ? reader.isFile() : reader.isStream()).toBe(true);
    await validateRecordBatchAsyncIterator(numBatches, reader[Symbol.asyncIterator]());
    expect(reader.closed).toBe(reader.autoDestroy);
    return reader;
}

export function validateRecordBatchIterator(numBatches: number, iterator: Iterable<RecordBatch> | IterableIterator<RecordBatch>) {
    let i = 0;
    try {
        for (const recordBatch of iterator) {
            expect(recordBatch).toBeInstanceOf(RecordBatch);
            expect(i++).toBeLessThan(numBatches);
        }
    } catch (e) { throw new Error(`${i}: ${e}`); }
    expect(i).toBe(numBatches);
    if (typeof (iterator as any).return === 'function') {
        (iterator as any).return();
    }
}

export async function validateRecordBatchAsyncIterator(numBatches: number, iterator: AsyncIterable<RecordBatch> | AsyncIterableIterator<RecordBatch>) {
    let i = 0;
    try {
        for await (const recordBatch of iterator) {
            expect(recordBatch).toBeInstanceOf(RecordBatch);
            expect(i++).toBeLessThan(numBatches);
        }
    } catch (e) { throw new Error(`${i}: ${e}`); }
    expect(i).toBe(numBatches);
    if (typeof (iterator as any).return === 'function') {
        await (iterator as any).return();
    }
}
