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

import { readJSON } from './json';
import { fromReadableStream } from './node';
import { RecordBatch } from '../../recordbatch';
import { readBuffers, readBuffersAsync } from './binary';
import { readRecordBatches, readRecordBatchesAsync, TypeDataLoader } from './vector';
import { Schema } from '../../type';
import { Message } from '../metadata';

export { readJSON, RecordBatch };
export { readBuffers, readBuffersAsync };
export { readRecordBatches, readRecordBatchesAsync };

export function* read(sources: Iterable<Uint8Array | Buffer | string> | object | string) {
    let input: any = sources;
    let messages: Iterable<{ schema: Schema, message: Message, loader: TypeDataLoader }>;
    if (typeof input === 'string') {
        try { input = JSON.parse(input); }
        catch (e) { input = sources; }
    }
    if (!input || typeof input !== 'object') {
        messages = (typeof input === 'string') ? readBuffers([input]) : [];
    } else {
        messages = (typeof input[Symbol.iterator] === 'function') ? readBuffers(input) : readJSON(input);
    }
    yield* readRecordBatches(messages);
}

export async function* readAsync(sources: AsyncIterable<Uint8Array | Buffer | string>) {
    for await (let recordBatch of readRecordBatchesAsync(readBuffersAsync(sources))) {
        yield recordBatch;
    }
}

export async function* readStream(stream: NodeJS.ReadableStream) {
    for await (const recordBatch of readAsync(fromReadableStream(stream))) {
        yield recordBatch as RecordBatch;
    }
}
