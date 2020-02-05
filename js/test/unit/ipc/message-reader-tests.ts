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

// import * as fs from 'fs';
import {
    generateRandomTables,
    // generateDictionaryTables
} from '../../data/tables';

import { ArrowIOTestHelper } from './helpers';
import { MessageReader, AsyncMessageReader } from '../../Arrow';

for (const table of generateRandomTables([10, 20, 30])) {

    const io = ArrowIOTestHelper.stream(table);
    const name = `[\n ${table.schema.fields.join(',\n ')}\n]`;
    const numMessages = table.chunks.reduce((numMessages, batch) => {
        return numMessages +
            /* recordBatch message */ 1 +
            /* dictionary messages */ batch.dictionaries.size;
    }, /* schema message */ 1);

    const validate = validateMessageReader.bind(0, numMessages);
    const validateAsync = validateAsyncMessageReader.bind(0, numMessages);

    describe(`MessageReader (${name})`, () => {
        describe(`should read all Messages`, () => {
            test(`Uint8Array`, io.buffer(validate));
            test(`Iterable`, io.iterable(validate));
        });
    });

    describe(`AsyncMessageReader (${name})`, () => {
        describe(`should read all Messages`, () => {
            test('AsyncIterable', io.asyncIterable(validateAsync));
            test('fs.FileHandle', io.fsFileHandle(validateAsync));
            test('fs.ReadStream', io.fsReadableStream(validateAsync));
            test('stream.Readable', io.nodeReadableStream(validateAsync));
            test('whatwg.ReadableStream', io.whatwgReadableStream(validateAsync));
            test('whatwg.ReadableByteStream', io.whatwgReadableByteStream(validateAsync));
        });
    });
}

export function validateMessageReader(numMessages: number, source: any) {
    const reader = new MessageReader(source);
    let index = 0;
    for (let message of reader) {

        if (index === 0) {
            expect(message.isSchema()).toBe(true);
            expect(message.bodyLength).toBe(0);
        } else {
            expect(message.isSchema()).toBe(false);
            expect(message.isRecordBatch() || message.isDictionaryBatch()).toBe(true);
        }

        try {
            expect(message.bodyLength % 8).toBe(0);
        } catch (e) { throw new Error(`bodyLength: ${e}`); }

        const body = reader.readMessageBody(message.bodyLength);
        expect(body).toBeInstanceOf(Uint8Array);
        expect(body.byteLength).toBe(message.bodyLength);
        expect(index++).toBeLessThan(numMessages);
    }
    expect(index).toBe(numMessages);
    reader.return();
}

export async function validateAsyncMessageReader(numMessages: number, source: any) {
    const reader = new AsyncMessageReader(source);
    let index = 0;
    for await (let message of reader) {

        if (index === 0) {
            expect(message.isSchema()).toBe(true);
            expect(message.bodyLength).toBe(0);
        } else {
            expect(message.isSchema()).toBe(false);
            expect(message.isRecordBatch() || message.isDictionaryBatch()).toBe(true);
        }

        try {
            expect(message.bodyLength % 8).toBe(0);
        } catch (e) { throw new Error(`bodyLength: ${e}`); }

        const body = await reader.readMessageBody(message.bodyLength);
        expect(body).toBeInstanceOf(Uint8Array);
        expect(body.byteLength).toBe(message.bodyLength);
        expect(index++).toBeLessThan(numMessages);
    }
    expect(index).toBe(numMessages);
    await reader.return();
}
