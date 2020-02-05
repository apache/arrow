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

import {
    generateRandomTables,
    // generateDictionaryTables
} from '../../../data/tables';
import { ArrowIOTestHelper } from '../helpers';
import { toArray } from 'ix/asynciterable/toarray';

import {
    validateRecordBatchReader,
    validateAsyncRecordBatchReader
} from '../validate';

import {
    RecordBatchReader,
    RecordBatchFileReader,
    AsyncRecordBatchFileReader
} from '../../../Arrow';

for (const table of generateRandomTables([10, 20, 30])) {

    const io = ArrowIOTestHelper.file(table);
    const name = `[\n ${table.schema.fields.join(',\n ')}\n]`;

    const validate = (source: any) => { validateRecordBatchReader('file', 3, RecordBatchReader.from(source)); };
    const validateAsync = async (source: any) => { await validateAsyncRecordBatchReader('file', 3, await RecordBatchReader.from(source)); };
    const validateAsyncWrapped = async (source: any) => { await validateAsyncRecordBatchReader('file', 3, await RecordBatchReader.from(Promise.resolve(source))); };

    describe(`RecordBatchFileReader (${name})`, () => {
        describe(`should read all RecordBatches`, () => {
            test(`Uint8Array`, io.buffer(validate));
            test(`Iterable`, io.iterable(validate));
        });
        describe(`should allow random access to record batches after iterating when autoDestroy=false`, () => {
            test(`Uint8Array`, io.buffer(validateRandomAccess));
            test(`Iterable`, io.iterable(validateRandomAccess));
        });
    });

    describe(`AsyncRecordBatchFileReader (${name})`, () => {
        describe(`should read all RecordBatches`, () => {

            test('AsyncIterable', io.asyncIterable(validateAsync));
            test('fs.FileHandle', io.fsFileHandle(validateAsync));
            test('fs.ReadStream', io.fsReadableStream(validateAsync));
            test('stream.Readable', io.nodeReadableStream(validateAsync));
            test('whatwg.ReadableStream', io.whatwgReadableStream(validateAsync));
            test('whatwg.ReadableByteStream', io.whatwgReadableByteStream(validateAsync));

            test('Promise<AsyncIterable>', io.asyncIterable(validateAsyncWrapped));
            test('Promise<fs.FileHandle>', io.fsFileHandle(validateAsyncWrapped));
            test('Promise<fs.ReadStream>', io.fsReadableStream(validateAsyncWrapped));
            test('Promise<stream.Readable>', io.nodeReadableStream(validateAsyncWrapped));
            test('Promise<ReadableStream>', io.whatwgReadableStream(validateAsyncWrapped));
            test('Promise<ReadableByteStream>', io.whatwgReadableByteStream(validateAsyncWrapped));
        });

        describe(`should allow random access to record batches after iterating when autoDestroy=false`, () => {

            test('AsyncIterable', io.asyncIterable(validateRandomAccessAsync));
            test('fs.FileHandle', io.fsFileHandle(validateRandomAccessAsync));
            test('fs.ReadStream', io.fsReadableStream(validateRandomAccessAsync));
            test('stream.Readable', io.nodeReadableStream(validateRandomAccessAsync));
            test('whatwg.ReadableStream', io.whatwgReadableStream(validateRandomAccessAsync));
            test('whatwg.ReadableByteStream', io.whatwgReadableByteStream(validateRandomAccessAsync));

            test('Promise<AsyncIterable>', io.asyncIterable(validateRandomAccessAsync));
            test('Promise<fs.FileHandle>', io.fsFileHandle(validateRandomAccessAsync));
            test('Promise<fs.ReadStream>', io.fsReadableStream(validateRandomAccessAsync));
            test('Promise<stream.Readable>', io.nodeReadableStream(validateRandomAccessAsync));
            test('Promise<ReadableStream>', io.whatwgReadableStream(validateRandomAccessAsync));
            test('Promise<ReadableByteStream>', io.whatwgReadableByteStream(validateRandomAccessAsync));
        });
    });
}

function validateRandomAccess(source: any) {
    const reader = RecordBatchReader.from(source) as RecordBatchFileReader;
    const schema = reader.open({ autoDestroy: false }).schema;
    const batches = [...reader];
    expect(reader.closed).toBe(false);
    expect(reader.schema).toBe(schema);
    while (batches.length > 0) {
        const expected = batches.pop()!;
        const actual = reader.readRecordBatch(batches.length);
        expect(actual).toEqualRecordBatch(expected);
    }
    reader.cancel();
    expect(reader.closed).toBe(true);
    expect(reader.schema).toBeUndefined();
}

async function validateRandomAccessAsync(source: any) {
    const reader = (await RecordBatchReader.from(source)) as AsyncRecordBatchFileReader;
    const schema = (await reader.open({ autoDestroy: false })).schema;
    const batches = await toArray(reader);
    expect(reader.closed).toBe(false);
    expect(reader.schema).toBe(schema);
    while (batches.length > 0) {
        const expected = batches.pop()!;
        const actual = await reader.readRecordBatch(batches.length);
        expect(actual).toEqualRecordBatch(expected);
    }
    await reader.cancel();
    expect(reader.closed).toBe(true);
    expect(reader.schema).toBeUndefined();
}
