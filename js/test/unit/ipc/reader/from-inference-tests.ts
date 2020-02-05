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
import {
    RecordBatchReader,
    RecordBatchFileReader,
    RecordBatchStreamReader,
    AsyncRecordBatchFileReader,
    AsyncRecordBatchStreamReader
} from '../../../Arrow';

/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

for (const table of generateRandomTables([10, 20, 30])) {
    const name = `[\n ${table.schema.fields.join(',\n ')}\n]`;
    describe('RecordBatchReader.from', ((table, name) => () => {
        testFromFile(ArrowIOTestHelper.file(table), name);
        testFromJSON(ArrowIOTestHelper.json(table), name);
        testFromStream(ArrowIOTestHelper.stream(table), name);
    })(table, name));
}

function testFromJSON(io: ArrowIOTestHelper, name: string) {
    describe(`should return a RecordBatchJSONReader (${name})`, () => {
        test(`Uint8Array`, io.buffer((buffer) => {
            const json = bignumJSONParse(`${Buffer.from(buffer)}`);
            const reader = RecordBatchReader.from(json);
            expect(reader.isSync()).toEqual(true);
            expect(reader.isAsync()).toEqual(false);
            expect(reader).toBeInstanceOf(RecordBatchStreamReader);
        }));
    });
}

function testFromFile(io: ArrowIOTestHelper, name: string) {

    describe(`should return a RecordBatchFileReader (${name})`, () => {

        test(`Uint8Array`, io.buffer(syncSync));
        test(`Iterable`, io.iterable(syncSync));
        test('AsyncIterable', io.asyncIterable(asyncSync));
        test('fs.FileHandle', io.fsFileHandle(asyncAsync));
        test('fs.ReadStream', io.fsReadableStream(asyncSync));
        test('stream.Readable', io.nodeReadableStream(asyncSync));
        test('whatwg.ReadableStream', io.whatwgReadableStream(asyncSync));
        test('whatwg.ReadableByteStream', io.whatwgReadableByteStream(asyncSync));

        test(`Promise<Uint8Array>`, io.buffer((source) => asyncSync(Promise.resolve(source))));
        test(`Promise<Iterable>`, io.iterable((source) => asyncSync(Promise.resolve(source))));
        test('Promise<AsyncIterable>', io.asyncIterable((source) => asyncSync(Promise.resolve(source))));
        test('Promise<fs.FileHandle>', io.fsFileHandle((source) => asyncAsync(Promise.resolve(source))));
        test('Promise<fs.ReadStream>', io.fsReadableStream((source) => asyncSync(Promise.resolve(source))));
        test('Promise<stream.Readable>', io.nodeReadableStream((source) => asyncSync(Promise.resolve(source))));
        test('Promise<whatwg.ReadableStream>', io.whatwgReadableStream((source) => asyncSync(Promise.resolve(source))));
        test('Promise<whatwg.ReadableByteStream>', io.whatwgReadableByteStream((source) => asyncSync(Promise.resolve(source))));
    });

    function syncSync(source: any) {
        const reader = RecordBatchReader.from(source);
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    }

    async function asyncSync(source: any) {
        const pending = RecordBatchReader.from(source);
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    }

    async function asyncAsync(source: any) {
        const pending = RecordBatchReader.from(source);
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchFileReader);
    }
}

function testFromStream(io: ArrowIOTestHelper, name: string) {

    describe(`should return a RecordBatchStreamReader (${name})`, () => {

        test(`Uint8Array`, io.buffer(syncSync));
        test(`Iterable`, io.iterable(syncSync));
        test('AsyncIterable', io.asyncIterable(asyncAsync));
        test('fs.FileHandle', io.fsFileHandle(asyncAsync));
        test('fs.ReadStream', io.fsReadableStream(asyncAsync));
        test('stream.Readable', io.nodeReadableStream(asyncAsync));
        test('whatwg.ReadableStream', io.whatwgReadableStream(asyncAsync));
        test('whatwg.ReadableByteStream', io.whatwgReadableByteStream(asyncAsync));

        test(`Promise<Uint8Array>`, io.buffer((source) => asyncSync(Promise.resolve(source))));
        test(`Promise<Iterable>`, io.iterable((source) => asyncSync(Promise.resolve(source))));
        test('Promise<AsyncIterable>', io.asyncIterable((source) => asyncAsync(Promise.resolve(source))));
        test('Promise<fs.FileHandle>', io.fsFileHandle((source) => asyncAsync(Promise.resolve(source))));
        test('Promise<fs.ReadStream>', io.fsReadableStream((source) => asyncAsync(Promise.resolve(source))));
        test('Promise<stream.Readable>', io.nodeReadableStream((source) => asyncAsync(Promise.resolve(source))));
        test('Promise<whatwg.ReadableStream>', io.whatwgReadableStream((source) => asyncAsync(Promise.resolve(source))));
        test('Promise<whatwg.ReadableByteStream>', io.whatwgReadableByteStream((source) => asyncAsync(Promise.resolve(source))));
    });

    function syncSync(source: any) {
        const reader = RecordBatchReader.from(source);
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    }

    async function asyncSync(source: any) {
        const pending = RecordBatchReader.from(source);
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    }

    async function asyncAsync(source: any) {
        const pending = RecordBatchReader.from(source);
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchStreamReader);
    }
}
