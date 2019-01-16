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

import { AsyncIterable } from 'ix';

import {
    Table,
    RecordBatchReader,
    RecordBatchWriter,
    RecordBatchFileWriter,
    RecordBatchJSONWriter,
    RecordBatchStreamWriter,
} from '../../../Arrow';

import {
    ArrowIOTestHelper,
    concatBuffersAsync
} from '../helpers';

import {
    validateRecordBatchReader,
    validateAsyncRecordBatchReader,
    validateRecordBatchAsyncIterator
} from '../validate';

(() => {

    if (process.env.TEST_NODE_STREAMS !== 'true') {
        return test('not testing node streams because process.env.TEST_NODE_STREAMS !== "true"', () => {});
    }

    /* tslint:disable */
    const { parse: bignumJSONParse } = require('json-bignum');

    for (const table of generateRandomTables([10, 20, 30])) {

        const file = ArrowIOTestHelper.file(table);
        const json = ArrowIOTestHelper.json(table);
        const stream = ArrowIOTestHelper.stream(table);
        const name = `[\n ${table.schema.fields.join(',\n ')}\n]`;

        describe(`RecordBatchWriter.throughNode (${name})`, () => {

            describe('file', () => {
                describe(`convert`, () => {
                    test('fs.ReadStream', file.fsReadableStream(validateConvert.bind(0, RecordBatchStreamWriter)));
                    test('stream.Readable', file.nodeReadableStream(validateConvert.bind(0, RecordBatchStreamWriter)));
                });
                describe(`through`, () => {
                    test('fs.ReadStream', file.fsReadableStream(validateThrough.bind(0, RecordBatchFileWriter)));
                    test('stream.Readable', file.nodeReadableStream(validateThrough.bind(0, RecordBatchFileWriter)));
                });
            });

            describe('stream', () => {
                describe(`convert`, () => {
                    test('fs.ReadStream', stream.fsReadableStream(validateConvert.bind(0, RecordBatchFileWriter)));
                    test('stream.Readable', stream.nodeReadableStream(validateConvert.bind(0, RecordBatchFileWriter)));
                });
                describe(`through`, () => {
                    test('fs.ReadStream', stream.fsReadableStream(validateThrough.bind(0, RecordBatchStreamWriter)));
                    test('stream.Readable', stream.nodeReadableStream(validateThrough.bind(0, RecordBatchStreamWriter)));
                });
            });

            async function validateConvert(RBWImplementation: typeof RecordBatchWriter, source: NodeJS.ReadableStream) {
                const stream = source
                    .pipe(RecordBatchReader.throughNode())
                    .pipe(RBWImplementation.throughNode());
                const type = RBWImplementation === RecordBatchFileWriter ? 'file' : 'stream';
                await validateAsyncRecordBatchReader(type, 3, await RecordBatchReader.from(stream));
            }

            async function validateThrough(RBWImplementation: typeof RecordBatchWriter, source: NodeJS.ReadableStream) {
                const stream = source
                    .pipe(RecordBatchReader.throughNode())
                    .pipe(RBWImplementation.throughNode())
                    .pipe(RecordBatchReader.throughNode());
                await validateRecordBatchAsyncIterator(3, stream[Symbol.asyncIterator]());
            }
        });

        describe(`toNodeStream (${name})`, () => {

            const wrapArgInPromise = (fn: (p: Promise<any>) => any) => (x: any) => fn(Promise.resolve(x));

            describe(`RecordBatchJSONWriter`, () => {

                const toJSON = (x: any): { schema: any } => bignumJSONParse(`${Buffer.from(x)}`);

                test('Uint8Array', json.buffer((source) => validate(toJSON(source))));
                test('Promise<Uint8Array>', json.buffer((source) => validate(Promise.resolve(toJSON(source)))));

                async function validate(source: { schema: any } | Promise<{ schema: any }>) {
                    const reader = await RecordBatchReader.from(<any> source);
                    const writer = await RecordBatchJSONWriter.writeAll(reader);
                    const buffer = await concatBuffersAsync(writer.toNodeStream());
                    validateRecordBatchReader('json', 3, RecordBatchReader.from(toJSON(buffer)));
                }
            });

            describe(`RecordBatchFileWriter`, () => {

                describe(`sync write/read`, () => {

                    test(`Uint8Array`, file.buffer(validate));
                    test(`Iterable`, file.iterable(validate));
                    test('AsyncIterable', file.asyncIterable(validate));
                    test('fs.FileHandle', file.fsFileHandle(validate));
                    test('fs.ReadStream', file.fsReadableStream(validate));
                    test('stream.Readable', file.nodeReadableStream(validate));
                    test('whatwg.ReadableStream', file.whatwgReadableStream(validate));
                    test('whatwg.ReadableByteStream', file.whatwgReadableByteStream(validate));
                    test('Promise<AsyncIterable>', file.asyncIterable(wrapArgInPromise(validate)));
                    test('Promise<fs.FileHandle>', file.fsFileHandle(wrapArgInPromise(validate)));
                    test('Promise<fs.ReadStream>', file.fsReadableStream(wrapArgInPromise(validate)));
                    test('Promise<stream.Readable>', file.nodeReadableStream(wrapArgInPromise(validate)));
                    test('Promise<ReadableStream>', file.whatwgReadableStream(wrapArgInPromise(validate)));
                    test('Promise<ReadableByteStream>', file.whatwgReadableByteStream(wrapArgInPromise(validate)));

                    async function validate(source: any) {
                        const reader = await RecordBatchReader.from(source);
                        const writer = await RecordBatchFileWriter.writeAll(reader);
                        const stream = await RecordBatchReader.from(writer.toNodeStream());
                        await validateAsyncRecordBatchReader('file', 3, stream);
                    }
                });

                describe(`async write/read`, () => {

                    test(`Uint8Array`, file.buffer(validate));
                    test(`Iterable`, file.iterable(validate));
                    test('AsyncIterable', file.asyncIterable(validate));
                    test('fs.FileHandle', file.fsFileHandle(validate));
                    test('fs.ReadStream', file.fsReadableStream(validate));
                    test('stream.Readable', file.nodeReadableStream(validate));
                    test('whatwg.ReadableStream', file.whatwgReadableStream(validate));
                    test('whatwg.ReadableByteStream', file.whatwgReadableByteStream(validate));
                    test('Promise<AsyncIterable>', file.asyncIterable(wrapArgInPromise(validate)));
                    test('Promise<fs.FileHandle>', file.fsFileHandle(wrapArgInPromise(validate)));
                    test('Promise<fs.ReadStream>', file.fsReadableStream(wrapArgInPromise(validate)));
                    test('Promise<stream.Readable>', file.nodeReadableStream(wrapArgInPromise(validate)));
                    test('Promise<ReadableStream>', file.whatwgReadableStream(wrapArgInPromise(validate)));
                    test('Promise<ReadableByteStream>', file.whatwgReadableByteStream(wrapArgInPromise(validate)));

                    async function validate(source: any) {
                        const writer = new RecordBatchFileWriter();
                        /* no await */ writer.writeAll(await RecordBatchReader.from(source));
                        const reader = await RecordBatchReader.from(writer.toNodeStream());
                        await validateAsyncRecordBatchReader('file', 3, reader);
                    }
                });
            });

            describe(`RecordBatchStreamWriter`, () => {

                describe(`sync write/read`, () => {

                    test(`Uint8Array`, stream.buffer(validate));
                    test(`Iterable`, stream.iterable(validate));
                    test('AsyncIterable', stream.asyncIterable(validate));
                    test('fs.FileHandle', stream.fsFileHandle(validate));
                    test('fs.ReadStream', stream.fsReadableStream(validate));
                    test('stream.Readable', stream.nodeReadableStream(validate));
                    test('whatwg.ReadableStream', stream.whatwgReadableStream(validate));
                    test('whatwg.ReadableByteStream', stream.whatwgReadableByteStream(validate));
                    test('Promise<AsyncIterable>', stream.asyncIterable(wrapArgInPromise(validate)));
                    test('Promise<fs.FileHandle>', stream.fsFileHandle(wrapArgInPromise(validate)));
                    test('Promise<fs.ReadStream>', stream.fsReadableStream(wrapArgInPromise(validate)));
                    test('Promise<stream.Readable>', stream.nodeReadableStream(wrapArgInPromise(validate)));
                    test('Promise<ReadableStream>', stream.whatwgReadableStream(wrapArgInPromise(validate)));
                    test('Promise<ReadableByteStream>', stream.whatwgReadableByteStream(wrapArgInPromise(validate)));

                    async function validate(source: any) {
                        const reader = await RecordBatchReader.from(source);
                        const writer = await RecordBatchStreamWriter.writeAll(reader);
                        const stream = await RecordBatchReader.from(writer.toNodeStream());
                        await validateAsyncRecordBatchReader('stream', 3, stream);
                    }
                });

                describe(`async write/read`, () => {

                    test(`Uint8Array`, stream.buffer(validate));
                    test(`Iterable`, stream.iterable(validate));
                    test('AsyncIterable', stream.asyncIterable(validate));
                    test('fs.FileHandle', stream.fsFileHandle(validate));
                    test('fs.ReadStream', stream.fsReadableStream(validate));
                    test('stream.Readable', stream.nodeReadableStream(validate));
                    test('whatwg.ReadableStream', stream.whatwgReadableStream(validate));
                    test('whatwg.ReadableByteStream', stream.whatwgReadableByteStream(validate));
                    test('Promise<AsyncIterable>', stream.asyncIterable(wrapArgInPromise(validate)));
                    test('Promise<fs.FileHandle>', stream.fsFileHandle(wrapArgInPromise(validate)));
                    test('Promise<fs.ReadStream>', stream.fsReadableStream(wrapArgInPromise(validate)));
                    test('Promise<stream.Readable>', stream.nodeReadableStream(wrapArgInPromise(validate)));
                    test('Promise<ReadableStream>', stream.whatwgReadableStream(wrapArgInPromise(validate)));
                    test('Promise<ReadableByteStream>', stream.whatwgReadableByteStream(wrapArgInPromise(validate)));

                    async function validate(source: any) {
                        const writer = new RecordBatchStreamWriter();
                        /* no await */ writer.writeAll(await RecordBatchReader.from(source));
                        const reader = await RecordBatchReader.from(writer.toNodeStream());
                        await validateAsyncRecordBatchReader('stream', 3, reader);
                    }
                });
            });
        });
    }

    describe(`RecordBatchStreamWriter.throughNode`, () => {

        const sleep = (n: number) => new Promise((r) => setTimeout(r, n));

        it(`should write a stream of tables to the same output stream`, async () => {

            const tables = [] as Table[];
            const writer = RecordBatchStreamWriter.throughNode({ autoDestroy: false });
            const stream = AsyncIterable
                .from(generateRandomTables([10, 20, 30]))
                // insert some asynchrony
                .tap({ async next(table) { tables.push(table); await sleep(1); } })
                .pipe(writer);
                
            for await (const reader of RecordBatchReader.readAll(stream)) {
                const sourceTable = tables.shift()!;
                const streamTable = await Table.from(reader);
                expect(streamTable).toEqualTable(sourceTable);
            }

            expect(tables.length).toBe(0);
            expect(writer.readable).toBe(false);
            expect((writer as any).destroyed).toBe(true);
        });

        it(`should write a stream of record batches to the same output stream`, async () => {

            const tables = [] as Table[];
            const writer = RecordBatchStreamWriter.throughNode({ autoDestroy: false });
            const stream = AsyncIterable
                .from(generateRandomTables([10, 20, 30]))
                // insert some asynchrony
                .tap({ async next(table) { tables.push(table); await sleep(1); } })
                .flatMap((table) => AsyncIterable.as(table.chunks))
                .pipe(writer);
                
            for await (const reader of RecordBatchReader.readAll(stream)) {
                const sourceTable = tables.shift()!;
                const streamTable = await Table.from(reader);
                expect(streamTable).toEqualTable(sourceTable);
            }

            expect(tables.length).toBe(0);
            expect(writer.readable).toBe(false);
            expect((writer as any).destroyed).toBe(true);
        });

    });
})();
