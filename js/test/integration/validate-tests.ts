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

import '../jest-extensions';

import * as fs from 'fs';
import * as path from 'path';

import Arrow from '../Arrow';
import { zip } from 'ix/iterable/zip';
import { toArray } from 'ix/iterable/toarray';

import { AsyncIterableX } from 'ix/asynciterable/asynciterablex';
import { zip as zipAsync } from 'ix/asynciterable/zip';
import { toArray as toArrayAsync } from 'ix/asynciterable/toarray';

/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

const { Table, read } = Arrow;
const { fromReadableStream, readBuffersAsync, readRecordBatchesAsync } = Arrow;

if (!process.env.JSON_PATHS || !process.env.ARROW_PATHS) {
    throw new Error('Integration tests need paths to both json and arrow files');
}

function resolvePathArgs(paths: string) {
    let pathsArray = JSON.parse(paths) as string | string[];
    return (Array.isArray(pathsArray) ? pathsArray : [pathsArray])
        .map((p) => path.resolve(p))
        .map((p) => {
            if (fs.existsSync(p)) {
                return p;
            }
            console.error(`Could not find file "${p}"`);
            return undefined;
        });
}

const getOrReadFileBuffer = ((cache: any) => function getFileBuffer(path: string, ...args: any[]) {
    return cache[path] || (cache[path] = fs.readFileSync(path, ...args));
})({});

const jsonAndArrowPaths = toArray(zip(
    resolvePathArgs(process.env.JSON_PATHS!),
    resolvePathArgs(process.env.ARROW_PATHS!)
))
.filter(([p1, p2]) => p1 !== undefined && p2 !== undefined) as [string, string][];

describe(`Integration`, () => {
    for (const [jsonFilePath, arrowFilePath] of jsonAndArrowPaths) {
        let { name, dir } = path.parse(arrowFilePath);
        dir = dir.split(path.sep).slice(-2).join(path.sep);
        const json = bignumJSONParse(getOrReadFileBuffer(jsonFilePath, 'utf8'));
        const arrowBuffer = getOrReadFileBuffer(arrowFilePath) as Uint8Array;
        describe(path.join(dir, name), () => {
            testReaderIntegration(json, arrowBuffer);
            testTableFromBuffersIntegration(json, arrowBuffer);
            testTableToBuffersIntegration('json', 'file')(json, arrowBuffer);
            testTableToBuffersIntegration('binary', 'file')(json, arrowBuffer);
            testTableToBuffersIntegration('json', 'stream')(json, arrowBuffer);
            testTableToBuffersIntegration('binary', 'stream')(json, arrowBuffer);
        });
    }
    testReadingMultipleTablesFromTheSameStream();
});

function testReaderIntegration(jsonData: any, arrowBuffer: Uint8Array) {
    test(`json and arrow record batches report the same values`, () => {
        expect.hasAssertions();
        const jsonRecordBatches = toArray(read(jsonData));
        const binaryRecordBatches = toArray(read(arrowBuffer));
        for (const [jsonRecordBatch, binaryRecordBatch] of zip(jsonRecordBatches, binaryRecordBatches)) {
            expect(jsonRecordBatch.length).toEqual(binaryRecordBatch.length);
            expect(jsonRecordBatch.numCols).toEqual(binaryRecordBatch.numCols);
            for (let i = -1, n = jsonRecordBatch.numCols; ++i < n;) {
                const v1 = jsonRecordBatch.getChildAt(i);
                const v2 = binaryRecordBatch.getChildAt(i);
                const name = jsonRecordBatch.schema.fields[i].name;
                (expect([v1, `json`, name]) as any)
                    .toEqualVector([v2, `binary`]);
            }
        }
    });
}

function testTableFromBuffersIntegration(jsonData: any, arrowBuffer: Uint8Array) {
    test(`json and arrow tables report the same values`, () => {
        expect.hasAssertions();
        const jsonTable = Table.from(jsonData);
        const binaryTable = Table.from(arrowBuffer);
        expect(jsonTable.length).toEqual(binaryTable.length);
        expect(jsonTable.numCols).toEqual(binaryTable.numCols);
        for (let i = -1, n = jsonTable.numCols; ++i < n;) {
            const v1 = jsonTable.getColumnAt(i);
            const v2 = binaryTable.getColumnAt(i);
            const name = jsonTable.schema.fields[i].name;
            (expect([v1, `json`, name]) as any)
                .toEqualVector([v2, `binary`]);
        }
    });
}

function testTableToBuffersIntegration(srcFormat: 'json' | 'binary', arrowFormat: 'stream' | 'file') {
    const refFormat = srcFormat === `json` ? `binary` : `json`;
    return function testTableToBuffersIntegration(jsonData: any, arrowBuffer: Uint8Array) {
        test(`serialized ${srcFormat} ${arrowFormat} reports the same values as the ${refFormat} ${arrowFormat}`, () => {
            expect.hasAssertions();
            const refTable = Table.from(refFormat === `json` ? jsonData : arrowBuffer);
            const srcTable = Table.from(srcFormat === `json` ? jsonData : arrowBuffer);
            const dstTable = Table.from(srcTable.serialize(`binary`, arrowFormat === `stream`));
            expect(dstTable.length).toEqual(refTable.length);
            expect(dstTable.numCols).toEqual(refTable.numCols);
            for (let i = -1, n = dstTable.numCols; ++i < n;) {
                const v1 = dstTable.getColumnAt(i);
                const v2 = refTable.getColumnAt(i);
                const name = dstTable.schema.fields[i].name;
                (expect([v1, srcFormat, name]) as any)
                    .toEqualVector([v2, refFormat]);
            }
        });
    }
}

function testReadingMultipleTablesFromTheSameStream() {

    test('Can read multiple tables from the same stream with a special stream reader', async () => {

        async function* allTablesReadableStream() {
            for (const [, arrowPath] of jsonAndArrowPaths) {
                for await (const buffer of fs.createReadStream(arrowPath)) {
                    yield buffer as Uint8Array;
                }
            }
        }

        const pathsAsync = AsyncIterableX.from(jsonAndArrowPaths);
        const batchesAsync = readBatches(allTablesReadableStream());
        const pathsAndBatches = zipAsync(pathsAsync, batchesAsync);

        for await (const [[jsonFilePath, arrowFilePath], batches] of pathsAndBatches) {

            const streamTable = new Table(await toArrayAsync(batches));
            const binaryTable = Table.from(getOrReadFileBuffer(arrowFilePath) as Uint8Array);
            const jsonTable = Table.from(bignumJSONParse(getOrReadFileBuffer(jsonFilePath, 'utf8')));

            expect(streamTable.length).toEqual(jsonTable.length);
            expect(streamTable.length).toEqual(binaryTable.length);
            expect(streamTable.numCols).toEqual(jsonTable.numCols);
            expect(streamTable.numCols).toEqual(binaryTable.numCols);
            for (let i = -1, n = streamTable.numCols; ++i < n;) {
                const v1 = streamTable.getColumnAt(i);
                const v2 = jsonTable.getColumnAt(i);
                const v3 = binaryTable.getColumnAt(i);
                const name = streamTable.schema.fields[i].name;
                (expect([v1, `stream`, name]) as any).toEqualVector([v2, `json`]);
                (expect([v1, `stream`, name]) as any).toEqualVector([v3, `binary`]);
            }
        }
    });

    async function* readBatches(stream: AsyncIterable<Uint8Array>) {

        let message: any, done = false, broke = false;
        let source = buffers(fromReadableStream(stream as any));
    
        do {
            yield readRecordBatchesAsync(messages({
                next(x: any) { return source.next(x); },
                throw(x: any) { return source.throw!(x); },
                [Symbol.asyncIterator]() { return this; },
            }));
        } while (!done || (message = null));
    
        source.return && (await source.return());
    
        async function* messages(source: AsyncIterableIterator<Uint8Array>) {
            for await (message of readBuffersAsync(source)) {
                if (broke = message.message.headerType === 1) {
                    break;
                }
                yield message;
                message = null;
            }
            done = done || !broke;
            broke = false;
        }
    
        async function* buffers(source: AsyncIterableIterator<Uint8Array>) {
            while (!done) {
                message && (yield message.loader.bytes);
                const next = await source.next();
                if (!(done = next.done)) {
                    yield next.value;
                }
            }
        }
    }
}
