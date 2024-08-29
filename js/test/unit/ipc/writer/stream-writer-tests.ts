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
    generateDictionaryTables,
    generateRandomTables
} from '../../../data/tables.js';
import * as generate from '../../../generate-test-data.js';
import { validateRecordBatchIterator } from '../validate.js';

import type { RecordBatchStreamWriterOptions } from 'apache-arrow/ipc/writer';
import {
    builderThroughIterable,
    Dictionary,
    Field,
    Int32,
    RecordBatch,
    RecordBatchReader,
    RecordBatchStreamWriter,
    Schema,
    Table,
    Uint32,
    Vector
} from 'apache-arrow';

describe('RecordBatchStreamWriter', () => {

    const type = generate.sparseUnion(0, 0).vector.type;
    const schema = new Schema([new Field('dictSparseUnion', type)]);
    const table = generate.table([10, 20, 30], schema).table;
    const testName = `[${table.schema.fields.join(', ')}]`;
    testStreamWriter(table, testName, { writeLegacyIpcFormat: true });
    testStreamWriter(table, testName, { writeLegacyIpcFormat: false });

    for (const table of generateRandomTables([10, 20, 30])) {
        const testName = `[${table.schema.fields.join(', ')}]`;
        testStreamWriter(table, testName, { writeLegacyIpcFormat: true });
        testStreamWriter(table, testName, { writeLegacyIpcFormat: false });
    }

    for (const table of generateDictionaryTables([10, 20, 30])) {
        const testName = `${table.schema.fields[0]}`;
        testStreamWriter(table, testName, { writeLegacyIpcFormat: true });
        testStreamWriter(table, testName, { writeLegacyIpcFormat: false });
    }

    it(`should write multiple tables to the same output stream`, async () => {
        const tables = [] as Table[];
        const writer = new RecordBatchStreamWriter({ autoDestroy: false });
        const validate = (async () => {
            for await (const reader of RecordBatchReader.readAll(writer)) {
                const sourceTable = tables.shift()!;
                const streamTable = new Table(await reader.readAll());
                expect(streamTable).toEqualTable(sourceTable);
            }
        })();
        for (const table of generateRandomTables([10, 20, 30])) {
            tables.push(table);
            await writer.writeAll((async function* () {
                for (const chunk of table.batches) {
                    yield chunk; // insert some asynchrony
                    await new Promise((r) => setTimeout(r, 1));
                }
            }()));
        }
        writer.close();
        await validate;
    });

    it('should write delta dictionary batches', async () => {

        const name = 'dictionary_encoded_uint32';
        const chunks: Vector<Dictionary<Uint32, Int32>>[] = [];
        const {
            vector: sourceVector, values: sourceValues,
        } = generate.dictionary(1000, 20, new Uint32(), new Int32());

        const writer = RecordBatchStreamWriter.writeAll((function* () {
            const transform = builderThroughIterable({
                type: sourceVector.type, nullValues: [null],
                queueingStrategy: 'count', highWaterMark: 50,
            });
            for (const chunk of transform(sourceValues())) {
                chunks.push(chunk);
                yield new RecordBatch({ [name]: chunk.data[0] });
            }
        })());

        expect(new Vector(chunks)).toEqualVector(sourceVector);

        type T = { [name]: Dictionary<Uint32, Int32> };
        const sourceTable = new Table({ [name]: sourceVector });
        const resultTable = new Table(RecordBatchReader.from<T>(await writer.toUint8Array()));

        const child = resultTable.getChild(name)!;
        const dicts = child.data.map(({ dictionary }) => dictionary!);
        const dictionary = dicts[child.data.length - 1];

        expect(resultTable).toEqualTable(sourceTable);
        expect(dictionary).toBeInstanceOf(Vector);
        expect(dictionary.data).toHaveLength(20);
    });
});

function testStreamWriter(table: Table, name: string, options: RecordBatchStreamWriterOptions) {
    describe(`should write the Arrow IPC stream format (${name})`, () => {
        test(`Table`, validateTable.bind(0, table, options));
    });
}

async function validateTable(source: Table, options: RecordBatchStreamWriterOptions) {
    const writer = RecordBatchStreamWriter.writeAll(source, options);
    const reader = RecordBatchReader.from(await writer.toUint8Array());
    const result = new Table(...reader);
    validateRecordBatchIterator(3, source.batches);
    expect(result).toEqualTable(source);
}
