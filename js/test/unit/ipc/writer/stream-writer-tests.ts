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
    generateDictionaryTables
} from '../../../data/tables';

import { validateRecordBatchIterator } from '../validate';
import { Table, RecordBatchReader, RecordBatchStreamWriter } from '../../../Arrow';

describe('RecordBatchStreamWriter', () => {

    for (const table of generateRandomTables([10, 20, 30])) {
        testStreamWriter(table, `[${table.schema.fields.join(', ')}]`);
    }

    for (const table of generateDictionaryTables([10, 20, 30])) {
        testStreamWriter(table, `${table.schema.fields[0]}`);
    }

    test(`should write multiple tables to the same output stream`, async () => {
        const tables = [] as Table[];
        const writer = new RecordBatchStreamWriter({ autoDestroy: false });
        const validate = (async () => {
            for await (const reader of RecordBatchReader.readAll(writer)) {
                const sourceTable = tables.shift()!;
                const streamTable = await Table.from(reader);
                expect(streamTable).toEqualTable(sourceTable);
            }
        })();
        for (const table of generateRandomTables([10, 20, 30])) {
            tables.push(table);
            await writer.writeAll((async function*() {
                for (const chunk of table.chunks) {
                    yield chunk; // insert some asynchrony
                    await new Promise((r) => setTimeout(r, 1));
                }
            }()));
        }
        writer.close();
        await validate;
    });
});

function testStreamWriter(table: Table, name: string) {
    describe(`should write the Arrow IPC stream format (${name})`, () => {
        test(`Table`, validateTable.bind(0, table));
    });
}

async function validateTable(source: Table) {
    const writer = RecordBatchStreamWriter.writeAll(source);
    const result = await Table.from(writer.toUint8Array());
    validateRecordBatchIterator(3, source.chunks);
    expect(result).toEqualTable(source);
}
