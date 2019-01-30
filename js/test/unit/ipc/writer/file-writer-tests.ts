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
import { Table, RecordBatchFileWriter } from '../../../Arrow';

describe('RecordBatchFileWriter', () => {
    for (const table of generateRandomTables([10, 20, 30])) {
        testFileWriter(table, `[${table.schema.fields.join(', ')}]`);
    }
    for (const table of generateDictionaryTables([10, 20, 30])) {
        testFileWriter(table, `${table.schema.fields[0]}`);
    }
});

function testFileWriter(table: Table, name: string) {
    describe(`should write the Arrow IPC file format (${name})`, () => {
        test(`Table`, validateTable.bind(0, table));
    });
}

async function validateTable(source: Table) {
    const writer = RecordBatchFileWriter.writeAll(source);
    const result = await Table.from(writer.toUint8Array());
    validateRecordBatchIterator(3, source.chunks);
    expect(result).toEqualTable(source);
}
