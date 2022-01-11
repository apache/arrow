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

import { generateRandomTables } from '../../../data/tables.js';
import { ArrowIOTestHelper } from '../helpers.js';
import { validateRecordBatchReader } from '../validate.js';

import { RecordBatchReader, Table } from 'apache-arrow';

for (const table of generateRandomTables([10, 20, 30])) {

    const io = ArrowIOTestHelper.json(table);
    const name = `[\n ${table.schema.fields.join(',\n ')}\n]`;

    describe(`RecordBatchJSONReader (${name})`, () => {
        describe(`should read all RecordBatches`, () => {
            test(`Uint8Array`, io.buffer((buffer) => {
                const json = JSON.parse(Buffer.from(buffer).toString());
                validateRecordBatchReader('json', 3, RecordBatchReader.from(json));
            }));
        });
    });
}

// Test for failing integration test
// TODO: remove me
test(`gold dataset`, () => {
    const json = {
        'schema': {
            'fields': [
                {
                    'name': 'f0',
                    'type': {
                        'name': 'decimal',
                        'precision': 3,
                        'scale': 2
                    },
                    'nullable': true,
                    'children': []
                }
            ]
        },
        'batches': [
            {
                'count': 7,
                'columns': [
                    {
                        'name': 'f0',
                        'count': 7,
                        'VALIDITY': [
                            1,
                            0,
                            1,
                            1,
                            0,
                            1,
                            0
                        ],
                        'DATA': [
                            '-11697',
                            '-25234',
                            '27521',
                            '-18229',
                            '-12589',
                            '13359',
                            '16532'
                        ]
                    }
                ]
            }
        ]
    };

    const table = new Table(RecordBatchReader.from(json));
    const value = table.getChild('f0')!.get(0);
    expect(value.toString()).toBe('340282366920938463463374607431768199759');
});
