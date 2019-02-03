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
import { RecordBatchReader } from '../../../Arrow';
import { validateRecordBatchReader } from '../validate';

/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

for (const table of generateRandomTables([10, 20, 30])) {

    const io = ArrowIOTestHelper.json(table);
    const name = `[\n ${table.schema.fields.join(',\n ')}\n]`;

    describe(`RecordBatchJSONReader (${name})`, () => {
        describe(`should read all RecordBatches`, () => {
            test(`Uint8Array`, io.buffer((buffer) => {
                const json = bignumJSONParse(Buffer.from(buffer).toString());
                validateRecordBatchReader('json', 3, RecordBatchReader.from(json));
            }));
        });
    });
}
