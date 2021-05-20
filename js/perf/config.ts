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

import * as Arrow from '../src/Arrow.dom';

console.time('Prepare Data');

const LENGTH = 100000;
const NUM_BATCHES = 10;

const values = Arrow.Utf8Vector.from(['Charlottesville', 'New York', 'San Francisco', 'Seattle', 'Terre Haute', 'Washington, DC']);

const batches = Array.from({length: NUM_BATCHES}).map(() => {
    const lat = Float32Array.from(
        { length: LENGTH },
        () => ((Math.random() - 0.5) * 2 * 90));
    const lng = Float32Array.from(
        { length: LENGTH },
        () => ((Math.random() - 0.5) * 2 * 90));

    const origin = Uint8Array.from(
        { length: LENGTH },
        () => (Math.random() * 6));
    const destination = Uint8Array.from(
        { length: LENGTH },
        () => (Math.random() * 6));

    return Arrow.RecordBatch.new({
        'lat': Arrow.Float32Vector.from(lat),
        'lng': Arrow.Float32Vector.from(lng),
        'origin': Arrow.DictionaryVector.from(values, new Arrow.Int8(), origin),
        'destination': Arrow.DictionaryVector.from(values, new Arrow.Int8(), destination),
    });
});

const {schema} = batches[0];
const tracks = new Arrow.DataFrame(schema, batches);

console.timeEnd('Prepare Data');

export default [
    {
        name: 'tracks',
        df: tracks,
        ipc: tracks.serialize(),
        countBys: ['origin', 'destination'],
        counts: [
            {column: 'lat',    test: 'gt' as 'gt' | 'eq', value: 0        },
            {column: 'lng',    test: 'gt' as 'gt' | 'eq', value: 0        },
            {column: 'origin', test: 'eq' as 'gt' | 'eq', value: 'Seattle'},
        ],
    }
];
