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

import { Arrow } from './index.js';

// from https://stackoverflow.com/a/19303725/214950
let seed = 1;
function random() {
    const x = Math.sin(seed++) * 10_000;
    return x - Math.floor(x);
}

console.time('Prepare Data');

const LENGTH = 100_000;
const NUM_BATCHES = 10;
const cities = ['Charlottesville', 'New York', 'San Francisco', 'Seattle', 'Terre Haute', 'Washington, DC'];

const values = Arrow.vectorFromArray(cities, new Arrow.Utf8).memoize();

const batches = Array.from({ length: NUM_BATCHES }).map(() => {
    const lat = Float32Array.from(
        { length: LENGTH },
        () => ((random() - 0.5) * 2 * 90));
    const lng = Float32Array.from(
        { length: LENGTH },
        () => ((random() - 0.5) * 2 * 90));

    const origin = Uint8Array.from(
        { length: LENGTH },
        () => (random() * 6));
    const destination = Uint8Array.from(
        { length: LENGTH },
        () => (random() * 6));

    const originType = new Arrow.Dictionary(values.type, new Arrow.Int8, 0, false);
    const destinationType = new Arrow.Dictionary(values.type, new Arrow.Int8, 0, false);

    return new Arrow.RecordBatch({
        'lat': Arrow.makeData({ type: new Arrow.Float32, data: lat }),
        'lng': Arrow.makeData({ type: new Arrow.Float32, data: lng }),
        'origin': Arrow.makeData({ type: originType, length: origin.length, nullCount: 0, data: origin, dictionary: values }),
        'destination': Arrow.makeData({ type: destinationType, length: destination.length, nullCount: 0, data: destination, dictionary: values }),
    });
});

export const typedArrays = {
    uint8Array: Uint8Array.from({ length: LENGTH }, () => random() * 255),
    uint16Array: Uint16Array.from({ length: LENGTH }, () => random() * 255),
    uint32Array: Uint32Array.from({ length: LENGTH }, () => random() * 255),
    uint64Array: BigUint64Array.from({ length: LENGTH }, () => 42n),

    int8Array: Int8Array.from({ length: LENGTH }, () => random() * 255),
    int16Array: Int16Array.from({ length: LENGTH }, () => random() * 255),
    int32Array: Int32Array.from({ length: LENGTH }, () => random() * 255),
    int64Array: BigInt64Array.from({ length: LENGTH }, () => 42n),

    float32Array: Float32Array.from({ length: LENGTH }, () => random() * 255),
    float64Array: Float64Array.from({ length: LENGTH }, () => random() * 255)
};

export const arrays = {
    numbers: Array.from({ length: LENGTH }, () => random() * 255),
    booleans: Array.from({ length: LENGTH }, () => random() > 0.5),
    dictionary: Array.from({ length: LENGTH }, () => cities[Math.floor(random() * cities.length)])
};

export const vectors: { [k: string]: Arrow.Vector } = Object.fromEntries([
    ...Object.entries(typedArrays).map(([name, array]) => [name, Arrow.makeVector(array)]),
    ...Object.entries(arrays).map(([name, array]) => [name, Arrow.vectorFromArray(array)]),
    ['string', Arrow.vectorFromArray(arrays.dictionary, new Arrow.Utf8)],
]);

const tracks = new Arrow.Table(batches[0].schema, batches);

console.timeEnd('Prepare Data');

export default [
    {
        name: 'tracks',
        table: tracks,
        ipc: Arrow.RecordBatchStreamWriter.writeAll(tracks).toUint8Array(true),
        countBys: ['origin', 'destination'],
        counts: [
            { column: 'lat', test: 'gt' as 'gt' | 'eq', value: 0 },
            { column: 'lng', test: 'gt' as 'gt' | 'eq', value: 0 },
            { column: 'origin', test: 'eq' as 'gt' | 'eq', value: 'Seattle' },
        ],
    }
];
