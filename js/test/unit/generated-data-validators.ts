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
import {
    GeneratedTable,
    GeneratedRecordBatch,
    GeneratedVector
} from '../generate-test-data';

import { util } from 'apache-arrow';
const { createElementComparator: compare } = util;

type DeferredTest = { description: string; tests?: DeferredTest[]; run: (...args: any[]) => any };

function deferTest(description: string, run: (...args: any[]) => any) {
    return { description, run: () => test(description, run) } as DeferredTest;
}

function deferDescribe(description: string, tests: DeferredTest | DeferredTest[]) {
    const t = (Array.isArray(tests) ? tests : [tests]).filter(Boolean);
    return { description, tests: t, run: () => describe(description, () => { t.forEach((x) => x.run()); } ) };
}

export function validateTable({ keys, rows, cols, rowBatches, colBatches, keyBatches, table }: GeneratedTable) {
    return deferDescribe(`Table: ${table.schema}`, ([] as DeferredTest[]).concat(
        validateVector({ values: rows, vector: table }),
        table.chunks.map((recordBatch, i) =>
            deferDescribe(`recordBatch ${i}`, validateRecordBatch({
                keys: keyBatches[i], rows: rowBatches[i], cols: colBatches[i], recordBatch
            }))
        ),
        table.schema.fields.map((field, i) =>
            deferDescribe(`column ${i}: ${field}`, validateVector({
                keys: keys()[i],
                values: () => cols()[i],
                vector: table.getColumnAt(i)!
            }))
        )
    ));
}

export function validateRecordBatch({ rows, cols, keys, recordBatch }: GeneratedRecordBatch) {
    return deferDescribe(`RecordBatch: ${recordBatch.schema}`, ([] as DeferredTest[]).concat(
        validateVector({ values: rows, vector: recordBatch }),
        recordBatch.schema.fields.map((field, i) =>
            deferDescribe(`Field: ${field}`, validateVector({
                keys: keys()[i],
                values: () => cols()[i],
                vector: recordBatch.getChildAt(i)!
            }))
        )
    ));
}

export function validateVector({ values: createTestValues, vector, keys }: GeneratedVector, sliced = false) {

    const values = createTestValues();
    const suites = [
        deferDescribe(`Validate ${vector.type} (sliced=${sliced})`, [
            deferTest(`length is correct`, () => {
                expect(vector).toHaveLength(values.length);
            }),
            deferTest(`gets expected values`, () => {
                expect.hasAssertions();
                let i = -1, n = vector.length, actual, expected;
                try {
                    while (++i < n) {
                        actual = vector.get(i);
                        expected = values[i];
                        expect(actual).toArrowCompare(expected);
                    }
                } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
            }),
            (keys && keys.length > 0) && deferTest(`dictionary indices should match`, () => {
                expect.hasAssertions();
                let indices = (vector as any).indices;
                let i = -1, n = indices.length;
                try {
                    while (++i < n) {
                        indices.isValid(i)
                            ? expect(indices.get(i)).toBe(keys[i])
                            : expect(indices.get(i)).toBeNull();
                    }
                } catch (e) { throw new Error(`${indices}[${i}]: ${e}`); }
            }) || null as any as DeferredTest,
            deferTest(`sets expected values`, () => {
                expect.hasAssertions();
                let i = -1, n = vector.length, actual, expected;
                try {
                    while (++i < n) {
                        expected = vector.get(i);
                        vector.set(i, expected);
                        actual = vector.get(i);
                        expect(actual).toArrowCompare(expected);
                    }
                } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
            }),
            deferTest(`iterates expected values`, () => {
                expect.hasAssertions();
                let i = -1, actual, expected;
                try {
                    for (actual of vector) {
                        expected = values[++i];
                        expect(actual).toArrowCompare(expected);
                    }
                } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
            }),
            deferTest(`indexOf returns expected values`, () => {
                expect.hasAssertions();
                let i = -1, n = vector.length;
                const shuffled = shuffle(values);
                let value: any, actual, expected;
                try {
                    while (++i < n) {
                        value = shuffled[i];
                        actual = vector.indexOf(value);
                        expected = values.findIndex(compare(value));
                        expect(actual).toBe(expected);
                    }
                    // I would be pretty surprised if randomatic ever generates these values
                    expect(vector.indexOf('purple elephants')).toBe(-1);
                    expect(vector.indexOf('whistling wombats')).toBe(-1);
                    expect(vector.indexOf('carnivorous novices')).toBe(-1);
                } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
            })
        ])
    ] as DeferredTest[];

    if (!sliced) {
        const begin = (values.length * .25) | 0;
        const end = (values.length * .75) | 0;
        suites.push(
            // test slice with no args
            validateVector({
                vector: vector.slice(),
                values: () => values.slice(),
                keys: keys ? keys.slice() : undefined
            }, true),
            // test slicing half the array
            validateVector({
                vector: vector.slice(begin, end),
                values: () => values.slice(begin, end),
                keys: keys ? keys.slice(begin, end) : undefined
            }, true),
            // test concat each end together
            validateVector({
                vector: vector.slice(0, begin).concat(vector.slice(end)),
                values: () => values.slice(0, begin).concat(values.slice(end)),
                keys: keys ? [...keys.slice(0, begin), ...keys.slice(end)] : undefined
            }, true)
        );

        return deferDescribe(`Vector`, suites);
    }

    return suites[0];
}

function shuffle(input: any[]) {
    const result = input.slice();
    let j, tmp, i = result.length;
    while (--i > 0) {
        j = (Math.random() * (i + 1)) | 0;
        tmp = result[i];
        result[i] = result[j];
        result[j] = tmp;
    }
    return result;
}
