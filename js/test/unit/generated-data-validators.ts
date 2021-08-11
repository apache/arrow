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

import { RecordBatch, Schema, Vector, util } from 'apache-arrow';

const { createElementComparator: compare } = util;

export function validateTable({ keys, rows, cols, rowBatches, colBatches, keyBatches, table }: GeneratedTable) {
    describe(`Table: ${table.schema}`, () => {
        validateVector({ values: rows, vector: new Vector(table.data) });
        table.data.forEach((batch, i) => {
            describe(`recordBatch ${i}`, () => {
                validateRecordBatch({
                    keys: keyBatches[i], rows: rowBatches[i], cols: colBatches[i],
                    recordBatch: new RecordBatch(new Schema(batch.type.children), batch)
                });
            })
        });
        table.schema.fields.forEach((field, i) => {
            describe(`column ${i}: ${field}`, () => {
                validateVector({
                    keys: keys()[i],
                    values: () => cols()[i],
                    vector: table.getChildAt(i)!
                });
            });
        });
    });
}

export function validateRecordBatch({ rows, cols, keys, recordBatch }: GeneratedRecordBatch) {
    describe(`RecordBatch: ${recordBatch.schema}`, () => {
        validateVector({ values: rows, vector: new Vector([recordBatch.data]) }),
        recordBatch.schema.fields.forEach((field, i) => {
            describe(`Field: ${field}`, () => {
                validateVector({
                    keys: keys()[i],
                    values: () => cols()[i],
                    vector: recordBatch.getChildAt(i)!
                });
            });
        });
    });
}

export function validateVector({ values: createTestValues, vector, keys }: GeneratedVector<any>) {

    const values = createTestValues();
    const begin = (values.length * .25) | 0;
    const end = (values.length * .75) | 0;

    describe(`Vector<${vector.type}>`, () => {
        // test no slice
        describe(`sliced=false`, () => vectorTests(values, vector, keys));
        // test slice with no args
        describe(`sliced=true, begin=, end=`, () => {
            vectorTests(
                values.slice(), // values,
                vector.slice(), // vector,
                keys ? keys.slice() : undefined // keys
            );
        });
        // test slicing half the array
        describe(`sliced=true, begin=${begin}, end=${end}`, () => {
            vectorTests(
                values.slice(begin, end), // values,
                vector.slice(begin, end), // vector,
                keys ? keys.slice(begin, end) : undefined // keys
            );
        });
        // test concat each end together
        describe(`sliced=true, begin=${end}, end=${begin}, concat=true`, () => {
            vectorTests(
                values.slice(0, begin).concat(values.slice(end)), // values,
                vector.slice(0, begin).concat(vector.slice(end)), // vector,
                keys ? [...keys.slice(0, begin), ...keys.slice(end)] : undefined // keys
            );
        });
    });
}

function vectorTests(values: any[], vector: Vector<any>, keys?: number[]) {
        test(`length is correct`, () => {
            expect(vector).toHaveLength(values.length);
        });
        test(`gets expected values`, () => {
            expect.hasAssertions();
            let i = -1, n = vector.length, actual, expected;
            try {
                while (++i < n) {
                    actual = vector[i];
                    expected = values[i];
                    expect(actual).toArrowCompare(expected);
                }
            } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
        });
        if (keys && keys.length > 0) {
            test(`dictionary indices should match`, () => {
                expect.hasAssertions();
                let indices = new Vector(vector.data.map((data) => data.clone(vector.type.indices)));
                let i = -1, n = indices.length;
                try {
                    while (++i < n) {
                        indices.isValid(i)
                            ? expect(indices[i]).toBe(keys[i])
                            : expect(indices[i]).toBeNull();
                    }
                } catch (e) { throw new Error(`${indices}[${i}]: ${e}`); }
            });
        }
        test(`sets expected values`, () => {
            expect.hasAssertions();
            let i = -1, n = vector.length, actual, expected;
            try {
                while (++i < n) {
                    expected = values[i];
                    vector[i] = expected;
                    actual = vector[i];
                    expect(actual).toArrowCompare(expected);
                }
            } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
        });
        test(`iterates expected values`, () => {
            expect.hasAssertions();
            let i = -1, actual, expected;
            try {
                for (actual of vector) {
                    expected = values[++i];
                    expect(actual).toArrowCompare(expected);
                }
            } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
        });
        test(`indexOf returns expected values`, () => {
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
        });
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
