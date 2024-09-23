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

import { Bool, makeVector, vectorFromArray } from 'apache-arrow';

const newBoolVector = (length: number, data: Uint8Array) => makeVector({ type: new Bool(), length, data });


describe(`BoolVector`, () => {
    const values = [true, true, false, true, true, false, false, false];
    const n = values.length;
    const vector = newBoolVector(n, new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0]));
    test(`gets expected values`, () => {
        for (let i = 0; i < values.length; i++) {
            expect(vector.get(i)).toEqual(values[i]);
            expect(vector.at(i)).toEqual(values.at(i));
            expect(vector.at(-i)).toEqual(values.at(-i));
        }
    });
    test(`iterates expected values`, () => {
        let i = -1;
        for (const v of vector) {
            expect(++i).toBeLessThan(n);
            expect(v).toEqual(values[i]);
        }
    });
    test(`indexOf returns expected values`, () => {
        for (const test_value of [true, false]) {
            const expected = values.indexOf(test_value);
            expect(vector.indexOf(test_value)).toEqual(expected);
        }
    });
    test(`indexOf returns -1 when value not found`, () => {
        const v = newBoolVector(3, new Uint8Array([0xFF]));
        expect(v.indexOf(false)).toBe(-1);
    });
    test(`can set values to true and false`, () => {
        const v = newBoolVector(n, new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0]));
        const expected1 = [true, true, false, true, true, false, false, false];
        const expected2 = [true, true, true, true, true, false, false, false];
        const expected3 = [true, true, false, false, false, false, true, true];
        function validate(expected: boolean[]) {
            for (let i = 0; i < n; i++) {
                expect(v.get(i)).toEqual(expected[i]);
            }
        }
        validate(expected1);
        v.set(2, true);
        validate(expected2);
        v.set(2, false);
        validate(expected1);
        v.set(3, false);
        v.set(4, false);
        v.set(6, true);
        v.set(7, true);
        validate(expected3);
        v.set(3, true);
        v.set(4, true);
        v.set(6, false);
        v.set(7, false);
        validate(expected1);
    });
    test(`packs 0 values`, () => {
        const expected = new Uint8Array(64);
        expect(vectorFromArray([], new Bool()).data[0].values).toEqual(expected);
    });
    test(`packs 3 values`, () => {
        const expected = new Uint8Array(64);
        expected[0] = 5;
        expect(vectorFromArray([
            true, false, true
        ]).data[0].values).toEqual(expected);
    });
    test(`packs 8 values`, () => {
        const expected = new Uint8Array(64);
        expected[0] = 27;
        expect(vectorFromArray([
            true, true, false, true, true, false, false, false
        ]).data[0].values).toEqual(expected);
    });
    test(`packs 25 values`, () => {
        const expected = new Uint8Array(64);
        expected[0] = 27;
        expected[1] = 216;
        expect(vectorFromArray([
            true, true, false, true, true, false, false, false,
            false, false, false, true, true, false, true, true,
            false
        ]).data[0].values).toEqual(expected);
    });
    test(`from with boolean Array packs values`, () => {
        const expected = new Uint8Array(64);
        expected[0] = 5;
        expect(vectorFromArray([true, false, true])
            .slice().data[0].values
        ).toEqual(expected);
    });
});
