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
    Int32, Dictionary, DateUnit, util,
    Data, Vector, Utf8Vector, DateVector, DictionaryVector,
} from '../../Arrow';

describe(`DateVector`, () => {
    const extras = [
        new Date(2000, 0, 1),
        new Date(1991, 5, 28, 12, 11, 10)
    ];
    describe(`unit = MILLISECOND`, () => {
        const values = [
            new Date(1989, 5, 22, 1, 2, 3),
            new Date(1988, 3, 25, 4, 5, 6),
            new Date(1987, 2, 24, 7, 8, 9),
            new Date(2018, 4, 12, 17, 30, 0)
        ];
        const vector = DateVector.from(values);
        basicVectorTests(vector, values, extras);
    });
    describe(`unit = DAY`, () => {
        // Use UTC to ensure that dates are always at midnight
        const values = [
            new Date(Date.UTC(1989, 5, 22)),
            new Date(Date.UTC(1988, 3, 25)),
            new Date(Date.UTC(1987, 2, 24)),
            new Date(Date.UTC(2018, 4, 12))
        ];
        const vector = DateVector.from(values, DateUnit.DAY);
        basicVectorTests(vector, values, extras);
    });
});

describe(`DictionaryVector`, () => {

    const dictionary = ['foo', 'bar', 'baz'];
    const extras = ['abc', '123']; // values to search for that should NOT be found
    const dictionary_vec = Utf8Vector.from(dictionary);

    const indices = Array.from({length: 50}, () => Math.random() * 3 | 0);
    const validity = Array.from({ length: indices.length }, () => Math.random() > 0.2 ? true : false);

    describe(`index with nullCount == 0`, () => {

        const values = Array.from(indices).map((d) => dictionary[d]);
        const vector = DictionaryVector.from(dictionary_vec, new Int32(), indices);

        basicVectorTests(vector, values, extras);

        describe(`sliced`, () => {
            basicVectorTests(vector.slice(10, 20), values.slice(10,20), extras);
        });
    });

    describe(`index with nullCount > 0`, () => {

        const nullBitmap = util.packBools(validity);
        const nullCount = validity.reduce((acc, d) => acc + (d ? 0 : 1), 0);
        const values = Array.from(indices).map((d, i) => validity[i] ? dictionary[d] : null);
        const type = new Dictionary(dictionary_vec.type, new Int32(), null, null, dictionary_vec);
        const vector = Vector.new(Data.Dictionary(type, 0, indices.length, nullCount, nullBitmap, indices));

        basicVectorTests(vector, values, ['abc', '123']);
        describe(`sliced`, () => {
            basicVectorTests(vector.slice(10, 20), values.slice(10,20), extras);
        });
    });
});

describe(`Utf8Vector`, () => {
    const values = ['foo', 'bar', 'baz', 'foo bar', 'bar'];
    const vector = Utf8Vector.from(values);
    basicVectorTests(vector, values, ['abc', '123']);
    describe(`sliced`, () => {
        basicVectorTests(vector.slice(1,3), values.slice(1,3), ['foo', 'abc']);
    });
});

// Creates some basic tests for the given vector.
// Verifies that:
// - `get` and the native iterator return the same data as `values`
// - `indexOf` returns the same indices as `values`
function basicVectorTests(vector: Vector, values: any[], extras: any[]) {

    const n = values.length;

    test(`gets expected values`, () => {
        let i = -1;
        while (++i < n) {
            expect(vector.get(i)).toEqual(values[i]);
        }
    });
    test(`iterates expected values`, () => {
        expect.hasAssertions();
        let i = -1;
        for (let v of vector) {
            expect(++i).toBeLessThan(n);
            expect(v).toEqual(values[i]);
        }
    });
    test(`indexOf returns expected values`, () => {
        let testValues = values.concat(extras);

        for (const value of testValues) {
            const actual = vector.indexOf(value);
            const expected = values.indexOf(value);
            expect(actual).toEqual(expected);
        }
    });
}
