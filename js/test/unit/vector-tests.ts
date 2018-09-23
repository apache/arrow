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

import { TextEncoder } from 'text-encoding-utf-8';
import Arrow from '../Arrow';
import { TypedArray, TypedArrayConstructor } from '../../src/Arrow';

const utf8Encoder = new TextEncoder('utf-8');

const { packBools } = Arrow.util;
const { BoolData, FlatData, FlatListData, DictionaryData } = Arrow.data;
const { IntVector, FloatVector, BoolVector, Utf8Vector, DateVector, DictionaryVector } = Arrow.vector;
const {
    Dictionary, Utf8, Bool,
    Float16, Float32, Float64,
    Int8, Int16, Int32, Int64,
    Uint8, Uint16, Uint32, Uint64,
} = Arrow.type;

const { DateUnit } = Arrow.enum_;

const FixedSizeVectors = {
    Int64Vector: [IntVector, Int64] as [typeof IntVector, typeof Int64],
    Uint64Vector: [IntVector, Uint64] as [typeof IntVector, typeof Uint64],
};

const FixedWidthVectors = {
    Int8Vector: [IntVector, Int8] as [typeof IntVector, typeof Int8],
    Int16Vector: [IntVector, Int16] as [typeof IntVector, typeof Int16],
    Int32Vector: [IntVector, Int32] as [typeof IntVector, typeof Int32],
    Uint8Vector: [IntVector, Uint8] as [typeof IntVector, typeof Uint8],
    Uint16Vector: [IntVector, Uint16] as [typeof IntVector, typeof Uint16],
    Uint32Vector: [IntVector, Uint32] as [typeof IntVector, typeof Uint32],
    Float32Vector: [FloatVector, Float32] as [typeof FloatVector, typeof Float32],
    Float64Vector: [FloatVector, Float64] as [typeof FloatVector, typeof Float64],
};

const fixedSizeVectors = toMap(FixedSizeVectors, Object.keys(FixedSizeVectors));
const fixedWidthVectors = toMap(FixedWidthVectors, Object.keys(FixedWidthVectors));
const randomBytes = (n: number) => Uint8Array.from(
    { length: n },
    () => Math.random() * 255 | 0
);
const bytes = Array.from(
    { length: 5 },
    () => randomBytes(64)
);

describe(`BoolVector`, () => {
    const values = [true, true, false, true, true, false, false, false], n = values.length;
    const vector = new BoolVector(new BoolData(new Bool(), n, null, new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0])));
    test(`gets expected values`, () => {
        let i = -1;
        while (++i < n) {
            expect(vector.get(i)).toEqual(values[i]);
        }
    });
    test(`iterates expected values`, () => {
        let i = -1;
        for (let v of vector) {
            expect(++i).toBeLessThan(n);
            expect(v).toEqual(values[i]);
        }
    });
    test(`indexOf returns expected values`, () => {
        for (let test_value of [true, false]) {
            const expected = values.indexOf(test_value);
            expect(vector.indexOf(test_value)).toEqual(expected);
        }
    });
    test(`indexOf returns -1 when value not found`, () => {
        const v = new BoolVector(new BoolData(new Bool(), 3, null, new Uint8Array([0xFF])));
        expect(v.indexOf(false)).toEqual(-1);
    });
    test(`can set values to true and false`, () => {
        const v = new BoolVector(new BoolData(new Bool(), n, null, new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0])));
        const expected1 = [true, true, false, true, true, false, false, false];
        const expected2 = [true, true,  true, true, true, false, false, false];
        const expected3 = [true, true, false, false, false, false, true, true];
        function validate(expected: boolean[]) {
            for (let i = -1; ++i < n;) {
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
        expect(BoolVector.from([]).values).toEqual(
            new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0]));
    });
    test(`packs 3 values`, () => {
        expect(BoolVector.from([
            true, false, true
        ]).values).toEqual(new Uint8Array([5, 0, 0, 0, 0, 0, 0, 0]));
    });
    test(`packs 8 values`, () => {
        expect(BoolVector.from([
            true, true, false, true, true, false, false, false
        ]).values).toEqual(new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0]));
    });
    test(`packs 25 values`, () => {
        expect(BoolVector.from([
            true, true, false, true, true, false, false, false,
            false, false, false, true, true, false, true, true,
            false
        ]).values).toEqual(new Uint8Array([27, 216, 0, 0, 0, 0, 0, 0]));
    });
    test(`from with boolean Array packs values`, () => {
        expect(BoolVector
            .from([true, false, true])
            .slice().values
        ).toEqual(new Uint8Array([5, 0, 0, 0, 0, 0, 0, 0]));
    });
});

describe('Float16Vector', () => {
    const values = concatTyped(Uint16Array, ...bytes);
    const vector = bytes
        .map((b) => new Uint16Array(b.buffer))
        .map((b) => new FloatVector<Float16>(new FlatData(new Float16(), b.length, null, b)))
        .reduce((v: any, v2) => v.concat(v2));
    const n = values.length;
    const clamp = (x: number) => (x -  32767) / 32767;
    const float16s = new Float32Array([...values].map((x) => clamp(x)));
    test(`gets expected values`, () => {
        let i = -1;
        while (++i < n) {
            expect(vector.get(i)).toEqual(clamp(values[i]));
        }
    });
    test(`iterates expected values`, () => {
        expect.hasAssertions();
        let i = -1;
        for (let v of vector) {
            expect(++i).toBeLessThan(n);
            expect(v).toEqual(clamp(values[i]));
        }
    });
    test(`indexOf returns expected values`, () => {
        const randomValues = new Uint16Array(randomBytes(64).buffer);
        for (let value of [...values, ...randomValues]) {
            const expected = values.indexOf(value);
            expect(vector.indexOf(clamp(value))).toEqual(expected);
        }
    });
    test(`slices the entire array`, () => {
        expect(vector.slice().toArray()).toEqual(float16s);
    });
    test(`slice returns a TypedArray`, () => {
        expect(vector.slice().toArray()).toBeInstanceOf(Float32Array);
    });
    test(`slices from -20 to length`, () => {
        expect(vector.slice(-20).toArray()).toEqual(float16s.slice(-20));
    });
    test(`slices from 0 to -20`, () => {
        expect(vector.slice(0, -20).toArray()).toEqual(float16s.slice(0, -20));
    });
    test(`slices the array from 0 to length - 20`, () => {
        expect(vector.slice(0, n - 20).toArray()).toEqual(float16s.slice(0, n - 20));
    });
    test(`slices the array from 0 to length + 20`, () => {
        expect(vector.slice(0, n + 20).toArray()).toEqual(float16s.slice(0, n + 20));
    });
});

for (const [VectorName, [VectorType, DataType]] of fixedSizeVectors) {
    describe(`${VectorName}`, () => {
        const type = new DataType();
        const values = concatTyped(type.ArrayType as any, ...bytes);
        const vector = bytes
            .map((b) => new type.ArrayType(b.buffer))
            .map((b) => new VectorType(new FlatData(type, b.length * 0.5, null, b)))
            .reduce((v: any, v2) => v.concat(v2));
        const n = values.length * 0.5;
        test(`gets expected values`, () => {
            let i = -1;
            while (++i < n) {
                expect(vector.get(i)).toEqual(values.slice(2 * i, 2 * (i + 1)));
            }
        });
        test(`iterates expected values`, () => {
            let i = -1;
            for (let v of vector) {
                expect(++i).toBeLessThan(n);
                expect(v).toEqual(values.slice(2 * i, 2 * (i + 1)));
            }
        });
        test(`indexOf returns expected values`, () => {
            // Create a set of test data composed of all of the actual values
            // and a few random values
            let testValues = concatTyped(
                type.ArrayType,
                ...bytes,
                ...[randomBytes(8 * 2 * type.ArrayType.BYTES_PER_ELEMENT)]
            );

            for (let i = -1, n = testValues.length / 2 | 0; ++i < n;) {
                const value = testValues.slice(2 * i, 2 * (i + 1));
                const expected = values.findIndex((d, i) => i % 2 === 0 && d === value[0] && testValues[i + 1] === value[1]);
                expect(vector.indexOf(value)).toEqual(expected >= 0 ? expected / 2 : -1);
            }
        });
        test(`slices the entire array`, () => {
            expect(vector.slice().toArray()).toEqual(values);
        });
        test(`slice returns a TypedArray`, () => {
            expect(vector.slice().toArray()).toBeInstanceOf(type.ArrayType);
        });
        test(`slices from -20 to length`, () => {
            expect(vector.slice(-20).toArray()).toEqual(values.slice(-40));
        });
        test(`slices from 0 to -20`, () => {
            expect(vector.slice(0, -20).toArray()).toEqual(values.slice(0, -40));
        });
        test(`slices the array from 0 to length - 20`, () => {
            expect(vector.slice(0, n - 20).toArray()).toEqual(values.slice(0, values.length - 40));
        });
        test(`slices the array from 0 to length + 20`, () => {
            expect(vector.slice(0, n + 20).toArray()).toEqual(values.slice(0, values.length + 40));
        });
    });
}

for (const [VectorName, [VectorType, DataType]] of fixedWidthVectors) {
    describe(`${VectorName}`, () => {
        const type = new DataType();
        const values = concatTyped(type.ArrayType as any, ...bytes);
        const vector = bytes
            .map((b) => new type.ArrayType(b.buffer))
            .map((b) => new VectorType(new FlatData<any>(type, b.length, null, b)))
            .reduce((v: any, v2) => v.concat(v2));

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
            // Create a set of test data composed of all of the actual values
            // and a few random values
            let testValues = concatTyped(
                type.ArrayType,
                ...bytes,
                ...[randomBytes(8 * type.ArrayType.BYTES_PER_ELEMENT)]
            );

            for (const value of testValues) {
                const expected = values.indexOf(value);
                expect(vector.indexOf(value)).toEqual(expected);
            }
        });
        test(`slices the entire array`, () => {
            expect(vector.slice().toArray()).toEqual(values);
        });
        test(`slice returns a TypedArray`, () => {
            expect(vector.slice().toArray()).toBeInstanceOf(type.ArrayType);
        });
        test(`slices from -20 to length`, () => {
            expect(vector.slice(-20).toArray()).toEqual(values.slice(-20));
        });
        test(`slices from 0 to -20`, () => {
            expect(vector.slice(0, -20).toArray()).toEqual(values.slice(0, -20));
        });
        test(`slices the array from 0 to length - 20`, () => {
            expect(vector.slice(0, n - 20).toArray()).toEqual(values.slice(0, n - 20));
        });
        test(`slices the array from 0 to length + 20`, () => {
            expect(vector.slice(0, n + 20).toArray()).toEqual(values.slice(0, n + 20));
        });
    });
}

describe(`Utf8Vector`, () => {
    const values = ['foo', 'bar', 'baz', 'foo bar', 'bar'], n = values.length;
    let offset = 0;
    const offsets = Uint32Array.of(0, ...values.map((d) => { offset += d.length; return offset; }));
    const vector = new Utf8Vector(new FlatListData(new Utf8(), n, null, offsets, utf8Encoder.encode(values.join(''))));
    basicVectorTests(vector, values, ['abc', '123']);
    describe(`sliced`, () => {
        basicVectorTests(vector.slice(1,3), values.slice(1,3), ['foo', 'abc']);
    });
});

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
    let offset = 0;
    const offsets = Uint32Array.of(0, ...dictionary.map((d) => { offset += d.length; return offset; }));
    const dictionary_vec = new Utf8Vector(new FlatListData(new Utf8(), dictionary.length, null, offsets, utf8Encoder.encode(dictionary.join(''))));

    const indices = Array.from({length: 50}, () => Math.random() * 3 | 0);

    describe(`index with nullCount == 0`, () => {
        const indices_data = new FlatData(new Int32(), indices.length, new Uint8Array(0), indices);

        const values = Array.from(indices).map((d) => dictionary[d]);
        const vector = new DictionaryVector(new DictionaryData(new Dictionary(dictionary_vec.type, indices_data.type), dictionary_vec, indices_data));

        basicVectorTests(vector, values, extras);

        describe(`sliced`, () => {
            basicVectorTests(vector.slice(10, 20), values.slice(10,20), extras);
        });
    });

    describe(`index with nullCount > 0`, () => {
        const validity = Array.from({length: indices.length}, () => Math.random() > 0.2 ? true : false);
        const indices_data = new FlatData(new Int32(), indices.length, packBools(validity), indices, 0, validity.reduce((acc, d) => acc + (d ? 0 : 1), 0));
        const values = Array.from(indices).map((d, i) => validity[i] ? dictionary[d] : null);
        const vector = new DictionaryVector(new DictionaryData(new Dictionary(dictionary_vec.type, indices_data.type), dictionary_vec, indices_data));

        basicVectorTests(vector, values, ['abc', '123']);
        describe(`sliced`, () => {
            basicVectorTests(vector.slice(10, 20), values.slice(10,20), extras);
        });
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
            const expected = values.indexOf(value);
            expect(vector.indexOf(value)).toEqual(expected);
        }
    });
}

function toMap<T>(entries: Record<string, T>, keys: string[]) {
    return keys.reduce((map, key) => {
        map.set(key, entries[key] as T);
        return map;
    }, new Map<string, T>());
}

function concatTyped<T extends TypedArray>(ArrayType: TypedArrayConstructor<T>, ...bytes: any[]) {
    const BPE = ArrayType.BYTES_PER_ELEMENT;
    return bytes.reduce((v, bytes) => {
        const l = bytes.byteLength / BPE;
        const a = new ArrayType(v.length + l);
        const b = new ArrayType(bytes.buffer);
        a.set(v);
        a.set(b, v.length);
        return a;
    }, new ArrayType(0)) as T;
}