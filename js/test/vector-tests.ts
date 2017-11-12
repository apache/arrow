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

import Arrow from './Arrow';
import {
    TypedArray,
    TypedArrayConstructor,
    NumericVectorConstructor,
} from './Arrow';

const {
    BoolVector,
    Int64Vector,
    Uint64Vector,
    Int8Vector,
    Int16Vector,
    Int32Vector,
    Uint8Vector,
    Uint16Vector,
    Uint32Vector,
    Float16Vector,
    Float32Vector,
    Float64Vector,
} = Arrow;

const FixedSizeVectors = {
    Int64Vector: [Int64Vector, Int32Array] as [NumericVectorConstructor<number, any>, any],
    Uint64Vector: [Uint64Vector, Uint32Array] as [NumericVectorConstructor<number, any>, any]
};

const FixedWidthVectors = {
    Int8Vector: [Int8Vector, Int8Array] as [NumericVectorConstructor<number, any>, any],
    Int16Vector: [Int16Vector, Int16Array] as [NumericVectorConstructor<number, any>, any],
    Int32Vector: [Int32Vector, Int32Array] as [NumericVectorConstructor<number, any>, any],
    Uint8Vector: [Uint8Vector, Uint8Array] as [NumericVectorConstructor<number, any>, any],
    Uint16Vector: [Uint16Vector, Uint16Array] as [NumericVectorConstructor<number, any>, any],
    Uint32Vector: [Uint32Vector, Uint32Array] as [NumericVectorConstructor<number, any>, any],
    Float32Vector: [Float32Vector, Float32Array] as [NumericVectorConstructor<number, any>, any],
    Float64Vector: [Float64Vector, Float64Array] as [NumericVectorConstructor<number, any>, any]
};

const fixedSizeVectors = toMap(FixedSizeVectors, Object.keys(FixedSizeVectors));
const fixedWidthVectors = toMap(FixedWidthVectors, Object.keys(FixedWidthVectors));
const bytes = Array.from(
    { length: 5 },
    () => Uint8Array.from(
        { length: 64 },
        () => Math.random() * 255 | 0));

describe(`BoolVector`, () => {
    const vector = new BoolVector({ data: new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0]) });
    const values = [true, true, false, true, true, false, false, false];
    const n = values.length;
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
    test(`can set values to true and false`, () => {
        const v = new BoolVector({ data: new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0]) });
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
        expect(BoolVector.pack([])).toEqual(
            new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0]));
    });
    test(`packs 3 values`, () => {
        expect(BoolVector.pack([
            true, false, true
        ])).toEqual(new Uint8Array([5, 0, 0, 0, 0, 0, 0, 0]));
    });
    test(`packs 8 values`, () => {
        expect(BoolVector.pack([
            true, true, false, true, true, false, false, false
        ])).toEqual(new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0]));
    });
    test(`packs 25 values`, () => {
        expect(BoolVector.pack([
            true, true, false, true, true, false, false, false,
            false, false, false, true, true, false, true, true,
            false
        ])).toEqual(new Uint8Array([27, 216, 0, 0, 0, 0, 0, 0]));
    });
    test(`from with boolean Array packs values`, () => {
        expect(new BoolVector({
            data: BoolVector.pack([true, false, true])
        }).slice()).toEqual(new Uint8Array([5, 0, 0, 0, 0, 0, 0, 0]));
    });
});

describe('Float16Vector', () => {
    const values = concatTyped(Uint16Array, ...bytes);
    const vector = bytes
        .map((b) => new Float16Vector({ data: new Uint16Array(b.buffer) }))
        .reduce((v: any, v2) => v.concat(v2));
    const n = values.length;
    const clamp = (x: number) => Math.min((x -  32767) / 32767, 1);
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
    test(`slices the entire array`, () => {
        expect(vector.slice()).toEqual(values);
    });
    test(`slice returns a TypedArray`, () => {
        expect(vector.slice()).toBeInstanceOf(Uint16Array);
    });
    test(`slices from -20 to length`, () => {
        expect(vector.slice(-20)).toEqual(values.slice(-20));
    });
    test(`slices from 0 to -20`, () => {
        expect(vector.slice(0, -20)).toEqual(values.slice(0, -20));
    });
    test(`slices the array from 0 to length - 20`, () => {
        expect(vector.slice(0, n - 20)).toEqual(values.slice(0, n - 20));
    });
    test(`slices the array from 0 to length + 20`, () => {
        expect(vector.slice(0, n + 20)).toEqual(
            concatTyped(Uint16Array, values, values.slice(0, 20)));
    });
});

for (const [VectorName, [VectorType, ArrayType]] of fixedSizeVectors) {
    describe(`${VectorName}`, () => {
        const values = concatTyped(ArrayType, ...bytes);
        const vector = bytes
            .map((b) => new VectorType({ data: new ArrayType(b.buffer) }))
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
        test(`slices the entire array`, () => {
            expect(vector.slice()).toEqual(values);
        });
        test(`slice returns a TypedArray`, () => {
            expect(vector.slice()).toBeInstanceOf(ArrayType);
        });
        test(`slices from -20 to length`, () => {
            expect(vector.slice(-20)).toEqual(values.slice(-40));
        });
        test(`slices from 0 to -20`, () => {
            expect(vector.slice(0, -20)).toEqual(values.slice(0, -40));
        });
        test(`slices the array from 0 to length - 20`, () => {
            expect(vector.slice(0, n - 20)).toEqual(values.slice(0, values.length - 40));
        });
        test(`slices the array from 0 to length + 20`, () => {
            expect(vector.slice(0, n + 20)).toEqual(
                concatTyped(ArrayType, values, values.slice(0, 40)));
        });
    });
}

for (const [VectorName, [VectorType, ArrayType]] of fixedWidthVectors) {
    describe(`${VectorName}`, () => {
        const values = concatTyped(ArrayType, ...bytes);
        const vector = bytes
            .map((b) => new VectorType({ data: new ArrayType(b.buffer) }))
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
        test(`slices the entire array`, () => {
            expect(vector.slice()).toEqual(values);
        });
        test(`slice returns a TypedArray`, () => {
            expect(vector.slice()).toBeInstanceOf(ArrayType);
        });
        test(`slices from -20 to length`, () => {
            expect(vector.slice(-20)).toEqual(values.slice(-20));
        });
        test(`slices from 0 to -20`, () => {
            expect(vector.slice(0, -20)).toEqual(values.slice(0, -20));
        });
        test(`slices the array from 0 to length - 20`, () => {
            expect(vector.slice(0, n - 20)).toEqual(values.slice(0, n - 20));
        });
        test(`slices the array from 0 to length + 20`, () => {
            expect(vector.slice(0, n + 20)).toEqual(
                concatTyped(ArrayType, values, values.slice(0, 20)));
        });
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