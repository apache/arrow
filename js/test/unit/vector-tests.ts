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

import Arrow from '../Arrow';
import { type, TypedArray, TypedArrayConstructor } from '../../src/Arrow';

const { BoolData, FlatData } = Arrow.data;
const { IntVector, FloatVector, BoolVector } = Arrow.vector;
const {
    Bool,
    Float16, Float32, Float64,
    Int8, Int16, Int32, Int64,
    Uint8, Uint16, Uint32, Uint64,
} = Arrow.type;

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
const bytes = Array.from(
    { length: 5 },
    () => Uint8Array.from(
        { length: 64 },
        () => Math.random() * 255 | 0));

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
        .map((b) => new FloatVector<type.Float16>(new FlatData(new Float16(), b.length, null, b)))
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