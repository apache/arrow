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

import { flatbuffers } from 'flatbuffers';
import Long = flatbuffers.Long;
import {
    BitVector,
    TypedVector,
    Int64Vector,
    Uint64Vector,
    Int8Vector,
    Int16Vector,
    Int32Vector,
    Uint8Vector,
    Uint16Vector,
    Uint32Vector,
    Float32Vector,
    Float64Vector,
} from './Arrow';

const LongVectors = { Int64Vector, Uint64Vector };
const ByteVectors = { Int8Vector, Int16Vector, Int32Vector, Uint8Vector, Uint16Vector, Uint32Vector, Float32Vector, Float64Vector };

const longVectors = toMap<typeof TypedVector>(LongVectors, Object.keys(LongVectors));
const byteVectors = toMap<typeof TypedVector>(ByteVectors, Object.keys(ByteVectors));
const bytes = Array.from(
    { length: 5 },
    () => Uint8Array.from(
        { length: 64 },
        () => Math.random() * 255 | 0));

describe(`BitVector`, () => {
    const vector = new BitVector(new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0]));
    const values = [true, true, false, true, true, false, false, false];
    const n = values.length;
    vector.length = 1;
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
        const v = new BitVector(new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0]));
        const expected1 = [true, true, false, true, true, false, false, false];
        const expected2 = [true, true,  true, true, true, false, false, false];
        const expected3 = [true, true, false, false, false, false, true, true];
        function validate(expected) {
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
        expect(BitVector.pack([])).toEqual(
            new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0]));
    });
    test(`packs 3 values`, () => {
        expect(BitVector.pack([
            true, false, true
        ])).toEqual(new Uint8Array([5, 0, 0, 0, 0, 0, 0, 0]));
    });
    test(`packs 8 values`, () => {
        expect(BitVector.pack([
            true, true, false, true, true, false, false, false
        ])).toEqual(new Uint8Array([27, 0, 0, 0, 0, 0, 0, 0]));
    });
    test(`packs 25 values`, () => {
        expect(BitVector.pack([
            true, true, false, true, true, false, false, false,
            false, false, false, true, true, false, true, true,
            false
        ])).toEqual(new Uint8Array([27, 216, 0, 0, 0, 0, 0, 0]));
    });
    test(`from with boolean Array packs values`, () => {
        expect(BitVector.from([
            true, false, true
        ]).slice()).toEqual(new Uint8Array([5, 0, 0, 0, 0, 0, 0, 0]));
    });
});

for (const [VectorName, VectorType] of longVectors) {
    const ArrayType = VectorType.prototype.arrayType;
    describe(`${VectorName}`, () => {
        const values = concatTyped(ArrayType, ...bytes);
        const bLists = bytes.map((b) => new ArrayType(b.buffer));
        const vector = new VectorType(null, ...bLists);
        const n = vector.length = values.length * 0.5;
        test(`gets expected values`, () => {
            let i = -1;
            while (++i < n) {
                expect(vector.get(i)).toEqual(new Long(
                    values[i * 2], values[i * 2 + 1]
                ));
            }
        });
        test(`iterates expected values`, () => {
            let i = -1;
            for (let v of vector) {
                expect(++i).toBeLessThan(n);
                expect(v).toEqual(new Long(
                    values[i * 2], values[i * 2 + 1]
                ));
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

for (const [VectorName, VectorType] of byteVectors) {
    const ArrayType = VectorType.prototype.arrayType;
    describe(`${VectorName}`, () => {
        const values = concatTyped(ArrayType, ...bytes);
        const bLists = bytes.map((b) => new ArrayType(b.buffer));
        const vector = new VectorType(null, ...bLists);
        const n = vector.length = values.length;
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

function toMap<T>(entries: any, keys: string[]) {
    return keys.reduce((map, key) => {
        map.set(key, entries[key] as T);
        return map;
    }, new Map<string, T>());
}

function concatTyped(ArrayType: any, ...bytes: any[]) {
    const BPM = ArrayType.BYTES_PER_ELEMENT;
    return bytes.reduce((v, bytes) => {
        const l = bytes.byteLength / BPM;
        const a = new ArrayType(v.length + l);
        const b = new ArrayType(bytes.buffer);
        a.set(v);
        a.set(b, v.length);
        return a;
    }, new ArrayType(0)) as Array<number>;
}