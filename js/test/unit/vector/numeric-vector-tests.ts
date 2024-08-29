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

/* eslint-disable jest/no-identical-title */

import {
    util,
    Vector, makeVector, vectorFromArray, makeData,
    Float, Float16, Float32, Float64,
    Int, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64, DataType
} from 'apache-arrow';
import type {
    TypedArray,
    TypedArrayConstructor,
    BigIntArray,
    BigIntArrayConstructor
} from 'apache-arrow/interfaces';

const { joinUint8Arrays, float64ToUint16, uint16ToFloat64 } = util;

const uint16ToFloat64Array = (b: ArrayBuffer) => new Float64Array([...new Uint16Array(b)].map(x => uint16ToFloat64(x)));
const randomBytes = (n: number) => new Uint16Array([
    ...Uint16Array.from([0, 65_535]),
    ...Uint16Array.from({ length: (n / 2) - 2 }, () => Math.trunc(Math.random() * 65_536)),
]).buffer;

const testValueBuffers = Array.from({ length: 5 }, () => randomBytes(64));
const testValuesBuffer = joinUint8Arrays(testValueBuffers.map((b) => new Uint8Array(b)))[0].buffer;

const checkDtype = <T extends DataType>(Ctor: new (...args: any) => T, v: Vector<T>) => expect(v.type).toBeInstanceOf(Ctor);
const valuesArray = <T extends TypedArray>(ArrayType: TypedArrayConstructor<T>) => [...valuesTyped<T>(ArrayType)];
const valuesTyped = <T extends TypedArray>(ArrayType: TypedArrayConstructor<T>) => new ArrayType(testValuesBuffer);
const bigIntValuesTyped = <T extends BigIntArray>(ArrayType: BigIntArrayConstructor<T>) => new ArrayType(testValuesBuffer);
const bigIntValuesArray = <T extends BigIntArray>(ArrayType: BigIntArrayConstructor<T>) => [...bigIntValuesTyped<T>(ArrayType)];

describe(`FloatVector`, () => {

    describe(`makeVector infers the type from the input TypedArray`, () => {

        const u16s = valuesTyped(Uint16Array).map((x) => float64ToUint16(uint16ToFloat64(x)));
        const f16s = valuesArray(Uint16Array).map(x => uint16ToFloat64(x));
        const f32s = valuesTyped(Float32Array);
        const f64s = valuesTyped(Float64Array);
        const f16Vec = new Vector([makeData({ type: new Float16, data: u16s })]);
        const f32Vec = makeVector(valuesTyped(Float32Array));
        const f64Vec = makeVector(valuesTyped(Float64Array));

        // test strong typing at compile-time
        test(`return type is correct`, () => checkDtype(Float16, f16Vec));
        test(`return type is correct`, () => checkDtype(Float32, f32Vec));
        test(`return type is correct`, () => checkDtype(Float64, f64Vec));
        test(`throws on bad input`, () => {
            expect(() => makeVector(<any>{})).toThrow('Unrecognized input');
        });

        testAndValidateVector(f16Vec, u16s, f16s);
        testAndValidateVector(f32Vec, f32s);
        testAndValidateVector(f64Vec, f64s);
    });

    describe(`Vector<Float16>`, () => {
        testFloatVector(Float16, valuesArray(Uint16Array).map(x => uint16ToFloat64(x)));
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Float16>`, () => {
            const u16s = valuesTyped(Uint16Array).map((x) => float64ToUint16(uint16ToFloat64(x)));
            const f16s = valuesArray(Uint16Array).map(x => uint16ToFloat64(x));
            const vector = vectorFromArray(f16s, new Float16);
            testAndValidateVector(vector, u16s, f16s);
            test(`return type is correct`, () => checkDtype(Float16, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length * 2); });
        });
    });
    describe(`Vector<Float32>`, () => {
        testFloatVector(Float32);
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Float32>`, () => {
            const values = valuesArray(Float32Array);
            const vector = vectorFromArray(values, new Float32);
            testAndValidateVector(vector, valuesTyped(Float32Array), values);
            test(`return type is correct`, () => checkDtype(Float32, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length * 4); });
        });
    });
    describe(`Vector<Float64>`, () => {
        testFloatVector(Float64);
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Float64>`, () => {
            const values = valuesArray(Float64Array);
            const vector = vectorFromArray(values);
            testAndValidateVector(vector, valuesTyped(Float64Array), values);
            test(`return type is correct`, () => checkDtype(Float64, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length * 8); });
        });
    });
});

describe(`IntVector`, () => {

    describe(`makeVector infers the type from the input TypedArray`, () => {

        const i8s = valuesTyped(Int8Array);
        const i16s = valuesTyped(Int16Array);
        const i32s = valuesTyped(Int32Array);
        const i64s = bigIntValuesTyped(BigInt64Array);
        const u8s = valuesTyped(Uint8Array);
        const u16s = valuesTyped(Uint16Array);
        const u32s = valuesTyped(Uint32Array);
        const u64s = bigIntValuesTyped(BigUint64Array);
        const i8Vec = makeVector(i8s);
        const i16Vec = makeVector(i16s);
        const i32Vec = makeVector(i32s);
        const i64Vec = makeVector(i64s);
        const u8Vec = makeVector(u8s);
        const u16Vec = makeVector(u16s);
        const u32Vec = makeVector(u32s);
        const u64Vec = makeVector(u64s);

        // test strong typing at compile-time
        test(`return type is correct`, () => checkDtype(Int8, i8Vec));
        test(`return type is correct`, () => checkDtype(Int16, i16Vec));
        test(`return type is correct`, () => checkDtype(Int32, i32Vec));
        test(`return type is correct`, () => checkDtype(Int64, i64Vec));
        test(`return type is correct`, () => checkDtype(Uint8, u8Vec));
        test(`return type is correct`, () => checkDtype(Uint16, u16Vec));
        test(`return type is correct`, () => checkDtype(Uint32, u32Vec));
        test(`return type is correct`, () => checkDtype(Uint64, u64Vec));
        test(`throws on bad input`, () => {
            expect(() => makeVector(<any>{})).toThrow('Unrecognized input');
        });

        testAndValidateVector(i8Vec, i8s);
        testAndValidateVector(i16Vec, i16s);
        testAndValidateVector(i32Vec, i32s);
        testAndValidateVector(i64Vec, i64s);
        testAndValidateVector(u8Vec, u8s);
        testAndValidateVector(u16Vec, u16s);
        testAndValidateVector(u32Vec, u32s);
        testAndValidateVector(u64Vec, u64s);
    });

    describe(`Vector<Int8>`, () => {
        testIntVector(Int8);
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Int8>`, () => {
            const values = valuesArray(Int8Array);
            const vector = vectorFromArray(values, new Int8);
            testAndValidateVector(vector, valuesTyped(Int8Array), values);
            test(`return type is correct`, () => checkDtype(Int8, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length); });
        });
    });
    describe(`Vector<Int16>`, () => {
        testIntVector(Int16);
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Int16>`, () => {
            const values = valuesArray(Int16Array);
            const vector = vectorFromArray(values, new Int16);
            testAndValidateVector(vector, valuesTyped(Int16Array), values);
            test(`return type is correct`, () => checkDtype(Int16, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length * 2); });
        });
    });
    describe(`Vector<Int32>`, () => {
        testIntVector(Int32);
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Int32>`, () => {
            const values = valuesArray(Int32Array);
            const vector = vectorFromArray(values, new Int32);
            testAndValidateVector(vector, valuesTyped(Int32Array), values);
            test(`return type is correct`, () => checkDtype(Int32, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length * 4); });
        });
    });
    describe(`Vector<Int64>`, () => {
        testIntVector(Int64);
        testIntVector(Int64, bigIntValuesArray(BigInt64Array));
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Int64>`, () => {
            const values = bigIntValuesArray(BigInt64Array);
            const vector = vectorFromArray(values);
            testAndValidateVector(vector, bigIntValuesTyped(BigInt64Array), values);
            test(`return type is correct`, () => checkDtype(Int64, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length * 8); });
        });
    });
    describe(`Vector<Uint8>`, () => {
        testIntVector(Uint8);
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Uint8>`, () => {
            const values = valuesArray(Uint8Array);
            const vector = vectorFromArray(values, new Uint8);
            testAndValidateVector(vector, valuesTyped(Uint8Array), values);
            test(`return type is correct`, () => checkDtype(Uint8, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length); });
        });
    });
    describe(`Vector<Uint16>`, () => {
        testIntVector(Uint16);
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Uint16>`, () => {
            const values = valuesArray(Uint16Array);
            const vector = vectorFromArray(values, new Uint16);
            testAndValidateVector(vector, valuesTyped(Uint16Array), values);
            test(`return type is correct`, () => checkDtype(Uint16, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length * 2); });
        });
    });
    describe(`Vector<Uint32>`, () => {
        testIntVector(Uint32);
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Uint32>`, () => {
            const values = valuesArray(Uint32Array);
            const vector = vectorFromArray(values, new Uint32);
            testAndValidateVector(vector, valuesTyped(Uint32Array), values);
            test(`return type is correct`, () => checkDtype(Uint32, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length * 4); });
        });
    });
    describe(`Vector<Uint64>`, () => {
        testIntVector(Uint64);
        testIntVector(Uint64, bigIntValuesArray(BigUint64Array));
        describe(`vectorFromArray accepts regular Arrays to construct Vector<Uint64>`, () => {
            const values = bigIntValuesArray(BigUint64Array);
            const vector = vectorFromArray(values, new Uint64);
            testAndValidateVector(vector, bigIntValuesTyped(BigUint64Array), values);
            test(`return type is correct`, () => checkDtype(Uint64, vector));
            test(`has correct byteLength`, () => { expect(vector.byteLength).toBe(vector.length * 8); });
        });
    });
});

function testIntVector<T extends Int>(DataType: new () => T, values?: Array<any>) {

    const type = new DataType();
    const ArrayType = type.ArrayType;

    const typed: TypedArray | BigIntArray = new ArrayType(testValuesBuffer);
    const jsArray = values || [...typed];
    const vector = new Vector([makeData({ type, offset: 0, length: typed.length, nullCount: 0, nullBitmap: null, data: typed })]);
    const chunked = testValueBuffers.map((b) => new ArrayType(b))
        .map((b) => new Vector([makeData({ type, offset: 0, length: b.length, nullCount: 0, nullBitmap: null, data: b })]))
        .reduce((v: any, v2) => v.concat(v2));

    const vectorBegin = Math.trunc(vector.length * .25);
    const vectorEnd = Math.trunc(vector.length * .75);
    const typedBegin = vectorBegin * (typed.length / vector.length);
    const typedEnd = vectorEnd * (typed.length / vector.length);
    const jsArrayBegin = vectorBegin * (jsArray.length / vector.length);
    const jsArrayEnd = vectorEnd * (jsArray.length / vector.length);

    const combos = [[`vector`, vector], [`chunked`, chunked]] as [string, Vector<T>][];
    for (const [chunksType, vector] of combos) {
        describe(chunksType, () => {
            // test base case no slicing
            describe(`base case no slicing`, () => { testAndValidateVector(vector, typed, jsArray); });
            // test slicing without args
            describe(`slicing without args`, () => { testAndValidateVector(vector.slice(), typed.slice(), jsArray.slice()); });
            // test slicing the middle half
            describe(`slice the middle half`, () => {
                testAndValidateVector(
                    vector.slice(vectorBegin, vectorEnd),
                    typed.slice(typedBegin, typedEnd),
                    jsArray.slice(jsArrayBegin, jsArrayEnd)
                );
            });

            // test splicing out the middle half
            describe(`splicing out the middle half`, () => {
                testAndValidateVector(
                    vector.slice(0, vectorBegin).concat(vector.slice(vectorEnd)),
                    new ArrayType([...typed.slice(0, typedBegin), ...typed.slice(typedEnd)] as any[]),
                    [...jsArray.slice(0, jsArrayBegin), ...jsArray.slice(jsArrayEnd)]
                );
            });
        });
    }
}

function testFloatVector<T extends Float>(DataType: new () => T, values?: Array<any>) {

    const type = new DataType();
    const ArrayType = type.ArrayType;

    const typed = valuesTyped(ArrayType);
    const jsArray = values || [...typed];
    const vector = new Vector([makeData({ type, offset: 0, length: typed.length, nullCount: 0, nullBitmap: null, data: typed })]);
    const chunked = testValueBuffers.map((b) => new ArrayType(b))
        .map((b) => new Vector([makeData({ type, offset: 0, length: b.length, nullCount: 0, nullBitmap: null, data: b })]))
        .reduce((v: any, v2) => v.concat(v2));

    const begin = Math.trunc(vector.length * .25);
    const end = Math.trunc(vector.length * .75);
    const combos = [[`vector`, vector], [`chunked`, chunked]] as [string, Vector<T>][];

    for (const [chunksType, vector] of combos) {
        describe(chunksType, () => {
            // test base case no slicing
            describe(`base case no slicing`, () => { testAndValidateVector(vector, typed, jsArray); });
            // test slicing without args
            describe(`slicing without args`, () => { testAndValidateVector(vector.slice(), typed.slice(), jsArray.slice()); });
            // test slicing the middle half
            describe(`slice the middle half`, () => {
                testAndValidateVector(
                    vector.slice(begin, end),
                    typed.slice(begin, end),
                    jsArray.slice(begin, end)
                );
            });
            // test splicing out the middle half
            describe(`splicing out the middle half`, () => {
                testAndValidateVector(
                    vector.slice(0, begin).concat(vector.slice(end)),
                    new ArrayType([...typed.slice(0, begin), ...typed.slice(end)]),
                    [...jsArray.slice(0, begin), ...jsArray.slice(end)]
                );
            });
        });
    }
}

function testAndValidateVector<T extends Int | Float>(vector: Vector<T>, typed: T['TArray'], values: any[] = [...typed]) {
    gets_expected_values(vector, typed, values);
    iterates_expected_values(vector, typed, values);
    indexof_returns_expected_values(vector, typed, values);
    slice_returns_a_typedarray(vector);
    slices_the_entire_array(vector, typed);
    slices_from_minus_20_to_length(vector, typed);
    slices_from_0_to_minus_20(vector, typed);
    slices_the_array_from_0_to_length_minus_20(vector, typed);
    slices_the_array_from_0_to_length_plus_20(vector, typed);
}

function gets_expected_values<T extends Int | Float>(vector: Vector<T>, typed: T['TArray'], values: any[] = [...typed]) {
    test(`gets expected values`, () => {
        expect.hasAssertions();
        let i = -1, n = vector.length;
        try {
            while (++i < n) {
                expect(vector.get(i)).toEqual(values[i]);
            }
        } catch (e) { throw new Error(`${i}: ${e}`); }
    });
}

function iterates_expected_values<T extends Int | Float>(vector: Vector<T>, typed: T['TArray'], values: any[] = [...typed]) {
    test(`iterates expected values`, () => {
        expect.hasAssertions();
        let i = -1, n = vector.length;
        try {
            for (const v of vector) {
                expect(++i).toBeLessThan(n);
                expect(v).toEqual(values[i]);
            }
        } catch (e) { throw new Error(`${i}: ${e}`); }
    });
}

function indexof_returns_expected_values<T extends Int | Float>(vector: Vector<T>, typed: T['TArray'], values: any = [...typed]) {
    test(`indexOf returns expected values`, () => {

        expect.hasAssertions();

        const BPE = vector.ArrayType.BYTES_PER_ELEMENT;
        const isFloat16 = util.compareTypes(vector.type, new Float16());

        // Create a few random values
        let missing: any = new vector.ArrayType(randomBytes(8 * 2 * BPE));

        // Special cases convert the values and/or missing to the
        // representations that indexOf() expects to receive

        if (isFloat16) {
            missing = uint16ToFloat64Array(missing);
        }

        const original = values.slice();
        // Combine with the expected values and shuffle the order
        const shuffled = shuffle(values.concat([...missing]));
        let i = -1, j: number, k: number, n = shuffled.length;

        try {
            while (++i < n) {
                const search = shuffled[i];
                if (typeof search !== 'number' || !Number.isNaN(search)) {
                    expect(vector.indexOf(search)).toEqual(original.indexOf(search));
                } else {
                    for (j = -1, k = original.length; ++j < k;) {
                        if (Number.isNaN(original[j])) { break; }
                    }
                    expect(vector.indexOf(search)).toEqual(j < k ? j : -1);
                }
            }
        } catch (e) { throw new Error(`${i} (${shuffled[i]}): ${e}`); }
    });
}

function slice_returns_a_typedarray<T extends Int | Float>(vector: Vector<T>) {
    test(`slice returns a TypedArray`, () => {
        expect.hasAssertions();
        expect((vector.slice().toArray())).toBeInstanceOf(vector.ArrayType);
    });
}

function slices_the_entire_array<T extends Int | Float>(vector: Vector<T>, values: T['TArray']) {
    test(`slices the entire array`, () => {
        expect.hasAssertions();
        expect(vector.slice().toArray()).toEqual(values);
    });
}

function slices_from_minus_20_to_length<T extends Int | Float>(vector: Vector<T>, values: T['TArray']) {
    test(`slices from -20 to length`, () => {
        expect.hasAssertions();
        expect(vector.slice(-20).toArray()).toEqual(values.slice(-(20)));
    });
}

function slices_from_0_to_minus_20<T extends Int | Float>(vector: Vector<T>, values: T['TArray']) {
    test(`slices from 0 to -20`, () => {
        expect.hasAssertions();
        expect(vector.slice(0, -20).toArray()).toEqual(values.slice(0, -(20)));
    });
}

function slices_the_array_from_0_to_length_minus_20<T extends Int | Float>(vector: Vector<T>, values: T['TArray']) {
    test(`slices the array from 0 to length - 20`, () => {
        expect.hasAssertions();
        expect(vector.slice(0, - 20).toArray()).toEqual(values.slice(0, - 20));
    });
}

function slices_the_array_from_0_to_length_plus_20<T extends Int | Float>(vector: Vector<T>, values: T['TArray']) {
    test(`slices the array from 0 to length + 20`, () => {
        expect.hasAssertions();
        expect(vector.slice(0, vector.length + 20).toArray()).toEqual(values.slice(0, values.length + (20)));
    });
}

function shuffle(input: any[]) {
    const result = input.slice();
    let j, tmp, i = result.length;
    while (--i > 0) {
        j = Math.trunc(Math.random() * (i + 1));
        tmp = result[i];
        result[i] = result[j];
        result[j] = tmp;
    }
    return result;
}
