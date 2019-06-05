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
    util,
    Data, Vector,
    Float, Float16, Float32, Float64,
    Int, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64,
    FloatVector, Float16Vector, Float32Vector, Float64Vector,
    IntVector, Int8Vector, Int16Vector, Int32Vector, Int64Vector,
    Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector,
} from '../../Arrow';

import { VectorType as V } from '../../../src/interfaces';
import { TypedArray, TypedArrayConstructor } from '../../../src/interfaces';
import { BigIntArray, BigIntArrayConstructor } from '../../../src/interfaces';

const { joinUint8Arrays } = util;
const uint16ToFloat64 = (x: number) => (x -  32767) / 32767;
const uint16ToFloat64Array = (b: ArrayBuffer) => new Float64Array([...new Uint16Array(b)].map(uint16ToFloat64));
const randomBytes = (n: number) => Uint16Array.from({ length: n / 2 }, () => (Math.random() * 65536) | 0).buffer;

const testValueBuffers = Array.from({ length: 5 }, () => randomBytes(64));
const testValuesBuffer = joinUint8Arrays(testValueBuffers.map((b) => new Uint8Array(b)))[0].buffer;

const checkType = <T, R extends T>(Ctor: new (...args: any) => T, inst: R) => expect(inst).toBeInstanceOf(Ctor);
const valuesArray = <T extends TypedArray>(ArrayType: TypedArrayConstructor<T>) => [...valuesTyped<T>(ArrayType)];
const valuesTyped = <T extends TypedArray>(ArrayType: TypedArrayConstructor<T>) => new ArrayType(testValuesBuffer);
const bigIntValuesTyped = <T extends BigIntArray>(ArrayType: BigIntArrayConstructor<T>) => new ArrayType(testValuesBuffer);
const bigIntValuesArray = <T extends BigIntArray>(ArrayType: BigIntArrayConstructor<T>) => [...bigIntValuesTyped<T>(ArrayType)];

describe(`FloatVector`, () => {

    describe(`FloatVector.from infers the type from the input TypedArray`, () => {

        const u16s = valuesTyped(Uint16Array);
        const f16s = valuesArray(Uint16Array).map(uint16ToFloat64);
        const f32s = valuesTyped(Float32Array);
        const f64s = valuesTyped(Float64Array);
        const f16Vec = FloatVector.from(u16s);
        const f32Vec = FloatVector.from(valuesTyped(Float32Array));
        const f64Vec = FloatVector.from(valuesTyped(Float64Array));

        // test strong typing at compile-time
        test(`return type is correct`, () => checkType(Float16Vector, f16Vec));
        test(`return type is correct`, () => checkType(Float32Vector, f32Vec));
        test(`return type is correct`, () => checkType(Float64Vector, f64Vec));
        test(`throws on bad input`, () => {
            expect(() => FloatVector.from(<any> {})).toThrow('Unrecognized FloatVector input');
        });

        testAndValidateVector(f16Vec, u16s, f16s);
        testAndValidateVector(f32Vec, f32s);
        testAndValidateVector(f64Vec, f64s);
    });

    describe(`Float16Vector`, () => {
        testFloatVector(Float16, valuesArray(Uint16Array).map(uint16ToFloat64));
        describe(`Float16Vector.from accepts regular Arrays`, () => {
            const u16s = valuesTyped(Uint16Array);
            const f16s = valuesArray(Uint16Array).map(uint16ToFloat64);
            const vector = Float16Vector.from(f16s);
            test(`return type is correct`, () => checkType(Float16Vector, vector));
            testAndValidateVector(vector, u16s, f16s);
        });
        describe(`Float16Vector.from accepts Uint16Arrays`, () => {
            const u16s = valuesTyped(Uint16Array);
            const f16s = valuesArray(Uint16Array).map(uint16ToFloat64);
            const vector = Float16Vector.from(u16s);
            test(`return type is correct`, () => checkType(Float16Vector, vector));
            testAndValidateVector(vector, u16s, f16s);
        });
    });
    describe(`Float32Vector`, () => {
        testFloatVector(Float32);
        describe(`Float32Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Float32Array);
            const vector = Float32Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Float32Array), values);
            test(`return type is correct`, () => checkType(Float32Vector, vector));
        });
    });
    describe(`Float64Vector`, () => {
        testFloatVector(Float64);
        describe(`Float64Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Float64Array);
            const vector = Float64Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Float64Array), values);
            test(`return type is correct`, () => checkType(Float64Vector, vector));
        });
    });
});

describe(`IntVector`, () => {

    describe(`IntVector.from infers the type from the input TypedArray`, () => {

        const i8s = valuesTyped(Int8Array);
        const i16s = valuesTyped(Int16Array);
        const i32s = valuesTyped(Int32Array);
        const i64s = valuesTyped(Int32Array);
        const u8s = valuesTyped(Uint8Array);
        const u16s = valuesTyped(Uint16Array);
        const u32s = valuesTyped(Uint32Array);
        const u64s = valuesTyped(Uint32Array);
        const i8Vec = IntVector.from(i8s);
        const i16Vec = IntVector.from(i16s);
        const i32Vec = IntVector.from(i32s);
        const i64Vec = IntVector.from(i64s, true);
        const u8Vec = IntVector.from(u8s);
        const u16Vec = IntVector.from(u16s);
        const u32Vec = IntVector.from(u32s);
        const u64Vec = IntVector.from(u64s, true);

        // test strong typing at compile-time
        test(`return type is correct`, () => checkType(Int8Vector, i8Vec));
        test(`return type is correct`, () => checkType(Int16Vector, i16Vec));
        test(`return type is correct`, () => checkType(Int32Vector, i32Vec));
        test(`return type is correct`, () => checkType(Int64Vector, i64Vec));
        test(`return type is correct`, () => checkType(Uint8Vector, u8Vec));
        test(`return type is correct`, () => checkType(Uint16Vector, u16Vec));
        test(`return type is correct`, () => checkType(Uint32Vector, u32Vec));
        test(`return type is correct`, () => checkType(Uint64Vector, u64Vec));
        test(`throws on bad input`, () => {
            expect(() => IntVector.from(<any> {})).toThrow('Unrecognized IntVector input');
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

    describe(`Int8Vector`, () => {
        testIntVector(Int8);
        describe(`Int8Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Int8Array);
            const vector = Int8Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Int8Array), values);
            test(`return type is correct`, () => checkType(Int8Vector, vector));
        });
    });
    describe(`Int16Vector`, () => {
        testIntVector(Int16);
        describe(`Int16Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Int16Array);
            const vector = Int16Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Int16Array), values);
            test(`return type is correct`, () => checkType(Int16Vector, vector));
        });
    });
    describe(`Int32Vector`, () => {
        testIntVector(Int32);
        describe(`Int32Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Int32Array);
            const vector = Int32Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Int32Array), values);
            test(`return type is correct`, () => checkType(Int32Vector, vector));
        });
    });
    describe(`Int64Vector`, () => {
        testIntVector(Int64);
        testIntVector(Int64, bigIntValuesArray(BigInt64Array));
        describe(`Int64Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Int32Array);
            const vector = Int64Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Int32Array), values);
            testAndValidateVector(vector, valuesTyped(Int32Array), bigIntValuesArray(BigInt64Array));
            test(`return type is correct`, () => checkType(Int64Vector, vector));
        });
    });
    describe(`Uint8Vector`, () => {
        testIntVector(Uint8);
        describe(`Uint8Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Uint8Array);
            const vector = Uint8Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Uint8Array), values);
            test(`return type is correct`, () => checkType(Uint8Vector, vector));
        });
    });
    describe(`Uint16Vector`, () => {
        testIntVector(Uint16);
        describe(`Uint16Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Uint16Array);
            const vector = Uint16Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Uint16Array), values);
            test(`return type is correct`, () => checkType(Uint16Vector, vector));
        });
    });
    describe(`Uint32Vector`, () => {
        testIntVector(Uint32);
        describe(`Uint32Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Uint32Array);
            const vector = Uint32Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Uint32Array), values);
            test(`return type is correct`, () => checkType(Uint32Vector, vector));
        });
    });
    describe(`Uint64Vector`, () => {
        testIntVector(Uint64);
        testIntVector(Uint64, bigIntValuesArray(BigUint64Array));
        describe(`Uint64Vector.from accepts regular Arrays`, () => {
            const values = valuesArray(Uint32Array);
            const vector = Uint64Vector.from(values);
            testAndValidateVector(vector, valuesTyped(Uint32Array), values);
            testAndValidateVector(vector, valuesTyped(Uint32Array), bigIntValuesArray(BigUint64Array));
            test(`return type is correct`, () => checkType(Uint64Vector, vector));
        });
    });
});

function testIntVector<T extends Int>(DataType: new () => T, values?: Array<any>) {

    const type = new DataType();
    const ArrayType = type.ArrayType;
    const stride = type.bitWidth < 64 ? 1 : 2;

    const typed = valuesTyped(ArrayType);
    const jsArray = values || [...typed];
    const vector = Vector.new(Data.Int(type, 0, typed.length / stride, 0, null, typed));
    const chunked = testValueBuffers.map((b) => new ArrayType(b))
        .map((b) => Vector.new(Data.Int(type, 0, b.length / stride, 0, null, b)))
        .reduce((v: any, v2) => v.concat(v2));

    const vectorBegin = (vector.length * .25) | 0;
    const vectorEnd = (vector.length * .75) | 0;
    const typedBegin = vectorBegin * (typed.length / vector.length);
    const typedEnd = vectorEnd * (typed.length / vector.length);
    const jsArrayBegin = vectorBegin * (jsArray.length / vector.length);
    const jsArrayEnd = vectorEnd * (jsArray.length / vector.length);

    const combos = [[`vector`, vector], [`chunked`, chunked]] as [string, V<T>][];
    combos.forEach(([chunksType, vector]) => {
        describe(chunksType, () => {
            // test base case no slicing
            describe(`base case no slicing`, () => testAndValidateVector(vector, typed, jsArray));
            // test slicing without args
            describe(`slicing without args`, () => testAndValidateVector(vector.slice(), typed.slice(), jsArray.slice()));
            // test slicing the middle half
            describe(`slice the middle half`, () => testAndValidateVector(
                vector.slice(vectorBegin, vectorEnd),
                typed.slice(typedBegin, typedEnd),
                jsArray.slice(jsArrayBegin, jsArrayEnd)
            ));
            // test splicing out the middle half
            describe(`splicing out the middle half`, () => testAndValidateVector(
                vector.slice(0, vectorBegin).concat(vector.slice(vectorEnd)),
                new ArrayType([...typed.slice(0, typedBegin), ...typed.slice(typedEnd)]),
                [...jsArray.slice(0, jsArrayBegin), ...jsArray.slice(jsArrayEnd)]
            ));
        });
    });
}

function testFloatVector<T extends Float>(DataType: new () => T, values?: Array<any>) {

    const type = new DataType();
    const ArrayType = type.ArrayType;

    const typed = valuesTyped(ArrayType);
    const jsArray = values || [...typed];
    const vector = Vector.new(Data.Float(type, 0, typed.length, 0, null, typed));
    const chunked = testValueBuffers.map((b) => new ArrayType(b))
        .map((b) => Vector.new(Data.Float(type, 0, b.length, 0, null, b)))
        .reduce((v: any, v2) => v.concat(v2));

    const begin = (vector.length * .25) | 0;
    const end = (vector.length * .75) | 0;
    const combos = [[`vector`, vector], [`chunked`, chunked]] as [string, V<T>][];

    combos.forEach(([chunksType, vector]) => {
        describe(chunksType, () => {
            // test base case no slicing
            describe(`base case no slicing`, () => testAndValidateVector(vector, typed, jsArray));
            // test slicing without args
            describe(`slicing without args`, () => testAndValidateVector(vector.slice(), typed.slice(), jsArray.slice()));
            // test slicing the middle half
            describe(`slice the middle half`, () => testAndValidateVector(
                vector.slice(begin, end),
                typed.slice(begin, end),
                jsArray.slice(begin, end)
            ));
            // test splicing out the middle half
            describe(`splicing out the middle half`, () => testAndValidateVector(
                vector.slice(0, begin).concat(vector.slice(end)),
                new ArrayType([...typed.slice(0, begin), ...typed.slice(end)]),
                [...jsArray.slice(0, begin), ...jsArray.slice(end)]
            ));
        });
    });
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
        let stride = vector.stride;
        try {
            if (stride === 1) {
                while (++i < n) {
                    expect(vector.get(i)).toEqual(values[i]);
                }
            } else if (typeof values[0] === 'bigint') {
                while (++i < n) {
                    const x: any = vector.get(i)!;
                    expect(0n + x).toEqual(values[i]);
                }
            } else {
                const vector64 = vector as Vector<Int64 | Uint64>;
                const ArrayType = (vector as Vector<Int64 | Uint64>).ArrayType;
                const i64 = () => new ArrayType(values.slice(stride * i, stride * (i + 1)));
                while (++i < n) {
                    expect((vector64.get(i) as any).subarray(0, stride)).toEqual(i64());
                }
            }
        } catch (e) { throw new Error(`${i}: ${e}`); }
    });
}

function iterates_expected_values<T extends Int | Float>(vector: Vector<T>, typed: T['TArray'], values: any[] = [...typed]) {
    test(`iterates expected values`, () => {
        expect.hasAssertions();
        let i = -1, n = vector.length;
        let stride = vector.stride;
        try {
            if (stride === 1) {
                for (let v of vector) {
                    expect(++i).toBeLessThan(n);
                    expect(v).toEqual(values[i]);
                }
            } else if (typeof values[0] === 'bigint') {
                let x: any;
                for (let v of vector) {
                    x = v;
                    expect(++i).toBeLessThan(n);
                    expect(0n + x).toEqual(values[i]);
                }
            } else {
                const vector64 = vector as Vector<Int64 | Uint64>;
                const ArrayType = (vector as Vector<Int64 | Uint64>).ArrayType;
                const i64 = () => new ArrayType(values.slice(stride * i, stride * (i + 1)));
                for (let v of vector64) {
                    expect(++i).toBeLessThan(n);
                    expect((v as any).subarray(0, stride)).toEqual(i64());
                }
            }
        } catch (e) { throw new Error(`${i}: ${e}`); }
    });
}

function indexof_returns_expected_values<T extends Int | Float>(vector: Vector<T>, typed: T['TArray'], values: any = [...typed]) {
    test(`indexOf returns expected values`, () => {

        expect.hasAssertions();

        const stride = vector.stride;
        const BPE = vector.ArrayType.BYTES_PER_ELEMENT;
        const isBigInt = typeof values[0] === 'bigint';
        const isInt64 = vector.type.compareTo(new Int64());
        const isFloat16 = vector.type.compareTo(new Float16());

        // Create a few random values
        let missing: any = new vector.ArrayType(randomBytes(8 * 2 * BPE));

        // Special cases convert the values and/or missing to the
        // representations that indexOf() expects to receive

        if (isFloat16) {
            missing = uint16ToFloat64Array(missing);
        } else if (isBigInt) {
            const BigIntArray = isInt64 ? BigInt64Array : BigUint64Array;
            missing = Array.from({ length: missing.length / stride },
                (_, i) => new BigIntArray(missing.buffer, BPE * stride * i, 1)[0]);
        } else if (stride !== 1) {
            values = Array.from({ length: typed.length / stride },
                (_, i) => typed.slice(stride * i, stride * (i + 1)));
            missing = Array.from({ length: missing.length / stride },
                (_, i) => missing.slice(stride * i, stride * (i + 1)));
        }

        const original = values.slice();
        // Combine with the expected values and shuffle the order
        const shuffled = shuffle(values.concat([...missing]));
        let i = -1, j: number, k: number, n = shuffled.length;

        try {
            if (!isBigInt) {
                while (++i < n) {
                    const search = shuffled[i];
                    if (typeof search !== 'number' || !isNaN(search)) {
                        expect(vector.indexOf(search)).toEqual(original.indexOf(search));
                    } else {
                        for (j = -1, k = original.length; ++j < k;) {
                            if (isNaN(original[j])) { break; }
                        }
                        expect(vector.indexOf(search)).toEqual(j < k ? j : -1);
                    }
                }
            } else {
                // Distinguish the bigint comparisons to ensure the indexOf type signature accepts bigints
                let shuffled64 = shuffled as bigint[];
                if (isInt64) {
                    let vector64 = (<unknown> vector) as Int64Vector;
                    while (++i < n) {
                        expect(vector64.indexOf(shuffled64[i])).toEqual(original.indexOf(shuffled64[i]));
                    }
                } else {
                    let vector64 = (<unknown> vector) as Uint64Vector;
                    while (++i < n) {
                        expect(vector64.indexOf(shuffled64[i])).toEqual(original.indexOf(shuffled64[i]));
                    }
                }
            }
        } catch (e) { throw new Error(`${i} (${shuffled[i]}): ${e}`); }
    });
}

function slice_returns_a_typedarray<T extends Int | Float>(vector: Vector<T>) {
    test(`slice returns a TypedArray`, () => {
        expect.hasAssertions();
        expect(vector.slice().toArray()).toBeInstanceOf(vector.ArrayType);
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
        expect(vector.slice(-20).toArray()).toEqual(values.slice(-(20 * vector.stride)));
    });
}

function slices_from_0_to_minus_20<T extends Int | Float>(vector: Vector<T>, values: T['TArray']) {
    test(`slices from 0 to -20`, () => {
        expect.hasAssertions();
        expect(vector.slice(0, -20).toArray()).toEqual(values.slice(0, -(20 * vector.stride)));
    });
}

function slices_the_array_from_0_to_length_minus_20 <T extends Int | Float>(vector: Vector<T>, values: T['TArray']) {
    test(`slices the array from 0 to length - 20`, () => {
        expect.hasAssertions();
        expect(vector.slice(0, vector.length - 20).toArray()).toEqual(values.slice(0, values.length - (20 * vector.stride)));
    });
}

function slices_the_array_from_0_to_length_plus_20<T extends Int | Float>(vector: Vector<T>, values: T['TArray']) {
    test(`slices the array from 0 to length + 20`, () => {
        expect.hasAssertions();
        expect(vector.slice(0, vector.length + 20).toArray()).toEqual(values.slice(0, values.length + (20 * vector.stride)));
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
