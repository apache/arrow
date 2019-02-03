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
    DataType, Data, Vector,
    Float, Float32, Float64,
    Int, Int8, Int16, Int32, Int64,
    Uint8, Uint16, Uint32, Uint64,
} from '../../Arrow';

const { joinUint8Arrays } = util;
const randomBytes = (n: number) => Uint8Array.from({ length: n }, () => Math.random() * 255 | 0);
const bytes = Array.from({ length: 5 }, () => randomBytes(64));

describe(`Int8Vector`, () => { testIntVector(Int8); });
describe(`Int16Vector`, () => { testIntVector(Int16); });
describe(`Int32Vector`, () => { testIntVector(Int32); });
describe(`Int64Vector`, () => { testIntVector(Int64); });
describe(`Uint64Vector`, () => { testIntVector(Uint64); });
describe(`Uint8Vector`, () => { testIntVector(Uint8); });
describe(`Uint16Vector`, () => { testIntVector(Uint16); });
describe(`Uint32Vector`, () => { testIntVector(Uint32); });
describe(`Float32Vector`, () => { testFloatVector(Float32); });
describe(`Float64Vector`, () => { testFloatVector(Float64); });

function testIntVector<T extends Int>(DataType: new () => T) {

    const type = new DataType();
    const stride = type.bitWidth < 64 ? 1 : 2;
    const values = new type.ArrayType(joinUint8Arrays(bytes)[0].buffer);

    const vector = bytes
        .map((b) => new type.ArrayType(b.buffer))
        .map((b) => Vector.new(Data.Int(type, 0, b.length / stride, 0, null, b)))
        .reduce((v: any, v2) => v.concat(v2));

    gets_expected_values(vector, values);
    iterates_expected_values(vector, values);
    indexof_returns_expected_values(vector, values);

    slice_returns_a_typedarray(vector);
    slices_the_entire_array(vector, values);
    slices_from_minus_20_to_length(vector, values);
    slices_from_0_to_minus_20(vector, values);
    slices_the_array_from_0_to_length_minus_20(vector, values);
    slices_the_array_from_0_to_length_plus_20(vector, values);
}

function testFloatVector<T extends Float>(DataType: new () => T) {

    const type = new DataType();
    const values = new type.ArrayType(joinUint8Arrays(bytes)[0].buffer);

    const vector = bytes
        .map((b) => new type.ArrayType(b.buffer))
        .map((b) => Vector.new(Data.Float(type, 0, b.length, 0, null, b)))
        .reduce((v: any, v2) => v.concat(v2));

    gets_expected_values(vector, values);
    iterates_expected_values(vector, values);
    indexof_returns_expected_values(vector, values);

    slice_returns_a_typedarray(vector);
    slices_the_entire_array(vector, values);
    slices_from_minus_20_to_length(vector, values);
    slices_from_0_to_minus_20(vector, values);
    slices_the_array_from_0_to_length_minus_20(vector, values);
    slices_the_array_from_0_to_length_plus_20(vector, values);
}

function gets_expected_values<T extends DataType>(vector: Vector<T>, values: T['TArray']) {
    test(`gets expected values`, () => {
        expect.hasAssertions();
        let i = -1, n = vector.length;
        let stride = vector.stride;
        try {
            if (stride === 1) {
                while (++i < n) {
                    expect(vector.get(i)).toEqual(values[i]);
                }
            } else {
                while (++i < n) {
                    expect(vector.get(i)!.subarray(0, stride))
                        .toEqual(values.slice(stride * i, stride * (i + 1)));
                }
            }
        } catch (e) { throw new Error(`${i}: ${e}`); }
    });
}

function iterates_expected_values<T extends DataType>(vector: Vector<T>, values: T['TArray']) {
    test(`iterates expected values`, () => {
        let i = -1, n = vector.length;
        let stride = vector.stride;
        try {
            if (stride === 1) {
                for (let v of vector) {
                    expect(++i).toBeLessThan(n);
                    expect(v).toEqual(values[i]);
                }
            } else {
                for (let v of vector) {
                    expect(++i).toBeLessThan(n);
                    expect(v!.subarray(0, stride))
                        .toEqual(values.slice(stride * i, stride * (i + 1)));
                }
            }
        } catch (e) { throw new Error(`${i}: ${e}`); }
    });
}

function indexof_returns_expected_values<T extends DataType>(vector: Vector<T>, values: T['TArray']) {
    test(`indexOf returns expected values`, () => {

        // Create a set of test data composed of all of the actual values and a few random values
        let testValues = new vector.ArrayType(joinUint8Arrays([
            ...bytes,
            ...[randomBytes(8 * 2 * vector.ArrayType.BYTES_PER_ELEMENT)]
        ])[0].buffer);

        let i = -1, n, stride = vector.stride;
        try {
            if (vector.stride === 1) {
                for (const value of testValues) {
                    ++i;
                    const expected = values.indexOf(value);
                    expect(vector.indexOf(value)).toEqual(expected);
                }
            } else {
                for (i = -1, n = testValues.length / stride | 0; ++i < n;) {
                    const value = testValues.slice(stride * i, stride * (i + 1));
                    const expected = values.findIndex((d: number, i: number) =>
                        i % stride === 0 && d === value[0] && testValues[i + 1] === value[1]);
                    expect(vector.indexOf(value)).toEqual(expected >= 0 ? expected / stride : -1);
                }
            }
        } catch (e) { throw new Error(`${i}: ${e}`); }
    });
}

function slice_returns_a_typedarray<T extends DataType>(vector: Vector<T>) {
    test(`slice returns a TypedArray`, () => {
        expect(vector.slice().toArray()).toBeInstanceOf(vector.ArrayType);
    });
}

function slices_the_entire_array<T extends DataType>(vector: Vector<T>, values: T['TArray']) {
    test(`slices the entire array`, () => {
        expect(vector.slice().toArray()).toEqual(values);
    });
}

function slices_from_minus_20_to_length<T extends DataType>(vector: Vector<T>, values: T['TArray']) {
    test(`slices from -20 to length`, () => {
        expect(vector.slice(-20).toArray()).toEqual(values.slice(-(20 * vector.stride)));
    });
}

function slices_from_0_to_minus_20<T extends DataType>(vector: Vector<T>, values: T['TArray']) {
    test(`slices from 0 to -20`, () => {
        expect(vector.slice(0, -20).toArray()).toEqual(values.slice(0, -(20 * vector.stride)));
    });
}

function slices_the_array_from_0_to_length_minus_20 <T extends DataType>(vector: Vector<T>, values: T['TArray']) {
    test(`slices the array from 0 to length - 20`, () => {
        expect(vector.slice(0, vector.length - 20).toArray()).toEqual(values.slice(0, values.length - (20 * vector.stride)));
    });
}

function slices_the_array_from_0_to_length_plus_20<T extends DataType>(vector: Vector<T>, values: T['TArray']) {
    test(`slices the array from 0 to length + 20`, () => {
        expect(vector.slice(0, vector.length + 20).toArray()).toEqual(values.slice(0, values.length + (20 * vector.stride)));
    });
}
