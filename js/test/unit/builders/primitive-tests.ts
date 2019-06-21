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
    Vector, DataType,
    Bool, Int8, Int16, Int32, Uint8, Uint16, Uint32, Float16, Float32, Float64
} from '../../Arrow';

import {
    validateVector,
    encodeAll, encodeEach, encodeEachDOM, encodeEachNode,
    boolsNoNulls, boolsWithNulls,
    int8sNoNulls, int8sWithNulls, int8sWithMaxInts,
    int16sNoNulls, int16sWithNulls, int16sWithMaxInts,
    int32sNoNulls, int32sWithNulls, int32sWithMaxInts,
    uint8sNoNulls, uint8sWithNulls, uint8sWithMaxInts,
    uint16sNoNulls, uint16sWithNulls, uint16sWithMaxInts,
    uint32sNoNulls, uint32sWithNulls, uint32sWithMaxInts,
    float16sNoNulls, float16sWithNulls, float16sWithNaNs,
    float32sNoNulls, float32sWithNulls, float64sWithNaNs,
    float64sNoNulls, float64sWithNulls, float32sWithNaNs,
} from './utils';

const testDOMStreams = process.env.TEST_DOM_STREAMS === 'true';
const testNodeStreams = process.env.TEST_NODE_STREAMS === 'true';

describe('BoolBuilder', () => {

    runTestsWithEncoder('encodeAll: 5', encodeAll(() => new Bool()));
    runTestsWithEncoder('encodeEach: 5', encodeEach(() => new Bool(), 5));
    runTestsWithEncoder('encodeEach: 25', encodeEach(() => new Bool(), 25));
    runTestsWithEncoder('encodeEach: undefined', encodeEach(() => new Bool()));
    testDOMStreams && runTestsWithEncoder('encodeEachDOM: 25', encodeEachDOM(() => new Bool(), 25));
    testNodeStreams && runTestsWithEncoder('encodeEachNode: 25', encodeEachNode(() => new Bool(), 25));

    function runTestsWithEncoder<T extends DataType>(name: string, encode: (vals: (T['TValue'] | null)[], nullVals?: any[]) => Promise<Vector<T>>) {
        describe(`${encode.name} ${name}`, () => {
            it(`encodes bools no nulls`, async () => {
                const vals = boolsNoNulls(20);
                validateVector(vals, await encode(vals, []), []);
            });
            it(`encodes bools with nulls`, async () => {
                const vals = boolsWithNulls(20);
                validateVector(vals, await encode(vals, [null]), [null]);
            });
        });
    }
});

type PrimitiveTypeOpts<T extends DataType> = [
    new (...args: any[]) => T,
    (count: number) => (T['TValue'] | null)[],
    (count: number) => (T['TValue'] | null)[],
    (count: number) => (T['TValue'] | null)[]
];

[
    [Int8, int8sNoNulls, int8sWithNulls, int8sWithMaxInts] as PrimitiveTypeOpts<Int8>,
    [Int16, int16sNoNulls, int16sWithNulls, int16sWithMaxInts] as PrimitiveTypeOpts<Int16>,
    [Int32, int32sNoNulls, int32sWithNulls, int32sWithMaxInts] as PrimitiveTypeOpts<Int32>,
    [Uint8, uint8sNoNulls, uint8sWithNulls, uint8sWithMaxInts] as PrimitiveTypeOpts<Uint8>,
    [Uint16, uint16sNoNulls, uint16sWithNulls, uint16sWithMaxInts] as PrimitiveTypeOpts<Uint16>,
    [Uint32, uint32sNoNulls, uint32sWithNulls, uint32sWithMaxInts] as PrimitiveTypeOpts<Uint32>,
].forEach(([TypeCtor, noNulls, withNulls, withNaNs]) => {

    describe(`${TypeCtor.name}Builder`, () => {

        const typeFactory = () => new TypeCtor();
        const valueName = TypeCtor.name.toLowerCase();

        runTestsWithEncoder('encodeAll', encodeAll(typeFactory));
        runTestsWithEncoder('encodeEach: 5', encodeEach(typeFactory, 5));
        runTestsWithEncoder('encodeEach: 25', encodeEach(typeFactory, 25));
        runTestsWithEncoder('encodeEach: undefined', encodeEach(typeFactory));
        testDOMStreams && runTestsWithEncoder('encodeEachDOM: 25', encodeEachDOM(typeFactory, 25));
        testNodeStreams && runTestsWithEncoder('encodeEachNode: 25', encodeEachNode(typeFactory, 25));

        function runTestsWithEncoder<T extends DataType>(name: string, encode: (vals: (T['TValue'] | null)[], nullVals?: any[]) => Promise<Vector<T>>) {
            describe(`${name}`, () => {
                it(`encodes ${valueName} no nulls`, async () => {
                    const vals = noNulls(20);
                    validateVector(vals, await encode(vals, []), []);
                });
                it(`encodes ${valueName} with nulls`, async () => {
                    const vals = withNulls(20);
                    validateVector(vals, await encode(vals, [null]), [null]);
                });
                it(`encodes ${valueName} with MAX_INT`, async () => {
                    const vals = withNaNs(20);
                    validateVector(vals, await encode(vals, [0x7fffffff]), [0x7fffffff]);
                });
            });
        }
    });
});

[
    [Float16, float16sNoNulls, float16sWithNulls, float16sWithNaNs] as PrimitiveTypeOpts<Float16>,
    [Float32, float32sNoNulls, float32sWithNulls, float32sWithNaNs] as PrimitiveTypeOpts<Float32>,
    [Float64, float64sNoNulls, float64sWithNulls, float64sWithNaNs] as PrimitiveTypeOpts<Float64>,
].forEach(([TypeCtor, noNulls, withNulls, withNaNs]) => {

    describe(`${TypeCtor.name}Builder`, () => {

        const typeFactory = () => new TypeCtor();
        const valueName = TypeCtor.name.toLowerCase();

        runTestsWithEncoder('encodeAll', encodeAll(typeFactory));
        runTestsWithEncoder('encodeEach: 5', encodeEach(typeFactory, 5));
        runTestsWithEncoder('encodeEach: 25', encodeEach(typeFactory, 25));
        runTestsWithEncoder('encodeEach: undefined', encodeEach(typeFactory));
        testDOMStreams && runTestsWithEncoder('encodeEachDOM: 25', encodeEachDOM(typeFactory, 25));
        testNodeStreams && runTestsWithEncoder('encodeEachNode: 25', encodeEachNode(typeFactory, 25));

        function runTestsWithEncoder<T extends DataType>(name: string, encode: (vals: (T['TValue'] | null)[], nullVals?: any[]) => Promise<Vector<T>>) {
            describe(`${name}`, () => {
                it(`encodes ${valueName} no nulls`, async () => {
                    const vals = noNulls(20);
                    validateVector(vals, await encode(vals, []), []);
                });
                it(`encodes ${valueName} with nulls`, async () => {
                    const vals = withNulls(20);
                    validateVector(vals, await encode(vals, [null]), [null]);
                });
                it(`encodes ${valueName} with NaNs`, async () => {
                    const vals = withNaNs(20);
                    validateVector(vals, await encode(vals, [NaN]), [NaN]);
                });
            });
        }
    });
});
