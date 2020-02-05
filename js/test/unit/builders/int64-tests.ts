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

import { util, Vector, DataType, Int64 } from '../../Arrow';
import {
    validateVector,
    encodeAll, encodeEach, encodeEachDOM, encodeEachNode,
    int64sNoNulls, int64sWithNulls, int64sWithMaxInts,
} from './utils';

const testDOMStreams = process.env.TEST_DOM_STREAMS === 'true';
const testNodeStreams = process.env.TEST_NODE_STREAMS === 'true';

const typeFactory = () => new Int64();
const valueName = `Int64`.toLowerCase();
const encode0 = encodeAll(typeFactory);
const encode1 = encodeEach(typeFactory);
const encode2 = encodeEach(typeFactory, 5);
const encode3 = encodeEach(typeFactory, 25);
const encode4 = encodeEachDOM(typeFactory, 25);
const encode5 = encodeEachNode(typeFactory, 25);

const nulls0: any[] = [0x7fffffff];
const nulls1: any[] = [0x7fffffff];
nulls0[0] = new Uint32Array([0x7fffffff, 0x7fffffff]);
nulls1[0] = util.BN.new(nulls0[0])[Symbol.toPrimitive]();

type EncodeValues<T extends DataType> = (values: (T['TValue'] | null)[], nullVals?: any[]) => Promise<Vector<T>>;

function encodeAndValidate<T extends DataType>(encode: EncodeValues<T>, providedNulls: any[] = [], expectedNulls = providedNulls) {
    return (values: any[]) => {
        return async () => {
            const vector = await encode(values, providedNulls);
            const expected = values.map((x) => {
                switch (typeof x) {
                    case 'number': return new Int32Array([x, 0]);
                    case 'bigint': return new Int32Array(new BigInt64Array([x]).buffer);
                }
                return x ? x.slice() : x;
            });
            return validateVector(expected, vector, expectedNulls);
        };
    };
}

describe(`Int64Builder`, () => {
    describe(`encode single chunk`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode0, [], [])(int64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode0, [null], [null])(int64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode0, nulls0, nulls1)(int64sWithMaxInts(20)));
    });
    describe(`encode chunks length default`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode1, [], [])(int64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode1, [null], [null])(int64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode1, nulls0, nulls1)(int64sWithMaxInts(20)));
    });
    describe(`encode chunks length 5`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode2, [], [])(int64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode2, [null], [null])(int64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode2, nulls0, nulls1)(int64sWithMaxInts(20)));
    });
    describe(`encode chunks length 25`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode3, [], [])(int64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode3, [null], [null])(int64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode3, nulls0, nulls1)(int64sWithMaxInts(20)));
    });
    testDOMStreams && describe(`encode chunks length 25, WhatWG stream`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode4, [], [])(int64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode4, [null], [null])(int64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode4, nulls0, nulls1)(int64sWithMaxInts(20)));
    });
    testNodeStreams && describe(`encode chunks length 25, NodeJS stream`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode5, [], [])(int64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode5, [null], [null])(int64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode5, nulls0, nulls1)(int64sWithMaxInts(20)));
    });
});
