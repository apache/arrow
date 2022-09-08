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

import 'web-streams-polyfill';

import {
    validateVector,
    encodeAll, encodeEach, encodeEachDOM, encodeEachNode,
    uint64sNoNulls, uint64sWithNulls, uint64sWithMaxInts,
} from './utils.js';

import { Vector, DataType, Uint64 } from 'apache-arrow';

const testDOMStreams = process.env.TEST_DOM_STREAMS === 'true';
const testNodeStreams = process.env.TEST_NODE_STREAMS === 'true';

const typeFactory = () => new Uint64();
const valueName = `Uint64`.toLowerCase();
const encode0 = encodeAll(typeFactory);
const encode1 = encodeEach(typeFactory);
const encode2 = encodeEach(typeFactory, 5);
const encode3 = encodeEach(typeFactory, 25);
const encode4 = encodeEachDOM(typeFactory, 25);
const encode5 = encodeEachNode(typeFactory, 25);

const MAX_INT64 = 9223372034707292159n;

type ValuesToVector<T extends DataType> = (values: (T['TValue'] | null)[], nullVals?: any[]) => Promise<Vector<T>>;

function encodeAndValidate<T extends DataType>(encode: ValuesToVector<T>, providedNulls: any[] = [], expectedNulls = providedNulls) {
    return (values: any[]) => {
        return async () => {
            const vector = await encode(values, providedNulls);
            return validateVector(values, vector, expectedNulls);
        };
    };
}

describe(`Uint64Builder`, () => {
    describe(`encode single chunk`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode0, [], [])(uint64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode0, [null])(uint64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode0, [MAX_INT64])(uint64sWithMaxInts(20)));
    });
    describe(`encode chunks length default`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode1, [], [])(uint64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode1, [null])(uint64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode1, [MAX_INT64])(uint64sWithMaxInts(20)));
    });
    describe(`encode chunks length 5`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode2, [], [])(uint64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode2, [null])(uint64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode2, [MAX_INT64])(uint64sWithMaxInts(20)));
    });
    describe(`encode chunks length 25`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode3, [], [])(uint64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode3, [null])(uint64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode3, [MAX_INT64])(uint64sWithMaxInts(20)));
    });
    testDOMStreams && describe(`encode chunks length 25, WhatWG stream`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode4, [], [])(uint64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode4, [null])(uint64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode4, [MAX_INT64])(uint64sWithMaxInts(20)));
    });
    testNodeStreams && describe(`encode chunks length 25, NodeJS stream`, () => {
        it(`encodes ${valueName} no nulls`, encodeAndValidate(encode5, [], [])(uint64sNoNulls(20)));
        it(`encodes ${valueName} with nulls`, encodeAndValidate(encode5, [null])(uint64sWithNulls(20)));
        it(`encodes ${valueName} with MAX_INT`, encodeAndValidate(encode5, [MAX_INT64])(uint64sWithMaxInts(20)));
    });
});
