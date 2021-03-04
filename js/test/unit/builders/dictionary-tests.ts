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

import { validateVector } from './utils';
import { Dictionary, Utf8, Int32, Vector } from '../../Arrow';
import {
    encodeAll,
    encodeEach,
    encodeEachDOM,
    encodeEachNode,
    duplicateItems,
    stringsNoNulls,
    stringsWithNAs,
    stringsWithNulls,
    stringsWithEmpties
} from './utils';

const testDOMStreams = process.env.TEST_DOM_STREAMS === 'true';
const testNodeStreams = process.env.TEST_NODE_STREAMS === 'true';

describe('DictionaryBuilder', () => {
    describe('<Utf8, Int32>', () => {
        runTestsWithEncoder('encodeAll', encodeAll(() => new Dictionary(new Utf8(), new Int32())));
        runTestsWithEncoder('encodeEach: 5', encodeEach(() => new Dictionary(new Utf8(), new Int32()), 5));
        runTestsWithEncoder('encodeEach: 25', encodeEach(() => new Dictionary(new Utf8(), new Int32()), 25));
        runTestsWithEncoder('encodeEach: undefined', encodeEach(() => new Dictionary(new Utf8(), new Int32()), void 0));
        testDOMStreams && runTestsWithEncoder('encodeEachDOM: 25', encodeEachDOM(() => new Dictionary(new Utf8(), new Int32()), 25));
        testNodeStreams && runTestsWithEncoder('encodeEachNode: 25', encodeEachNode(() => new Dictionary(new Utf8(), new Int32()), 25));
    });
});

function runTestsWithEncoder(name: string, encode: (vals: (string | null)[], nullVals?: any[]) => Promise<Vector<Dictionary<Utf8, Int32>>>) {
    describe(`${encode.name} ${name}`, () => {
        it(`dictionary-encodes strings no nulls`, async () => {
            const vals = duplicateItems(20, stringsNoNulls(10));
            validateVector(vals, await encode(vals, []), []);
        });
        it(`dictionary-encodes strings with nulls`, async () => {
            const vals = duplicateItems(20, stringsWithNulls(10));
            validateVector(vals, await encode(vals, [null]), [null]);
        });
        it(`dictionary-encodes strings using n/a as the null value rep`, async () => {
            const vals = duplicateItems(20, stringsWithNAs(10));
            validateVector(vals, await encode(vals, ['n/a']), ['n/a']);
        });
        it(`dictionary-encodes strings using \\0 as the null value rep`, async () => {
            const vals = duplicateItems(20, stringsWithEmpties(10));
            validateVector(vals, await encode(vals, ['\0']), ['\0']);
        });
    });
}
