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
import { Vector, Utf8 } from '../../Arrow';
import {
    encodeAll,
    encodeEach,
    stringsNoNulls,
    stringsWithNAs,
    stringsWithNulls,
    stringsWithEmpties
} from './utils';

describe('Utf8Builder', () => {
    runTestsWithEncoder('encodeAll', encodeAll(() => new Utf8()));
    runTestsWithEncoder('encodeEach: 5', encodeEach(() => new Utf8(), 5));
    runTestsWithEncoder('encodeEach: 25', encodeEach(() => new Utf8(), 25));
    runTestsWithEncoder('encodeEach: undefined', encodeEach(() => new Utf8(), void 0));
});

function runTestsWithEncoder(name: string, encode: (vals: (string | null)[], nullVals?: any[]) => Vector<Utf8>) {
    describe(`${encode.name} ${name}`, () => {
        it(`encodes strings no nulls`, () => {
            const vals = stringsNoNulls(20);
            validateVector(vals, encode(vals, []), []);
        });
        it(`encodes strings with nulls`, () => {
            const vals = stringsWithNulls(20);
            validateVector(vals, encode(vals, [null]), [null]);
        });
        it(`encodes strings using n/a as the null value rep`, () => {
            const vals = stringsWithNAs(20);
            validateVector(vals, encode(vals, ['n/a']), ['n/a']);
        });
        it(`encodes strings using \\0 as the null value rep`, () => {
            const vals = stringsWithEmpties(20);
            validateVector(vals, encode(vals, ['\0']), ['\0']);
        });
    });
}
