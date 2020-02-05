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
import { Vector, DateDay, DateMillisecond } from '../../Arrow';
import {
    encodeAll,
    encodeEach,
    encodeEachDOM,
    encodeEachNode,
    date32sNoNulls,
    date64sNoNulls,
    date32sWithNulls,
    date64sWithNulls
} from './utils';

const testDOMStreams = process.env.TEST_DOM_STREAMS === 'true';
const testNodeStreams = process.env.TEST_NODE_STREAMS === 'true';

describe('DateDayBuilder', () => {

    runTestsWithEncoder('encodeAll', encodeAll(() => new DateDay()));
    runTestsWithEncoder('encodeEach: 5', encodeEach(() => new DateDay(), 5));
    runTestsWithEncoder('encodeEach: 25', encodeEach(() => new DateDay(), 25));
    runTestsWithEncoder('encodeEach: undefined', encodeEach(() => new DateDay()));
    testDOMStreams && runTestsWithEncoder('encodeEachDOM: 25', encodeEachDOM(() => new DateDay(), 25));
    testNodeStreams && runTestsWithEncoder('encodeEachNode: 25', encodeEachNode(() => new DateDay(), 25));

    function runTestsWithEncoder(name: string, encode: (vals: (Date | null)[], nullVals?: any[]) => Promise<Vector<DateDay>>) {
        describe(`${encode.name} ${name}`, () => {
            it(`encodes dates no nulls`, async () => {
                const vals = date32sNoNulls(20);
                validateVector(vals, await encode(vals, []), []);
            });
            it(`encodes dates with nulls`, async () => {
                const vals = date32sWithNulls(20);
                validateVector(vals, await encode(vals, [null]), [null]);
            });
        });
    }
});

describe('DateMillisecondBuilder', () => {

    runTestsWithEncoder('encodeAll', encodeAll(() => new DateMillisecond()));
    runTestsWithEncoder('encodeEach: 5', encodeEach(() => new DateMillisecond(), 5));
    runTestsWithEncoder('encodeEach: 25', encodeEach(() => new DateMillisecond(), 25));
    runTestsWithEncoder('encodeEach: undefined', encodeEach(() => new DateMillisecond()));
    testDOMStreams && runTestsWithEncoder('encodeEachDOM: 25', encodeEachDOM(() => new DateMillisecond(), 25));
    testNodeStreams && runTestsWithEncoder('encodeEachNode: 25', encodeEachNode(() => new DateMillisecond(), 25));

    function runTestsWithEncoder(name: string, encode: (vals: (Date | null)[], nullVals?: any[]) => Promise<Vector<DateMillisecond>>) {
        describe(`${encode.name} ${name}`, () => {
            it(`encodes dates no nulls`, async () => {
                const vals = date64sNoNulls(20);
                validateVector(vals, await encode(vals, []), []);
            });
            it(`encodes dates with nulls`, async () => {
                const vals = date64sWithNulls(20);
                validateVector(vals, await encode(vals, [null]), [null]);
            });
        });
    }
});

describe('DateMillisecondBuilder', () => {
    const encode = encodeAll(() => new DateMillisecond());
    const dates = [
        null,
        '2019-03-19T13:40:14.746Z',
        '2019-03-06T21:12:50.912Z',
        '2019-03-22T12:50:56.854Z',
        '2019-02-25T03:34:30.916Z',
        null,
        null,
        null,
        null,
        null,
        null,
        '2019-03-18T18:12:37.293Z',
        '2019-03-26T21:58:35.307Z',
        '2019-04-02T03:03:46.464Z',
        '2019-03-24T18:45:25.763Z',
        null,
        '2019-03-19T01:10:59.189Z',
        '2019-03-10T21:15:32.237Z',
        '2019-03-21T07:25:34.864Z',
        null
    ].map((x) => x === null ? x : new Date(x));
    it(`encodes dates with nulls`, async () => {
        const vals = dates.slice();
        validateVector(vals, await encode(vals, [null]), [null]);
    });
});
