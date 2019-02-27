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

import { zip } from 'ix/iterable/zip';
import { Table, Vector, RecordBatch, Column, util } from './Arrow';

declare global {
    namespace jest {
        interface Matchers<R> {
            toArrowCompare(expected: any): CustomMatcherResult;
            toEqualTable(expected: Table): CustomMatcherResult;
            toEqualRecordBatch(expected: RecordBatch): CustomMatcherResult;
            toEqualVector(expected: Vector | [Vector | null, string?, string?]): CustomMatcherResult;
        }
    }
}

expect.extend({
    toEqualTable,
    toEqualVector,
    toArrowCompare,
    toEqualRecordBatch
});

function format(jest: jest.MatcherUtils, x: any, y: any, msg= ' ') {
    return `${
        jest.utils.printExpected(x)
        }${msg}${
        jest.utils.printReceived(y)
    }`;
}

function toArrowCompare(this: jest.MatcherUtils, actual: any, expected: any) {
    if (!util.createElementComparator(expected)(actual)) {
        return { pass: false, message: () => format(this, expected, actual, ' != ') };
    }
    return { pass: true, message: () => '' };
}

function toEqualTable(this: jest.MatcherUtils, actual: Table, expected: Table) {
    const failures = [] as string[];
    try { expect(actual.length).toEqual(expected.length); } catch (e) { failures.push(`${e}`); }
    try { expect(actual.numCols).toEqual(expected.numCols); } catch (e) { failures.push(`${e}`); }
    try { expect(actual.schema.metadata).toEqual(expected.schema.metadata); } catch (e) { failures.push(`${e}`); }
    (() => {
        for (let i = -1, n = actual.numCols; ++i < n;) {
            const v1 = actual.getColumnAt(i);
            const v2 = expected.getColumnAt(i);
            const name = actual.schema.fields[i].name;
            try {
                expect([v1, `actual`, name]).toEqualVector([v2, `expected`, name]);
            } catch (e) { failures.push(`${e}`); }
        }
    })();
    return {
        pass: failures.length === 0,
        message: () => failures.join('\n'),
    };
}

function toEqualRecordBatch(this: jest.MatcherUtils, actual: RecordBatch, expected: RecordBatch) {
    const failures = [] as string[];
    try { expect(actual.length).toEqual(expected.length); } catch (e) { failures.push(`${e}`); }
    try { expect(actual.numCols).toEqual(expected.numCols); } catch (e) { failures.push(`${e}`); }
    (() => {
        for (let i = -1, n = actual.numCols; ++i < n;) {
            const v1 = actual.getChildAt(i);
            const v2 = expected.getChildAt(i);
            const name = actual.schema.fields[i].name;
            try {
                expect([v1, `actual`, name]).toEqualVector([v2, `expected`, name]);
            } catch (e) { failures.push(`${e}`); }
        }
    })();
    return {
        pass: failures.length === 0,
        message: () => failures.join('\n'),
    };
}

function toEqualVector<
    TActual extends Vector | [Vector | null, string?, string?],
    TExpected extends Vector | [Vector | null, string?]
>(this: jest.MatcherUtils, actual: TActual, expected: TExpected) {

    let [v1, format1 = '', columnName = ''] = Array.isArray(actual) ? actual : [actual];
    let [v2, format2 = ''] = Array.isArray(expected) ? expected : [expected];

    if (v1 instanceof Column && columnName === '') { columnName = v1.name; }

    if (v1 == null || v2 == null) {
        return {
            pass: false,
            message: [
                [columnName, `(${format(this, format1, format2, ' !== ')})`].filter(Boolean).join(':'),
                `${v1 == null ? 'actual' : 'expected'} is null`
            ].join('\n')
        };
    }

    let getFailures = new Array<string>();
    let propsFailures = new Array<string>();
    let iteratorFailures = new Array<string>();
    let allFailures = [
        { title: 'get', failures: getFailures },
        { title: 'props', failures: propsFailures },
        { title: 'iterator', failures: iteratorFailures }
    ];

    let props: (keyof Vector)[] = ['type', 'length', 'nullCount'];

    (() => {
        for (let i = -1, n = props.length; ++i < n;) {
            const prop = props[i];
            if (`${v1[prop]}` !== `${v2[prop]}`) {
                propsFailures.push(`${prop}: ${format(this, v1[prop], v2[prop], ' !== ')}`);
            }
        }
    })();

    (() => {
        for (let i = -1, n = v1.length; ++i < n;) {
            let x1 = v1.get(i), x2 = v2.get(i);
            if (!util.createElementComparator(x2)(x1)) {
                getFailures.push(`${i}: ${format(this, x1, x2, ' !== ')}`);
            }
        }
    })();

    (() => {
        let i = -1;
        for (let [x1, x2] of zip(v1, v2)) {
            ++i;
            if (!util.createElementComparator(x2)(x1)) {
                iteratorFailures.push(`${i}: ${format(this, x1, x2, ' !== ')}`);
            }
        }
    })();

    return {
        pass: allFailures.every(({ failures }) => failures.length === 0),
        message: () => [
            [columnName, `(${format(this, format1, format2, ' !== ')})`].filter(Boolean).join(':'),
            ...allFailures.map(({ failures, title }) =>
                !failures.length ? `` : [`${title}:`, ...failures].join(`\n`))
        ].join('\n')
    };
}
