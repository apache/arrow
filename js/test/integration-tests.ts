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

import Arrow from './Arrow';
import { zip } from 'ix/iterable/zip';
import { config, formats } from './test-config';

const { Table, readVectors } = Arrow;

expect.extend({
    toEqualVector(v1: any, v2: any) {

        const format = (x: any, y: any, msg= ' ') => `${
            this.utils.printExpected(x)}${
                msg}${
            this.utils.printReceived(y)
        }`;

        let getFailures = new Array<string>();
        let propsFailures = new Array<string>();
        let iteratorFailures = new Array<string>();
        let allFailures = [
            { title: 'get', failures: getFailures },
            { title: 'props', failures: propsFailures },
            { title: 'iterator', failures: iteratorFailures }
        ];

        let props = ['name', 'type', 'length', 'nullable', 'nullCount', 'metadata'];
        for (let i = -1, n = props.length; ++i < n;) {
            const prop = props[i];
            if (this.utils.stringify(v1[prop]) !== this.utils.stringify(v2[prop])) {
                propsFailures.push(`${prop}: ${format(v1[prop], v2[prop], ' !== ')}`);
            }
        }

        for (let i = -1, n = v1.length; ++i < n;) {
            let x1 = v1.get(i), x2 = v2.get(i);
            if (this.utils.stringify(x1) !== this.utils.stringify(x2)) {
                getFailures.push(`${i}: ${format(x1, x2, ' !== ')}`);
            }
        }

        let i = -1;
        for (let [x1, x2] of zip(v1, v2)) {
            ++i;
            if (this.utils.stringify(x1) !== this.utils.stringify(x2)) {
                iteratorFailures.push(`${i}: ${format(x1, x2, ' !== ')}`);
            }
        }

        return {
            pass: allFailures.every(({ failures }) => failures.length === 0),
            message: () => [
                `${v1.name}: (${format('cpp', 'java', ' !== ')})\n`,
                ...allFailures.map(({ failures, title }) =>
                    !failures.length ? `` : [`${title}:`, ...failures].join(`\n`))
            ].join('\n')
        };
    }
});

describe(`Integration`, () => {
    for (const format of formats) {
        describe(format, () => {
            for (const [cppArrow, javaArrow] of zip(config.cpp[format], config.java[format])) {
                describe(`${cppArrow.name}`, () => {
                    testReaderIntegration(cppArrow.buffers, javaArrow.buffers);
                    testTableFromBuffersIntegration(cppArrow.buffers, javaArrow.buffers);
                });
            }
        });
    }
});

function testReaderIntegration(cppBuffers: Uint8Array[], javaBuffers: Uint8Array[]) {
    test(`cpp and java vectors report the same values`, () => {
        expect.hasAssertions();
        for (const [cppVectors, javaVectors] of zip(readVectors(cppBuffers), readVectors(javaBuffers))) {
            expect(cppVectors.length).toEqual(javaVectors.length);
            for (let i = -1, n = cppVectors.length; ++i < n;) {
                (expect(cppVectors[i]) as any).toEqualVector(javaVectors[i]);
            }
        }
    });
}

function testTableFromBuffersIntegration(cppBuffers: Uint8Array[], javaBuffers: Uint8Array[]) {
    test(`cpp and java tables report the same values`, () => {
        expect.hasAssertions();
        const cppTable = Table.from(cppBuffers);
        const javaTable = Table.from(javaBuffers);
        const cppVectors = cppTable.columns;
        const javaVectors = javaTable.columns;
        expect(cppTable.length).toEqual(javaTable.length);
        expect(cppVectors.length).toEqual(javaVectors.length);
        for (let i = -1, n = cppVectors.length; ++i < n;) {
            (expect(cppVectors[i]) as any).toEqualVector(javaVectors[i]);
        }
    });
}
