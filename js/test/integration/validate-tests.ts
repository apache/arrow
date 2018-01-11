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

import * as fs from 'fs';
import * as path from 'path';

if (!process.env.JSON_PATH || !process.env.ARROW_PATH) {
    throw new Error('Integration tests need paths to both json and arrow files');
}

const jsonPath = path.resolve(process.env.JSON_PATH + '');
const arrowPath = path.resolve(process.env.ARROW_PATH + '');

if (!fs.existsSync(jsonPath) || !fs.existsSync(arrowPath)) {
    throw new Error('Integration tests need both json and arrow files to exist');
}

/* tslint:disable */
const { parse } = require('json-bignum');

const jsonData = parse(fs.readFileSync(jsonPath, 'utf8'));
const arrowBuffers: Uint8Array[] = [fs.readFileSync(arrowPath)];

import Arrow from '../Arrow';
import { zip } from 'ix/iterable/zip';
import { toArray } from 'ix/iterable/toarray';

const { Table, read } = Arrow;

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
                `${v1.name}: (${format('json', 'arrow', ' !== ')})\n`,
                ...allFailures.map(({ failures, title }) =>
                    !failures.length ? `` : [`${title}:`, ...failures].join(`\n`))
            ].join('\n')
        };
    }
});

describe(`Integration`, () => {
    testReaderIntegration();
    testTableFromBuffersIntegration();
});

function testReaderIntegration() {
    test(`json and arrow buffers report the same values`, () => {
        expect.hasAssertions();
        const jsonVectors = toArray(read(jsonData));
        const binaryVectors = toArray(read(arrowBuffers));
        for (const [jVectors, bVectors] of zip(jsonVectors, binaryVectors)) {
            expect(jVectors.length).toEqual(bVectors.length);
            for (let i = -1, n = jVectors.length; ++i < n;) {
                (expect(jVectors[i]) as any).toEqualVector(bVectors[i]);
            }
        }
    });
}

function testTableFromBuffersIntegration() {
    test(`json and arrow buffers report the same values`, () => {
        expect.hasAssertions();
        const jsonTable = Table.from(jsonData);
        const binaryTable = Table.from(arrowBuffers);
        const jsonVectors = jsonTable.columns;
        const binaryVectors = binaryTable.columns;
        expect(jsonTable.length).toEqual(binaryTable.length);
        expect(jsonVectors.length).toEqual(binaryVectors.length);
        for (let i = -1, n = jsonVectors.length; ++i < n;) {
            (expect(jsonVectors[i]) as any).toEqualVector(binaryVectors[i]);
        }
    });
}
