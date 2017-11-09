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
import { Table, readBuffers } from './Arrow';
import { config, formats } from './test-config';

describe.skip(`Integration`, () => {
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
        for (const [cppVectors, javaVectors] of zip(readBuffers(...cppBuffers), readBuffers(...javaBuffers))) {
            expect(cppVectors.length).toEqual(javaVectors.length);
            for (let i = -1, n = cppVectors.length; ++i < n;) {
                const cppVec = cppVectors[i];
                const javaVec = javaVectors[i];
                expect(cppVec.name).toEqual(javaVec.name);
                expect(cppVec.type).toEqual(javaVec.type);
                expect(cppVec.length).toEqual(javaVec.length);
                for (let j = -1, k = cppVec.length; ++j < k;) {
                    expect(cppVec.get(j)).toEqual(javaVec.get(i));
                }
            }
        }
    });
}

function testTableFromBuffersIntegration(cppBuffers: Uint8Array[], javaBuffers: Uint8Array[]) {
    test(`cpp and java tables report the same values`, () => {
        expect.hasAssertions();
        const cppTable = Table.from(...cppBuffers);
        const javaTable = Table.from(...javaBuffers);
        const cppVectors = cppTable.columns;
        const javaVectors = javaTable.columns;
        expect(cppTable.length).toEqual(javaTable.length);
        expect(cppVectors.length).toEqual(javaVectors.length);
        for (let i = -1, n = cppVectors.length; ++i < n;) {
            const cppVec = cppVectors[i];
            const javaVec = javaVectors[i];
            expect(cppVec.name).toEqual(javaVec.name);
            expect(cppVec.type).toEqual(javaVec.type);
            expect(cppVec.length).toEqual(javaVec.length);
            for (let j = -1, k = cppVec.length; ++j < k;) {
                expect(cppVec.get(j)).toEqual(javaVec.get(i));
            }
        }
    });
}
