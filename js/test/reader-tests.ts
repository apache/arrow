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
const { readVectors } = Arrow;
import { config, sources, formats } from './test-config';

describe(`readBuffers`, () => {
    for (const source of sources) {
        describe(source, () => {
            for (const format of formats) {
                describe(format, () => {
                    for (const { name, buffers } of config[source][format]) {
                        describe(name, () => {
                            testReaderIterator(buffers);
                            testVectorIterator(buffers);
                        });
                    }
                });
            }
        });
    }
});

function testReaderIterator(buffers: Uint8Array[]) {
    test(`reads each batch as an Array of Vectors`, () => {
        expect.hasAssertions();
        for (const vectors of readVectors(buffers)) {
            for (const vector of vectors) {
                expect(vector.name).toMatchSnapshot();
                expect(vector.type).toMatchSnapshot();
                expect(vector.length).toMatchSnapshot();
                for (let i = -1, n = vector.length; ++i < n;) {
                    expect(vector.get(i)).toMatchSnapshot();
                }
            }
        }
    });
}

function testVectorIterator(buffers: Uint8Array[]) {
    test(`vector iterators report the same values as get`, () => {
        expect.hasAssertions();
        for (const vectors of readVectors(buffers)) {
            for (const vector of vectors) {
                let i = -1, n = vector.length;
                for (let v of vector) {
                    expect(++i).toBeLessThan(n);
                    expect(v).toEqual(vector.get(i));
                }
                expect(++i).toEqual(n);
            }
        }
    });
}
