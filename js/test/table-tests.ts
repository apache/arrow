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
const { Table, readVectors } = Arrow;
import { config, sources, formats } from './test-config';

describe(`Table`, () => {
    for (const source of sources) {
        describe(source, () => {
            for (const format of formats) {
                describe(format, () => {
                    for (const { name, buffers } of config[source][format]) {
                        describe(name, () => {
                            testTableFromBuffers(buffers);
                            testColumnIterators(buffers);
                            testReaderVectorsAndTableColumns(buffers);
                            testTableRowIterator(buffers);
                            testTableRowIteratorCompact(buffers);
                            testEmptyTableToString();
                            testTableToStringPretty(buffers);
                            testTableToStringPrettyWithIndex(buffers);
                        });
                    }
                });
            }
        });
    }
});

function testTableFromBuffers(buffers: Uint8Array[]) {
    test(`creates a Table from Arrow buffers`, () => {
        expect.hasAssertions();
        const table = Table.from(buffers);
        for (const vector of table.columns) {
            expect(vector.name).toMatchSnapshot();
            expect(vector.type).toMatchSnapshot();
            expect(vector.length).toMatchSnapshot();
            for (let i = -1, n = vector.length; ++i < n;) {
                expect(vector.get(i)).toMatchSnapshot();
            }
        }
    });
}

function testColumnIterators(buffers: Uint8Array[]) {
    test(`vector iterators report the same values as get`, () => {
        expect.hasAssertions();
        const table = Table.from(buffers);
        for (const vector of table.columns) {
            let i = -1, n = vector.length;
            for (let v of vector) {
                expect(++i).toBeLessThan(n);
                expect(v).toEqual(vector.get(i));
            }
            expect(++i).toEqual(n);
        }
    });
}

function testReaderVectorsAndTableColumns(buffers: Uint8Array[]) {
    test(`batch and Table Vectors report the same values`, () => {
        expect.hasAssertions();
        let rowsTotal = 0;
        let table = Table.from(buffers);
        for (let vectors of readVectors(buffers)) {
            let rowsNow = Math.max(...vectors.map((v) => v.length));
            for (let vi = -1, vn = vectors.length; ++vi < vn;) {
                let v1 = vectors[vi];
                let v2 = table.columns[vi];
                expect(v1.name).toEqual(v2.name);
                expect(v1.type).toEqual(v2.type);
                for (let i = -1, n = v1.length; ++i < n;) {
                    expect(v1.get(i)).toEqual(v2.get(i + rowsTotal));
                }
            }
            rowsTotal += rowsNow;
        }
    });
}

function testTableRowIterator(buffers: Uint8Array[]) {
    test(`enumerates Table rows`, () => {
        expect.hasAssertions();
        const table = Table.from(buffers);
        expect(table.length).toMatchSnapshot();
        expect(table.columns.length).toMatchSnapshot();
        for (const row of table) {
            expect(row!.toObject()).toMatchSnapshot();
        }
    });
}

function testTableRowIteratorCompact(buffers: Uint8Array[]) {
    test(`enumerates Table rows compact`, () => {
        expect.hasAssertions();
        const table = Table.from(buffers);
        expect(table.length).toMatchSnapshot();
        expect(table.columns.length).toMatchSnapshot();
        for (const row of table) {
            expect(row!.toArray()).toMatchSnapshot();
        }
    });
}

function testEmptyTableToString() {
    test(`toString() prints an empty Table`, () => {
        expect(Table.from().toString()).toMatchSnapshot();
    });
}

function testTableToStringPretty(buffers: Uint8Array[]) {
    test(`toString() prints a pretty Table`, () => {
        expect(Table.from(buffers).toString()).toMatchSnapshot();
    });
}

function testTableToStringPrettyWithIndex(buffers: Uint8Array[]) {
    test(`toString({ index: true }) prints a pretty Table with an Index column`, () => {
        expect(Table.from(buffers).toString({ index: true })).toMatchSnapshot();
    });
}