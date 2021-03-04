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

import '../../jest-extensions';
import { zip } from 'ix/iterable';
import * as generate from '../../generate-test-data';
import { validateTable } from '../generated-data-validators';
import {
    Schema, Field, DataType, Int32, Float32, Utf8
} from '../../Arrow';

const toSchema = (...xs: [string, DataType][]) => new Schema(xs.map((x) => new Field(...x)));
const schema1             = toSchema(['a', new Int32()], ['b', new Float32()], ['c', new Utf8()]);
const partialOverlapWith1 = toSchema(['a', new Int32()], ['b', new Float32()], ['f', new Utf8()]);
const schema2             = toSchema(['d', new Int32()], ['e', new Float32()], ['f', new Utf8()]);

describe('Table.assign()', () => {
    describe(`should assign non-overlapping fields`, () => {
        const lhs = generate.table([20], schema1);
        const rhs = generate.table([20], schema2);
        const table = lhs.table.assign(rhs.table);
        const f = assignGeneratedTables(lhs, rhs);
        expect(table.schema.fields.map((f) => f.name)).toEqual(['a', 'b', 'c', 'd', 'e', 'f']);
        validateTable({ ...f([0,1,2], [3,4,5]), table }).run();
    });
    describe(`should assign partially-overlapping fields`, () => {
        const lhs = generate.table([20], schema1);
        const rhs = generate.table([20], partialOverlapWith1);
        const table = lhs.table.assign(rhs.table);
        const f = assignGeneratedTables(lhs, rhs);
        expect(table.schema.fields.map((f) => f.name)).toEqual(['a', 'b', 'c', 'f']);
        validateTable({ ...f([ , , 2], [0,1,3]), table }).run();
    });
    describe(`should assign completely-overlapping fields`, () => {
        const lhs = generate.table([20], schema2);
        const rhs = generate.table([20], schema2);
        const table = lhs.table.assign(rhs.table);
        const f = assignGeneratedTables(lhs, rhs);
        expect(table.schema.fields.map((f) => f.name)).toEqual(['d', 'e', 'f']);
        validateTable({ ...f([ , , ], [0,1,2]), table }).run();
    });
});

function assignGeneratedTables(lhs: generate.GeneratedTable, rhs: generate.GeneratedTable) {
    return function createAssignedTestData(lhsIndices: any[], rhsIndices: any[]) {
        const pluckLhs = (xs: any[], ys: any[] = []) => lhsIndices.reduce((ys, i, j) => {
            if (i !== undefined) { ys[i] = xs ? xs[j] : null; }
            return ys;
        }, ys);
        const pluckRhs = (xs: any[], ys: any[] = []) => rhsIndices.reduce((ys, i, j) => {
            if (i !== undefined) { ys[i] = xs ? xs[j] : null; }
            return ys;
        }, ys);
        const cols = () => [...pluckLhs(lhs.cols(), pluckRhs(rhs.cols()))];
        const keys = () => [...pluckLhs(lhs.keys(), pluckRhs(rhs.keys()))];
        const rows = () => [...zip(lhs.rows(), rhs.rows())].map(([x, y]) => [...pluckLhs(x, pluckRhs(y))]);
        const colBatches = [...zip(lhs.colBatches, rhs.colBatches)].map(([x, y]) => () => [...pluckLhs(x(), pluckRhs(y()))]);
        const keyBatches = [...zip(lhs.keyBatches, rhs.keyBatches)].map(([x, y]) => () => [...pluckLhs(x(), pluckRhs(y()))]);
        const rowBatches = [...zip(lhs.rowBatches, rhs.rowBatches)].map(([x, y]) => () => [...zip(x(), y())].map(([x, y]) => [...pluckLhs(x, pluckRhs(y))]));
        return { cols, keys, rows, colBatches, keyBatches, rowBatches };
    };
}
