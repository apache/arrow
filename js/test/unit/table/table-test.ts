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

import { Bool, Dictionary, Float32, Float64, Int32, Int8, makeTable, tableFromArrays, tableFromJSON } from 'apache-arrow';

describe('makeTable()', () => {
    test(`creates a new Table from Typed Arrays`, () => {
        const i32s = Int32Array.from({ length: 10 }, (_, i) => i);
        const f32s = Float32Array.from({ length: 10 }, (_, i) => i);
        const table = makeTable({ i32s, f32s });
        const i32 = table.getChild('i32s')!;
        const f32 = table.getChild('f32s')!;
        expect(table.getChild('foo' as any)).toBeNull();

        expect(table.numRows).toBe(10);
        expect(i32.type).toBeInstanceOf(Int32);
        expect(f32.type).toBeInstanceOf(Float32);
        expect(i32).toHaveLength(10);
        expect(f32).toHaveLength(10);
        expect(i32.toArray()).toBeInstanceOf(Int32Array);
        expect(f32.toArray()).toBeInstanceOf(Float32Array);
        expect(i32.toArray()).toEqual(i32s);
        expect(f32.toArray()).toEqual(f32s);
    });
});

describe('tableFromArrays()', () => {
    test(`creates table from typed arrays and JavaScript arrays`, () => {
        const table = tableFromArrays({
            a: new Float32Array([1, 2]),
            b: new Int8Array([1, 2]),
            c: [1, 2, 3],
            d: ['foo', 'bar'],
        });

        expect(table.numRows).toBe(3);
        expect(table.numCols).toBe(4);

        expect(table.getChild('a')!.type).toBeInstanceOf(Float32);
        expect(table.getChild('b')!.type).toBeInstanceOf(Int8);
        expect(table.getChild('c')!.type).toBeInstanceOf(Float64);
        expect(table.getChild('d')!.type).toBeInstanceOf(Dictionary);
        expect(table.getChild('e' as any)).toBeNull();
    });
});


describe('tableFromJSON()', () => {
    test(`creates table from array of objects`, () => {
        const table = tableFromJSON([{
            a: 42,
            b: true,
            c: 'foo',
        }, {
            a: 12,
            b: false,
            c: 'bar',
        }]);

        expect(table.numRows).toBe(2);
        expect(table.numCols).toBe(3);

        expect(table.getChild('a')!.type).toBeInstanceOf(Float64);
        expect(table.getChild('b')!.type).toBeInstanceOf(Bool);
        expect(table.getChild('c')!.type).toBeInstanceOf(Dictionary);
    });
});
