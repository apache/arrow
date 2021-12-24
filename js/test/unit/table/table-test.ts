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

import { Float32, Float64, Int8, makeTable, tableFrom, Utf8 } from 'apache-arrow';

describe('makeTable()', () => {
    test(`creates table from typed arrays`, () => {
        const table = makeTable({
            a: new Float32Array([1, 2]),
            b: new Int8Array([1, 2]),
        });

        expect(table.getChild('a')!.type).toBeInstanceOf(Float32);
        expect(table.getChild('b')!.type).toBeInstanceOf(Int8);
        expect(table.getChild('c' as any)).toBeNull();
    });
});

describe('tableFrom()', () => {
    test(`creates table from typed arrays and JavaScript arrays`, () => {
        const table = tableFrom({
            a: new Float32Array([1, 2]),
            b: new Int8Array([1, 2]),
            c: [1, 2, 3],
            d: ['foo', 'bar'],
        });

        expect(table.getChild('a')!.type).toBeInstanceOf(Float32);
        expect(table.getChild('b')!.type).toBeInstanceOf(Int8);
        expect(table.getChild('c')!.type).toBeInstanceOf(Float64);
        expect(table.getChild('d')!.type).toBeInstanceOf(Utf8);
        expect(table.getChild('e' as any)).toBeNull();
    });
});
