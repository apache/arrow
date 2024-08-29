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

import * as Arrow from 'apache-arrow';
const { BN } = Arrow.util;

describe(`BN`, () => {
    test(`to detect signed numbers, unsigned numbers and decimals`, () => {
        // SignedBigNum
        const i = new BN(new Int32Array([5, 0]));
        expect(i.signed).toBe(true);

        // UnsignedBigNum
        const u = new BN(new Uint32Array([5, 0]));
        expect(u.signed).toBe(false);

        // DecimalBigNum
        const d = new BN(new Uint16Array([1, 2, 3, 4, 5, 6, 7, 8]));
        expect(d.signed).toBe(true);
    });

    test(`toString for signed numbers`, () => {
        const i1 = new BN(new Int32Array([5, 33]), true);
        expect(i1.toString()).toBe('141733920773');

        const i2 = new BN(new Int32Array([0xFFFFFFFF, 0xFFFFFFFF]), true);
        expect(i2.toString()).toBe('-1');

        const i3 = new BN(new Int32Array([0x11111111, 0x11111111, 0x11111111]), true);
        expect(i3.toString()).toBe('5281877500950955839569596689');

        const i4 = new BN(new Int16Array([0xFFFF, 0xFFFF]), true);
        expect(i4.toString()).toBe('-1');

        const i5 = new BN(new Int32Array([0xFFFFFFFE, 0xFFFFFFFF, 0xFFFFFFFF]), true);
        expect(i5.toString()).toBe('-2');
    });

    test(`toString for unsigned numbers`, () => {
        const u1 = new BN(new Uint32Array([5, 33]), false);
        expect(u1.toString()).toBe('141733920773');

        const u2 = new BN(new Uint32Array([0xFFFFFFFF, 0xFFFFFFFF]), false);
        expect(u2.toString()).toBe('18446744073709551615');

        const u3 = new BN(new Uint32Array([0x11111111, 0x11111111, 0x11111111]), false);
        expect(u3.toString()).toBe('5281877500950955839569596689');

        const u4 = new BN(new Uint16Array([0xFFFF, 0xFFFF]), false);
        expect(u4.toString()).toBe('4294967295');
    });

    test(`toString for decimals`, () => {
        const toDecimal = (val: Uint32Array) => {
            const builder = Arrow.makeBuilder({
                type: new Arrow.Decimal(128, 0),
                nullValues: []
            });
            builder.append(val);
            return <Arrow.Type.Decimal><any>builder.finish().toVector().get(0);
        };

        const d2 = toDecimal(new Uint32Array([0xFFFFFFFE, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF]));
        expect(d2.toString()).toBe('-2');

        const d3 = toDecimal(new Uint32Array([0x00000000, 0x00000000, 0x00000000, 0x80000000]));
        expect(d3.toString()).toBe('-170141183460469231731687303715884105728');

        const d4 = toDecimal(new Uint32Array([0x9D91E773, 0x4BB90CED, 0xAB2354CC, 0x54278E9B]));
        expect(d4.toString()).toBe('111860543658909349380118287427608635251');
    });
});
