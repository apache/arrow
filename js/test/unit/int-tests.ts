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
const { Int64, Uint64, Int128 } = Arrow.util;

describe(`Uint64`, () => {
    test(`gets expected high/low bytes`, () => {
        const i = new Uint64(new Uint32Array([5, 0]));
        expect(i.high()).toBe(0);
        expect(i.low()).toBe(5);
    });
    test(`adds 32-bit numbers`, () => {
        const a = new Uint64(new Uint32Array([5, 0]));
        const b = new Uint64(new Uint32Array([9, 0]));
        const expected = new Uint64(new Uint32Array([14, 0]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`addition overflows 32-bit numbers`, () => {
        const a = new Uint64(new Uint32Array([0xFFFFFFFF, 0]));
        const b = new Uint64(new Uint32Array([9, 0]));
        const expected = new Uint64(new Uint32Array([8, 1]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`multiplies 32-bit numbers`, () => {
        const a = new Uint64(new Uint32Array([5, 0]));
        const b = new Uint64(new Uint32Array([9, 0]));
        const expected = new Uint64(new Uint32Array([45, 0]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication overflows 32-bit numbers`, () => {
        const a = new Uint64(new Uint32Array([0x80000000, 0]));
        const b = new Uint64(new Uint32Array([3, 0]));
        const expected = new Uint64(new Uint32Array([0x80000000, 1]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication is associative`, () => {
        const a = new Uint64(new Uint32Array([0x80000000, 0]));
        const b = new Uint64(new Uint32Array([3, 0]));
        expect(Uint64.multiply(a, b)).toEqual(Uint64.multiply(b, a));
    });
    test(`lessThan works on 32-bit numbers`, () => {
        const a = new Uint64(new Uint32Array([0x0000ABCD, 0]));
        const b = new Uint64(new Uint32Array([0x0000ABCF, 0]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on 64-bit numbers`, () => {
        const a = new Uint64(new Uint32Array([123, 32]));
        const b = new Uint64(new Uint32Array([568, 32]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`fromString parses string`, () => {
        expect(Uint64.fromString('6789123456789')).toEqual(new Int64(new Uint32Array([0xB74ABF15, 0x62C])));
    });
    test(`fromString parses big (full unsigned 64-bit) string`, () => {
        expect(Uint64.fromString('18364758544493064720')).toEqual(new Uint64(new Uint32Array([0x76543210, 0xFEDCBA98])));
    });
    test(`fromNumber converts 53-ish bit number`, () => {
        expect(Uint64.fromNumber(8086463330923024)).toEqual(new Uint64(new Uint32Array([0x76543210, 0x001CBA98])));
    });
});

describe(`Int64`, () => {
    test(`gets expected high/low bytes`, () => {
        const i = new Int64(new Uint32Array([5, 0]));
        expect(i.high()).toBe(0);
        expect(i.low()).toBe(5);
    });
    test(`adds 32-bit numbers`, () => {
        const a = new Int64(new Uint32Array([5, 0]));
        const b = new Int64(new Uint32Array([9, 0]));
        const expected = new Int64(new Uint32Array([14, 0]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`adds negative 32-bit numbers`, () => {
        const a = new Int64(new Uint32Array([56789, 0]));
        const b = new Int64(new Uint32Array([-66789, -1]));
        const expected = new Int64(new Uint32Array([-10000, -1]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`addition overflows 32-bit numbers`, () => {
        const a = new Int64(new Uint32Array([0xFFFFFFFF, 0]));
        const b = new Int64(new Uint32Array([9, 0]));
        const expected = new Int64(new Uint32Array([8, 1]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`multiplies 32-bit numbers`, () => {
        const a = new Int64(new Uint32Array([5, 0]));
        const b = new Int64(new Uint32Array([9, 0]));
        const expected = new Int64(new Uint32Array([45, 0]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication overflows 32-bit numbers`, () => {
        const a = new Int64(new Uint32Array([0x80000000, 0]));
        const b = new Int64(new Uint32Array([3, 0]));
        const expected = new Int64(new Uint32Array([0x80000000, 1]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication works on negative numbers`, () => {
        const a = new Int64(new Uint32Array([-5, -1]));
        const b = new Int64(new Uint32Array([-100, -1]));
        expect(a.times(b)).toEqual(new Int64(new Uint32Array([500, 0])));
        expect(a.times(b)).toEqual(new Int64(new Uint32Array([-50000, -1])));
        expect(a.times(b)).toEqual(new Int64(new Uint32Array([5000000, 0])));
    });
    test(`multiplication is associative`, () => {
        const a = new Int64(new Uint32Array([0x80000000, 0]));
        const b = new Int64(new Uint32Array([3, 0]));
        expect(Int64.multiply(a, b)).toEqual(Int64.multiply(b, a));
    });
    test(`lessThan works on 32-bit numbers`, () => {
        const a = new Int64(new Uint32Array([0x0000ABCD, 0]));
        const b = new Int64(new Uint32Array([0x0000ABCF, 0]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on 64-bit numbers`, () => {
        const a = new Int64(new Uint32Array([123, 32]));
        const b = new Int64(new Uint32Array([568, 32]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on negative numbers`, () => {
        const a = new Int64(new Uint32Array([0, -158]));
        const b = new Int64(new Uint32Array([-3, -1]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on mixed numbers`, () => {
        const a = new Int64(new Uint32Array([-3, -1]));
        const b = new Int64(new Uint32Array([0, 3]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`negate works on 32-bit number`, () => {
        expect(new Int64(new Uint32Array([123456, 0])).negate()).toEqual(new Int64(new Uint32Array([-123456, -1])));
    });
    test(`double negation is noop`, () => {
        const test = new Int64(new Uint32Array([6789, 12345]));
        const expected = new Int64(new Uint32Array([6789, 12345]));
        expect(test.negate().negate()).toEqual(expected);
    });
    test(`negate works on 64-bit number`, () => {
        expect(new Int64(new Uint32Array([0xB74ABF15, 0x62C])).negate()).toEqual(new Int64(new Uint32Array([0x48B540EB, 0xFFFFF9D3])));
    });
    test(`fromString parses string`, () => {
        expect(Int64.fromString('6789123456789')).toEqual(new Int64(new Uint32Array([0xB74ABF15, 0x62C])));
    });
    test(`fromString parses negative string`, () => {
        expect(Int64.fromString('-6789123456789')).toEqual(new Int64(new Uint32Array([0x48B540EB, 0xFFFFF9D3])));
    });
    test(`fromNumber converts 53-ish bit number`, () => {
        expect(Int64.fromNumber(8086463330923024)).toEqual(new Int64(new Uint32Array([0x76543210, 0x001CBA98])));
        expect(Int64.fromNumber(-8086463330923024)).toEqual(new Int64(new Uint32Array([0x89ABCDF0, 0xFFE34567])));
    });
});

describe(`Int128`, () => {
    test(`gets expected bytes`, () => {
        const i = new Int128(new Uint32Array([4, 3, 2, 1]));
        expect(i.high().high()).toBe(1);
        expect(i.high().low()).toBe(2);
        expect(i.low().high()).toBe(3);
        expect(i.low().low()).toBe(4);
    });
    test(`adds 32-bit numbers`, () => {
        const a = new Int128(new Uint32Array([5, 0, 0, 0]));
        const b = new Int128(new Uint32Array([9, 0, 0, 0]));
        const expected = new Int128(new Uint32Array([14, 0, 0, 0]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`adds negative 32-bit numbers`, () => {
        const a = new Int128(new Uint32Array([56789, 0, 0, 0]));
        const b = new Int128(new Uint32Array([-66789, -1, -1, -1]));
        const expected = new Int128(new Uint32Array([-10000, -1, -1, -1]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`addition overflows 32-bit numbers`, () => {
        const a = new Int128(new Uint32Array([0xFFFFFFFF, 0, 0, 0]));
        const b = new Int128(new Uint32Array([9, 0, 0, 0]));
        const expected = new Int128(new Uint32Array([8, 1, 0, 0]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`multiplies 32-bit numbers`, () => {
        const a = new Int128(new Uint32Array([5, 0, 0, 0]));
        const b = new Int128(new Uint32Array([9, 0, 0, 0]));
        const expected = new Int128(new Uint32Array([45, 0, 0, 0]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication overflows 32-bit numbers`, () => {
        const a = new Int128(new Uint32Array([0x80000000, 0, 0, 0]));
        const b = new Int128(new Uint32Array([3, 0, 0, 0]));
        const expected = new Int128(new Uint32Array([0x80000000, 1, 0, 0]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication works on negative numbers`, () => {
        const a = new Int128(new Uint32Array([-5, -1, -1, -1]));
        const b = new Int128(new Uint32Array([-100, -1, -1, -1]));
        expect(a.times(b)).toEqual(new Int128(new Uint32Array([500, 0, 0, 0])));
        expect(a.times(b)).toEqual(new Int128(new Uint32Array([-50000, -1, -1, -1])));
        expect(a.times(b)).toEqual(new Int128(new Uint32Array([5000000, 0, 0, 0])));
    });
    test(`multiplication is associative`, () => {
        const a = new Int128(new Uint32Array([4, 3, 2, 1]));
        const b = new Int128(new Uint32Array([3, 0, 0, 0]));
        expect(Int128.multiply(a, b)).toEqual(Int128.multiply(b, a));
    });
    test(`multiplication can produce 128-bit number`, () => {
        const a = new Int128(new Uint32Array([0, 0xF0000000, 0, 0]));
        const b = new Int128(new Uint32Array([0, 0x10000000, 0, 0]));
        expect(a.times(b)).toEqual(new Int128(new Uint32Array([0x00000000, 0x00000000, 0x00000000, 0xF000000])));
    });
    test(`fromString parses string`, () => {
        expect(Int128.fromString('1002111867823618826746863804903129070'))
            .toEqual(new Int64(new Uint32Array([0x00C0FFEE,
                0x00C0FFEE,
                0x00C0FFEE,
                0x00C0FFEE])));
    });
    test(`fromString parses negative string`, () => {
        expect(Int128.fromString('-12345678901234567890123456789012345678'))
            .toEqual(new Int64(new Uint32Array([0x21C70CB2,
                0x3BB66FAF,
                0x0FFDCCEC,
                0xF6B64F09])));
    });
    test(`fromNumber converts 53-ish bit number`, () => {
        expect(Int128.fromNumber(8086463330923024)).toEqual(new Int128(new Uint32Array([0x76543210, 0x001CBA98, 0, 0])));
        expect(Int128.fromNumber(-8086463330923024)).toEqual(new Int128(new Uint32Array([0x89ABCDF0, 0xFFE34567, 0xFFFFFFFF, 0xFFFFFFFF])));
    });
});
