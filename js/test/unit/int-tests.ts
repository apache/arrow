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

import Arrow from '../Arrow';
const { Int64, Uint64, Int128 } = Arrow.util;

describe(`Uint64`, () => {
    test(`gets expected high/low bytes`, () => {
        let i = new Uint64(new Uint32Array([5, 0]));
        expect(i.high()).toEqual(0);
        expect(i.low()).toEqual(5);
    });
    test(`adds 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([5, 0]));
        let b = new Uint64(new Uint32Array([9, 0]));
        let expected = new Uint64(new Uint32Array([14, 0]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`addition overflows 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([0xffffffff, 0]));
        let b = new Uint64(new Uint32Array([9, 0]));
        let expected = new Uint64(new Uint32Array([8, 1]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`multiplies 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([5, 0]));
        let b = new Uint64(new Uint32Array([9, 0]));
        let expected = new Uint64(new Uint32Array([45, 0]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication overflows 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([0x80000000, 0]));
        let b = new Uint64(new Uint32Array([3, 0]));
        let expected = new Uint64(new Uint32Array([0x80000000, 1]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication is associative`, () => {
        let a = new Uint64(new Uint32Array([0x80000000, 0]));
        let b = new Uint64(new Uint32Array([3, 0]));
        expect(Uint64.multiply(a, b)).toEqual(Uint64.multiply(b,a));
    });
    test(`lessThan works on 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([0x0000abcd, 0]));
        let b = new Uint64(new Uint32Array([0x0000abcf, 0]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on 64-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([123, 32]));
        let b = new Uint64(new Uint32Array([568, 32]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`fromString parses string`, () => {
        expect(Uint64.fromString('6789123456789')).toEqual(new Int64(new Uint32Array([0xb74abf15, 0x62c])));
    });
    test(`fromString parses big (full unsigned 64-bit) string`, () => {
        expect(Uint64.fromString('18364758544493064720')).toEqual(new Uint64(new Uint32Array([0x76543210, 0xfedcba98])));
    });
    test(`fromNumber converts 53-ish bit number`, () => {
        expect(Uint64.fromNumber(8086463330923024)).toEqual(new Uint64(new Uint32Array([0x76543210, 0x001cba98])));
    });
});

describe(`Int64`, () => {
    test(`gets expected high/low bytes`, () => {
        let i = new Int64(new Uint32Array([5, 0]));
        expect(i.high()).toEqual(0);
        expect(i.low()).toEqual(5);
    });
    test(`adds 32-bit numbers`, () => {
        let a = new Int64(new Uint32Array([5, 0]));
        let b = new Int64(new Uint32Array([9, 0]));
        let expected = new Int64(new Uint32Array([14, 0]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`adds negative 32-bit numbers`, () => {
        let a = new Int64(new Uint32Array([56789 ,  0]));
        let b = new Int64(new Uint32Array([-66789, -1]));
        let expected = new Int64(new Uint32Array([-10000, -1]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`addition overflows 32-bit numbers`, () => {
        let a = new Int64(new Uint32Array([0xffffffff, 0]));
        let b = new Int64(new Uint32Array([9, 0]));
        let expected = new Int64(new Uint32Array([8, 1]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`multiplies 32-bit numbers`, () => {
        let a = new Int64(new Uint32Array([5, 0]));
        let b = new Int64(new Uint32Array([9, 0]));
        let expected = new Int64(new Uint32Array([45, 0]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication overflows 32-bit numbers`, () => {
        let a = new Int64(new Uint32Array([0x80000000, 0]));
        let b = new Int64(new Uint32Array([3, 0]));
        let expected = new Int64(new Uint32Array([0x80000000, 1]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication works on negative numbers`, () => {
        let a = new Int64(new Uint32Array([-5, -1]));
        let b = new Int64(new Uint32Array([-100, -1]));
        expect(a.times(b)).toEqual(new Int64(new Uint32Array([    500,  0])));
        expect(a.times(b)).toEqual(new Int64(new Uint32Array([ -50000, -1])));
        expect(a.times(b)).toEqual(new Int64(new Uint32Array([5000000,  0])));
    });
    test(`multiplication is associative`, () => {
        let a = new Int64(new Uint32Array([0x80000000, 0]));
        let b = new Int64(new Uint32Array([3, 0]));
        expect(Int64.multiply(a, b)).toEqual(Int64.multiply(b,a));
    });
    test(`lessThan works on 32-bit numbers`, () => {
        let a = new Int64(new Uint32Array([0x0000abcd, 0]));
        let b = new Int64(new Uint32Array([0x0000abcf, 0]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on 64-bit numbers`, () => {
        let a = new Int64(new Uint32Array([123, 32]));
        let b = new Int64(new Uint32Array([568, 32]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on negative numbers`, () => {
        let a = new Int64(new Uint32Array([0,   -158]));
        let b = new Int64(new Uint32Array([-3,    -1]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on mixed numbers`, () => {
        let a = new Int64(new Uint32Array([-3, -1]));
        let b = new Int64(new Uint32Array([ 0,  3]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`negate works on 32-bit number`, () => {
        expect (new Int64(new Uint32Array([123456, 0])).negate()).toEqual(new Int64(new Uint32Array([-123456, -1])));
    });
    test(`double negation is noop`, () => {
        let test     = new Int64(new Uint32Array([6789, 12345]));
        let expected = new Int64(new Uint32Array([6789, 12345]));
        expect(test.negate().negate()).toEqual(expected);
    });
    test(`negate works on 64-bit number`, () => {
        expect (new Int64(new Uint32Array([0xb74abf15, 0x62c])).negate()).toEqual(new Int64(new Uint32Array([0x48b540eb, 0xfffff9d3])));
    });
    test(`fromString parses string`, () => {
        expect(Int64.fromString('6789123456789')).toEqual(new Int64(new Uint32Array([0xb74abf15, 0x62c])));
    });
    test(`fromString parses negative string`, () => {
        expect(Int64.fromString('-6789123456789')).toEqual(new Int64(new Uint32Array([0x48b540eb, 0xfffff9d3])));
    });
    test(`fromNumber converts 53-ish bit number`, () => {
        expect(Int64.fromNumber(8086463330923024)).toEqual(new Int64(new Uint32Array([0x76543210, 0x001cba98])));
        expect(Int64.fromNumber(-8086463330923024)).toEqual(new Int64(new Uint32Array([0x89abcdf0, 0xffe34567])));
    });
});

describe(`Int128`, () => {
    test(`gets expected bytes`, () => {
        let i = new Int128(new Uint32Array([4, 3, 2, 1]));
        expect(i.high().high()).toEqual(1);
        expect(i.high().low() ).toEqual(2);
        expect(i.low().high() ).toEqual(3);
        expect(i.low().low()  ).toEqual(4);
    });
    test(`adds 32-bit numbers`, () => {
        let a = new Int128(new Uint32Array([5, 0, 0, 0]));
        let b = new Int128(new Uint32Array([9, 0, 0, 0]));
        let expected = new Int128(new Uint32Array([14, 0, 0, 0]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`adds negative 32-bit numbers`, () => {
        let a = new Int128(new Uint32Array([56789 ,  0, 0, 0]));
        let b = new Int128(new Uint32Array([-66789, -1, -1, -1]));
        let expected = new Int128(new Uint32Array([-10000, -1, -1, -1]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`addition overflows 32-bit numbers`, () => {
        let a = new Int128(new Uint32Array([0xffffffff, 0, 0, 0]));
        let b = new Int128(new Uint32Array([9, 0, 0, 0]));
        let expected = new Int128(new Uint32Array([8, 1, 0, 0]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`multiplies 32-bit numbers`, () => {
        let a = new Int128(new Uint32Array([5, 0, 0, 0]));
        let b = new Int128(new Uint32Array([9, 0, 0, 0]));
        let expected = new Int128(new Uint32Array([45, 0, 0, 0]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication overflows 32-bit numbers`, () => {
        let a = new Int128(new Uint32Array([0x80000000, 0, 0, 0]));
        let b = new Int128(new Uint32Array([3, 0, 0, 0]));
        let expected = new Int128(new Uint32Array([0x80000000, 1, 0, 0]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication works on negative numbers`, () => {
        let a = new Int128(new Uint32Array([-5, -1, -1, -1]));
        let b = new Int128(new Uint32Array([-100, -1, -1, -1]));
        expect(a.times(b)).toEqual(new Int128(new Uint32Array([    500,   0,  0,  0])));
        expect(a.times(b)).toEqual(new Int128(new Uint32Array([ -50000,  -1, -1, -1])));
        expect(a.times(b)).toEqual(new Int128(new Uint32Array([5000000,   0,  0,  0])));
    });
    test(`multiplication is associative`, () => {
        let a = new Int128(new Uint32Array([4, 3, 2, 1]));
        let b = new Int128(new Uint32Array([3, 0, 0, 0]));
        expect(Int128.multiply(a, b)).toEqual(Int128.multiply(b,a));
    });
    test(`multiplication can produce 128-bit number`, () => {
        let a = new Int128(new Uint32Array([0, 0xf0000000, 0, 0]));
        let b = new Int128(new Uint32Array([0, 0x10000000, 0, 0]));
        expect(a.times(b)).toEqual(new Int128(new Uint32Array([0x00000000, 0x00000000, 0x00000000, 0xf000000])));
    });
    test(`fromString parses string`, () => {
        expect(Int128.fromString('1002111867823618826746863804903129070'))
            .toEqual(new Int64(new Uint32Array([0x00c0ffee,
                                               0x00c0ffee,
                                               0x00c0ffee,
                                               0x00c0ffee])));
    });
    test(`fromString parses negative string`, () => {
        expect(Int128.fromString('-12345678901234567890123456789012345678'))
            .toEqual(new Int64(new Uint32Array([0x21c70cb2,
                                                0x3bb66faf,
                                                0x0ffdccec,
                                                0xf6b64f09])));
    });
    test(`fromNumber converts 53-ish bit number`, () => {
        expect(Int128.fromNumber(8086463330923024)).toEqual(new Int128(new Uint32Array([0x76543210, 0x001cba98, 0, 0])));
        expect(Int128.fromNumber(-8086463330923024)).toEqual(new Int128(new Uint32Array([0x89abcdf0, 0xffe34567, 0xffffffff, 0xffffffff])));
    });
});
