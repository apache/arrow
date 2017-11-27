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

import {
    Int64
    Uint64
    Int128
} from './Arrow';

describe(`Uint64`, () => {
    test(`gets expected high/low bytes`, () => {
        let i = new Uint64(new Uint32Array([0, 5]));
        expect(i.high()).toEqual(0);
        expect(i.low()).toEqual(5);
    });
    test(`adds 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([0, 5]));
        let b = new Uint64(new Uint32Array([0, 9]));
        let expected = new Uint64(new Uint32Array([0, 14]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`addition overflows 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([0, 0xffffffff]));
        let b = new Uint64(new Uint32Array([0, 9]));
        let expected = new Uint64(new Uint32Array([1, 8]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`multiplies 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([0, 5]));
        let b = new Uint64(new Uint32Array([0, 9]));
        let expected = new Uint64(new Uint32Array([0, 45]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication overflows 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([0, 0x80000000]));
        let b = new Uint64(new Uint32Array([0, 3]));
        let expected = new Uint64(new Uint32Array([1, 0x80000000]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication is associative`, () => {
        let a = new Uint64(new Uint32Array([0, 0x80000000]));
        let b = new Uint64(new Uint32Array([0, 3]));
        expect(Uint64.multiply(a, b)).toEqual(Uint64.multiply(b,a));
    });
    test(`lessThan works on 32-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([0, 0x0000abcd]));
        let b = new Uint64(new Uint32Array([0, 0x0000abcf]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on 64-bit numbers`, () => {
        let a = new Uint64(new Uint32Array([32, 123]));
        let b = new Uint64(new Uint32Array([32, 568]));
        expect(a.lessThan(b)).toBeTruthy();
    });
});

describe(`Int64`, () => {
    test(`gets expected high/low bytes`, () => {
        let i = new Int64(new Int32Array([0, 5]));
        expect(i.high()).toEqual(0);
        expect(i.low()).toEqual(5);
    });
    test(`adds 32-bit numbers`, () => {
        let a = new Int64(new Int32Array([0, 5]));
        let b = new Int64(new Int32Array([0, 9]));
        let expected = new Int64(new Int32Array([0, 14]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`adds negative 32-bit numbers`, () => {
        let a = new Int64(new Int32Array([0 ,  56789]));
        let b = new Int64(new Int32Array([-1, -66789]));
        let expected = new Int64(new Int32Array([-1, -10000]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`addition overflows 32-bit numbers`, () => {
        let a = new Int64(new Int32Array([0, 0xffffffff]));
        let b = new Int64(new Int32Array([0, 9]));
        let expected = new Int64(new Int32Array([1, 8]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`multiplies 32-bit numbers`, () => {
        let a = new Int64(new Int32Array([0, 5]));
        let b = new Int64(new Int32Array([0, 9]));
        let expected = new Int64(new Int32Array([0, 45]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication overflows 32-bit numbers`, () => {
        let a = new Int64(new Int32Array([0, 0x80000000]));
        let b = new Int64(new Int32Array([0, 3]));
        let expected = new Int64(new Int32Array([1, 0x80000000]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication works on negative numbers`, () => {
        let a = new Int64(new Int32Array([-1, -5]));
        let b = new Int64(new Int32Array([-1, -100]));
        expect(a.times(b)).toEqual(new Int64(new Int32Array([ 0,    500])));
        expect(a.times(b)).toEqual(new Int64(new Int32Array([-1,  -50000])));
        expect(a.times(b)).toEqual(new Int64(new Int32Array([ 0, 5000000])));
    });
    test(`multiplication is associative`, () => {
        let a = new Int64(new Int32Array([0, 0x80000000]));
        let b = new Int64(new Int32Array([0, 3]));
        expect(Int64.multiply(a, b)).toEqual(Int64.multiply(b,a));
    });
    test(`lessThan works on 32-bit numbers`, () => {
        let a = new Int64(new Int32Array([0, 0x0000abcd]));
        let b = new Int64(new Int32Array([0, 0x0000abcf]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on 64-bit numbers`, () => {
        let a = new Int64(new Int32Array([32, 123]));
        let b = new Int64(new Int32Array([32, 568]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on negative numbers`, () => {
        let a = new Int64(new Int32Array([-158,   0]));
        let b = new Int64(new Int32Array([-1,    -3]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`lessThan works on mixed numbers`, () => {
        let a = new Int64(new Int32Array([-1, -3]));
        let b = new Int64(new Int32Array([ 0,  3]));
        expect(a.lessThan(b)).toBeTruthy();
    });
    test(`negate works on 32-bit number`, () => {
        expect (new Int64(new Int32Array([0, 123456])).negate()).toEqual(new Int64(new Int32Array([-1, -123456])));
    });
    test(`double negation is noop`, () => {
        let test     = new Int64(new Int32Array([12345, 6789]));
        let expected = new Int64(new Int32Array([12345, 6789]));
        expect(test.negate().negate()).toEqual(expected);
    });
    test(`negate works on 64-bit number`, () => {
        expect (new Int64(new Int32Array([0x62c, 0xb74abf15])).negate()).toEqual(new Int64(new Int32Array([0xfffff9d3, 0x48b540eb])));
    });
    test(`fromString parses string`, () => {
        expect(Int64.fromString('6789123456789')).toEqual(new Int64(new Int32Array([0x62c, 0xb74abf15])));
    });
    test(`fromString parses negative string`, () => {
        expect(Int64.fromString('-6789123456789')).toEqual(new Int64(new Int32Array([0xfffff9d3, 0x48b540eb])));
    });
});

describe(`Int128`, () => {
    test(`gets expected bytes`, () => {
        let i = new Int128(new Int32Array([1, 2, 3, 4]));
        expect(i.high().high()).toEqual(1);
        expect(i.high().low() ).toEqual(2);
        expect(i.low().high() ).toEqual(3);
        expect(i.low().low()  ).toEqual(4);
    });
    test(`adds 32-bit numbers`, () => {
        let a = new Int128(new Int32Array([0, 0, 0, 5]));
        let b = new Int128(new Int32Array([0, 0, 0, 9]));
        let expected = new Int128(new Int32Array([0, 0, 0, 14]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`adds negative 32-bit numbers`, () => {
        let a = new Int128(new Int32Array([0, 0, 0 ,  56789]));
        let b = new Int128(new Int32Array([-1, -1, -1, -66789]));
        let expected = new Int128(new Int32Array([-1, -1, -1, -10000]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`addition overflows 32-bit numbers`, () => {
        let a = new Int128(new Int32Array([0, 0, 0, 0xffffffff]));
        let b = new Int128(new Int32Array([0, 0, 0, 9]));
        let expected = new Int128(new Int32Array([0, 0, 1, 8]));
        expect(a.plus(b)).toEqual(expected);
    });
    test(`multiplies 32-bit numbers`, () => {
        let a = new Int128(new Int32Array([0, 0, 0, 5]));
        let b = new Int128(new Int32Array([0, 0, 0, 9]));
        let expected = new Int128(new Int32Array([0, 0, 0, 45]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication overflows 32-bit numbers`, () => {
        let a = new Int128(new Int32Array([0, 0, 0, 0x80000000]));
        let b = new Int128(new Int32Array([0, 0, 0, 3]));
        let expected = new Int128(new Int32Array([0, 0, 1, 0x80000000]));
        expect(a.times(b)).toEqual(expected);
    });
    test(`multiplication works on negative numbers`, () => {
        let a = new Int128(new Int32Array([-1, -1, -1, -5]));
        let b = new Int128(new Int32Array([-1, -1, -1, -100]));
        expect(a.times(b)).toEqual(new Int128(new Int32Array([ 0,  0,  0,    500])));
        expect(a.times(b)).toEqual(new Int128(new Int32Array([-1, -1, -1,  -50000])));
        expect(a.times(b)).toEqual(new Int128(new Int32Array([ 0,  0,  0, 5000000])));
    });
    test(`multiplication is associative`, () => {
        let a = new Int128(new Int32Array([1, 2, 3, 4]));
        let b = new Int128(new Int32Array([0, 0, 0, 3]));
        expect(Int128.multiply(a, b)).toEqual(Int128.multiply(b,a));
    });
    test(`multiplication can produce 128-bit number`, () => {
        let a = new Int128(new Int32Array([0, 0, 0xf0000000, 0]));
        let b = new Int128(new Int32Array([0, 0, 0x10000000, 0]));
        expect(a.times(b)).toEqual(new Int128(new Int32Array([0xf000000, 0x00000000, 0x00000000, 0x00000000])));
    });
    //test(`lessThan works on 32-bit numbers`, () => {
    //    let a = new Int128(new Int32Array([0, 0, 0, 0x0000abcd]));
    //    let b = new Int128(new Int32Array([0, 0, 0, 0x0000abcf]));
    //    expect(a.lessThan(b)).toBeTruthy();
    //});
    //test(`lessThan works on 64-bit numbers`, () => {
    //    let a = new Int128(new Int32Array([0, 0, 32, 123]));
    //    let b = new Int128(new Int32Array([0, 0, 32, 568]));
    //    expect(a.lessThan(b)).toBeTruthy();
    //});
    //test(`lessThan works on negative numbers`, () => {
    //    let a = new Int128(new Int32Array([-1, -1, -158,   0]));
    //    let b = new Int128(new Int32Array([-1, -1,   -1,  -3]));
    //    expect(a.lessThan(b)).toBeTruthy();
    //});
    //test(`lessThan works on mixed numbers`, () => {
    //    let a = new Int128(new Int32Array([-1, -1, -1, -3]));
    //    let b = new Int128(new Int32Array([ 0,  0,  0,  3]));
    //    expect(a.lessThan(b)).toBeTruthy();
    //});
    test(`fromString parses string`, () => {
        expect(Int128.fromString('1002111867823618826746863804903129070'))
            .toEqual(new Int64(new Int32Array([0x00c0ffee,
                                               0x00c0ffee,
                                               0x00c0ffee,
                                               0x00c0ffee])));
    });
    test(`fromString parses negative string`, () => {
        expect(Int128.fromString('-12345678901234567890123456789012345678'))
            .toEqual(new Int64(new Int32Array([0xf6b64f09,
                                               0x0ffdccec,
                                               0x3bb66faf,
                                               0x21c70cb2])));
    });
});

//describe('Float16Vector', () => {
//    const values = concatTyped(Uint16Array, ...bytes);
//    const vector = bytes
//        .map((b) => new Float16Vector({ data: new Uint16Array(b.buffer) }))
//        .reduce((v: any, v2) => v.concat(v2));
//    const n = values.length;
//    const clamp = (x: number) => Math.min((x -  32767) / 32767, 1);
//    test(`gets expected values`, () => {
//        let i = -1;
//        while (++i < n) {
//            expect(vector.get(i)).toEqual(clamp(values[i]));
//        }
//    });
//    test(`iterates expected values`, () => {
//        expect.hasAssertions();
//        let i = -1;
//        for (let v of vector) {
//            expect(++i).toBeLessThan(n);
//            expect(v).toEqual(clamp(values[i]));
//        }
//    });
//    test(`slices the entire array`, () => {
//        expect(vector.slice()).toEqual(values);
//    });
//    test(`slice returns a TypedArray`, () => {
//        expect(vector.slice()).toBeInstanceOf(Uint16Array);
//    });
//    test(`slices from -20 to length`, () => {
//        expect(vector.slice(-20)).toEqual(values.slice(-20));
//    });
//    test(`slices from 0 to -20`, () => {
//        expect(vector.slice(0, -20)).toEqual(values.slice(0, -20));
//    });
//    test(`slices the array from 0 to length - 20`, () => {
//        expect(vector.slice(0, n - 20)).toEqual(values.slice(0, n - 20));
//    });
//    test(`slices the array from 0 to length + 20`, () => {
//        expect(vector.slice(0, n + 20)).toEqual(
//            concatTyped(Uint16Array, values, values.slice(0, 20)));
//    });
//});
//
//for (const [VectorName, [VectorType, ArrayType]] of fixedSizeVectors) {
//    describe(`${VectorName}`, () => {
//        const values = concatTyped(ArrayType, ...bytes);
//        const vector = bytes
//            .map((b) => new VectorType({ data: new ArrayType(b.buffer) }))
//            .reduce((v: any, v2) => v.concat(v2));
//        const n = values.length * 0.5;
//        test(`gets expected values`, () => {
//            let i = -1;
//            while (++i < n) {
//                expect(vector.get(i)).toEqual(values.slice(2 * i, 2 * (i + 1)));
//            }
//        });
//        test(`iterates expected values`, () => {
//            let i = -1;
//            for (let v of vector) {
//                expect(++i).toBeLessThan(n);
//                expect(v).toEqual(values.slice(2 * i, 2 * (i + 1)));
//            }
//        });
//        test(`slices the entire array`, () => {
//            expect(vector.slice()).toEqual(values);
//        });
//        test(`slice returns a TypedArray`, () => {
//            expect(vector.slice()).toBeInstanceOf(ArrayType);
//        });
//        test(`slices from -20 to length`, () => {
//            expect(vector.slice(-20)).toEqual(values.slice(-40));
//        });
//        test(`slices from 0 to -20`, () => {
//            expect(vector.slice(0, -20)).toEqual(values.slice(0, -40));
//        });
//        test(`slices the array from 0 to length - 20`, () => {
//            expect(vector.slice(0, n - 20)).toEqual(values.slice(0, values.length - 40));
//        });
//        test(`slices the array from 0 to length + 20`, () => {
//            expect(vector.slice(0, n + 20)).toEqual(
//                concatTyped(ArrayType, values, values.slice(0, 40)));
//        });
//    });
//}
//
//for (const [VectorName, [VectorType, ArrayType]] of fixedWidthVectors) {
//    describe(`${VectorName}`, () => {
//        const values = concatTyped(ArrayType, ...bytes);
//        const vector = bytes
//            .map((b) => new VectorType({ data: new ArrayType(b.buffer) }))
//            .reduce((v: any, v2) => v.concat(v2));
//
//        const n = values.length;
//        test(`gets expected values`, () => {
//            let i = -1;
//            while (++i < n) {
//                expect(vector.get(i)).toEqual(values[i]);
//            }
//        });
//        test(`iterates expected values`, () => {
//            expect.hasAssertions();
//            let i = -1;
//            for (let v of vector) {
//                expect(++i).toBeLessThan(n);
//                expect(v).toEqual(values[i]);
//            }
//        });
//        test(`slices the entire array`, () => {
//            expect(vector.slice()).toEqual(values);
//        });
//        test(`slice returns a TypedArray`, () => {
//            expect(vector.slice()).toBeInstanceOf(ArrayType);
//        });
//        test(`slices from -20 to length`, () => {
//            expect(vector.slice(-20)).toEqual(values.slice(-20));
//        });
//        test(`slices from 0 to -20`, () => {
//            expect(vector.slice(0, -20)).toEqual(values.slice(0, -20));
//        });
//        test(`slices the array from 0 to length - 20`, () => {
//            expect(vector.slice(0, n - 20)).toEqual(values.slice(0, n - 20));
//        });
//        test(`slices the array from 0 to length + 20`, () => {
//            expect(vector.slice(0, n + 20)).toEqual(
//                concatTyped(ArrayType, values, values.slice(0, 20)));
//        });
//    });
//}
//
//function toMap<T>(entries: Record<string, T>, keys: string[]) {
//    return keys.reduce((map, key) => {
//        map.set(key, entries[key] as T);
//        return map;
//    }, new Map<string, T>());
//}
//
//function concatTyped<T extends TypedArray>(ArrayType: TypedArrayConstructor<T>, ...bytes: any[]) {
//    const BPE = ArrayType.BYTES_PER_ELEMENT;
//    return bytes.reduce((v, bytes) => {
//        const l = bytes.byteLength / BPE;
//        const a = new ArrayType(v.length + l);
//        const b = new ArrayType(bytes.buffer);
//        a.set(v);
//        a.set(b, v.length);
//        return a;
//    }, new ArrayType(0)) as T;
//}
