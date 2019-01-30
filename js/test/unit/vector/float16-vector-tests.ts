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

import { Data, Float16, Vector, util } from '../../Arrow';
const { joinUint8Arrays } = util;

const newFloat16Vector = (length: number, data: Uint16Array) => Vector.new(Data.Float(new Float16(), 0, length, 0, null, data));
const randomBytes = (n: number) => Uint8Array.from({ length: n }, () => Math.random() * 255 | 0);
const bytes = Array.from({ length: 5 }, () => randomBytes(64));

describe('Float16Vector', () => {
    const values = new Uint16Array(joinUint8Arrays(bytes)[0].buffer);
    const vector = bytes
        .map((b) => new Uint16Array(b.buffer))
        .map((b) => newFloat16Vector(b.length, b))
        .reduce((v: any, v2) => v.concat(v2));
    const n = values.length;
    const clamp = (x: number) => (x -  32767) / 32767;
    const float16s = new Float32Array([...values].map((x) => clamp(x)));
    test(`gets expected values`, () => {
        let i = -1;
        while (++i < n) {
            expect(vector.get(i)).toEqual(clamp(values[i]));
        }
    });
    test(`iterates expected values`, () => {
        expect.hasAssertions();
        let i = -1;
        for (let v of vector) {
            expect(++i).toBeLessThan(n);
            expect(v).toEqual(clamp(values[i]));
        }
    });
    test(`indexOf returns expected values`, () => {
        const randomValues = new Uint16Array(randomBytes(64).buffer);
        for (let value of [...values, ...randomValues]) {
            const expected = values.indexOf(value);
            expect(vector.indexOf(clamp(value))).toEqual(expected);
        }
    });
    test(`slices the entire array`, () => {
        expect(vector.slice().toArray()).toEqual(float16s);
    });
    test(`slice returns a TypedArray`, () => {
        expect(vector.slice().toArray()).toBeInstanceOf(Float32Array);
    });
    test(`slices from -20 to length`, () => {
        expect(vector.slice(-20).toArray()).toEqual(float16s.slice(-20));
    });
    test(`slices from 0 to -20`, () => {
        expect(vector.slice(0, -20).toArray()).toEqual(float16s.slice(0, -20));
    });
    test(`slices the array from 0 to length - 20`, () => {
        expect(vector.slice(0, n - 20).toArray()).toEqual(float16s.slice(0, n - 20));
    });
    test(`slices the array from 0 to length + 20`, () => {
        expect(vector.slice(0, n + 20).toArray()).toEqual(float16s.slice(0, n + 20));
    });
});
