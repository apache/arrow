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
import {
    Data, RecordBatch,
    Vector, Int32Vector, Float32Vector, Float32, Int32,
} from '../../Arrow';

function arange<T extends { length: number; [n: number]: number; }>(arr: T, n = arr.length) {
    for (let i = -1; ++i < n; arr[i] = i) { }
    return arr;
}

function numsRecordBatch(i32Len: number, f32Len: number) {
    return RecordBatch.new({
        i32: Int32Vector.from(new Int32Array(arange(new Array(i32Len)))) as Int32Vector,
        f32: Float32Vector.from(new Float32Array(arange(new Array(f32Len)))) as Float32Vector
    });
}

describe(`RecordBatch`, () => {
    describe(`new()`, () => {

        test(`creates a new RecordBatch from a Vector`, () => {

            const i32s = new Int32Array(arange(new Array<number>(10)));

            let i32 = Vector.new(Data.Int(new Int32(), 0, i32s.length, 0, null, i32s));
            expect(i32.length).toBe(i32s.length);
            expect(i32.nullCount).toBe(0);

            const batch = RecordBatch.new([i32], ['i32']);
            i32 = batch.getChildAt(0) as Int32Vector;

            expect(batch.schema.fields[0].name).toBe('i32');
            expect(i32.length).toBe(i32s.length);
            expect(i32.nullCount).toBe(0);

            expect(i32).toEqualVector(Int32Vector.from(i32s));
        });

        test(`creates a new RecordBatch from Vectors`, () => {

            const i32s = new Int32Array(arange(new Array<number>(10)));
            const f32s = new Float32Array(arange(new Array<number>(10)));

            let i32 = Vector.new(Data.Int(new Int32(), 0, i32s.length, 0, null, i32s));
            let f32 = Vector.new(Data.Float(new Float32(), 0, f32s.length, 0, null, f32s));
            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(f32s.length);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const batch = RecordBatch.new([i32, f32], ['i32', 'f32']);
            i32 = batch.getChildAt(0) as Int32Vector;
            f32 = batch.getChildAt(1) as Float32Vector;

            expect(batch.schema.fields[0].name).toBe('i32');
            expect(batch.schema.fields[1].name).toBe('f32');
            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(f32s.length);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            expect(i32).toEqualVector(Int32Vector.from(i32s));
            expect(f32).toEqualVector(Float32Vector.from(f32s));
        });

        test(`creates a new RecordBatch from Vectors with different lengths`, () => {

            const i32s = new Int32Array(arange(new Array<number>(20)));
            const f32s = new Float32Array(arange(new Array<number>(8)));

            let i32 = Int32Vector.from(i32s);
            let f32 = Float32Vector.from(f32s);

            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(f32s.length);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const batch = RecordBatch.new([i32, f32]);
            i32 = batch.getChildAt(0) as Int32Vector;
            f32 = batch.getChildAt(1) as Float32Vector;

            expect(batch.schema.fields[0].name).toBe('0');
            expect(batch.schema.fields[1].name).toBe('1');
            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(i32s.length); // new length should be the same as the longest sibling
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(i32s.length - f32s.length);

            const f32Expected = Data.Float(
                f32.type, 0, i32s.length,
                i32s.length - f32s.length,
                new Uint8Array(8).fill(255, 0, 1), f32s);

            expect(i32).toEqualVector(Int32Vector.from(i32s));
            expect(f32).toEqualVector(new Float32Vector(f32Expected));
        });
    });

    describe(`select()`, () => {
        test(`can select recordbatch children by name`, () => {
            const batch = numsRecordBatch(32, 27);
            const i32sBatch = batch.select('i32');
            expect(i32sBatch.numCols).toBe(1);
            expect(i32sBatch.length).toBe(32);
        });
    });
    describe(`selectAt()`, () => {
        test(`can select recordbatch children by index`, () => {
            const batch = numsRecordBatch(32, 45);
            const f32sBatch = batch.selectAt(1);
            expect(f32sBatch.numCols).toBe(1);
            expect(f32sBatch.length).toBe(45);
        });
    });
});
