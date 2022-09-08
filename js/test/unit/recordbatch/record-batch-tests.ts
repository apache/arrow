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

import '../../jest-extensions.js';
import { arange } from '../utils.js';

import { RecordBatch, makeVector } from 'apache-arrow';

function numsRecordBatch(i32Len: number, f32Len: number) {
    return new RecordBatch({
        i32: makeVector(new Int32Array(arange(new Array(i32Len)))).data[0],
        f32: makeVector(new Float32Array(arange(new Array(f32Len)))).data[0],
    });
}

describe(`RecordBatch`, () => {
    describe(`new()`, () => {

        test(`creates a new RecordBatch from a Vector`, () => {

            const i32s = new Int32Array(arange(new Array<number>(10)));

            let i32 = makeVector(i32s);
            expect(i32).toHaveLength(i32s.length);
            expect(i32.nullCount).toBe(0);

            const batch = new RecordBatch({ i32: i32.data[0] });
            i32 = batch.getChildAt(0)!;

            expect(batch.schema.fields[0].name).toBe('i32');
            expect(i32).toHaveLength(i32s.length);
            expect(i32.nullCount).toBe(0);

            expect(i32).toEqualVector(makeVector(i32s));
        });

        test(`creates a new RecordBatch from Vectors`, () => {

            const i32s = new Int32Array(arange(new Array<number>(10)));
            const f32s = new Float32Array(arange(new Array<number>(10)));

            let i32 = makeVector(i32s);
            let f32 = makeVector(f32s);
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const batch = new RecordBatch({ i32: i32.data[0], f32: f32.data[0] });
            i32 = batch.getChildAt(0)!;
            f32 = batch.getChildAt(1)!;

            expect(batch.schema.fields[0].name).toBe('i32');
            expect(batch.schema.fields[1].name).toBe('f32');
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            expect(i32).toEqualVector(makeVector(i32s));
            expect(f32).toEqualVector(makeVector(f32s));
        });

        test(`creates a new RecordBatch from Vectors with different lengths`, () => {

            const i32s = new Int32Array(arange(new Array<number>(20)));
            const f32s = new Float32Array(arange(new Array<number>(8)));

            let i32 = makeVector(i32s);
            let f32 = makeVector(f32s);

            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const batch = new RecordBatch({ 0: i32.data[0], 1: f32.data[0] });
            i32 = batch.getChildAt(0)!;
            f32 = batch.getChildAt(1)!;

            expect(batch.schema.fields[0].name).toBe('0');
            expect(batch.schema.fields[1].name).toBe('1');
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(i32s.length); // new length should be the same as the longest sibling
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(i32s.length - f32s.length);

            const f32Expected = makeVector({
                type: f32.type,
                data: f32s,
                offset: 0,
                length: i32s.length,
                nullCount: i32s.length - f32s.length,
                nullBitmap: new Uint8Array(8).fill(255, 0, 1),
            });

            expect(i32).toEqualVector(makeVector(i32s));
            expect(f32).toEqualVector(f32Expected);
        });
    });

    describe(`select()`, () => {
        test(`can select recordbatch children by name`, () => {
            const batch = numsRecordBatch(32, 27);
            const i32sBatch = batch.select(['i32']);
            expect(i32sBatch.numCols).toBe(1);
            expect(i32sBatch.numRows).toBe(32);
        });
    });
    describe(`selectAt()`, () => {
        test(`can select recordbatch children by index`, () => {
            const batch = numsRecordBatch(32, 45);
            const f32sBatch = batch.selectAt([1]);
            expect(f32sBatch.numCols).toBe(1);
            expect(f32sBatch.numRows).toBe(45);
        });
    });
});
