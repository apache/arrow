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

import '../jest-extensions.js';

import { arange } from './utils.js';

import {
    makeData, makeVector,
    Schema, Field, Table, RecordBatch,
    Vector, builderThroughIterable,
    Float32, Int32, Dictionary, Utf8, Int8,
    tableFromIPC, tableToIPC
} from 'apache-arrow';

const deepCopy = (t: Table) => tableFromIPC(tableToIPC(t));

const F32 = 0, I32 = 1, DICT = 2;

export const test_data = [
    {
        name: `single record batch`,
        table: getSingleRecordBatchTable,
        // Use Math.fround to coerce to float32
        values: () => [
            [Math.fround(-0.3), -1, 'a'],
            [Math.fround(-0.2), 1, 'b'],
            [Math.fround(-0.1), -1, 'c'],
            [Math.fround(0), 1, 'a'],
            [Math.fround(0.1), -1, 'b'],
            [Math.fround(0.2), 1, 'c'],
            [Math.fround(0.3), -1, 'a']
        ]
    }, {
        name: `multiple record batches`,
        table: getMultipleRecordBatchesTable,
        values: () => [
            [Math.fround(-0.3), -1, 'a'],
            [Math.fround(-0.2), 1, 'b'],
            [Math.fround(-0.1), -1, 'c'],
            [Math.fround(0), 1, 'a'],
            [Math.fround(0.1), -1, 'b'],
            [Math.fround(0.2), 1, 'c'],
            [Math.fround(0.3), -1, 'a'],
            [Math.fround(0.2), 1, 'b'],
            [Math.fround(0.1), -1, 'c'],
        ]
    }, {
        name: `struct`,
        table: () => {
            const struct = getStructTable().getChild('struct')!;
            const schema = new Schema<TestDataSchema>(struct.type.children);
            const chunks = struct.data.map((data) => new RecordBatch(schema, data));
            return new Table(schema, chunks);
        },
        // Use Math.fround to coerce to float32
        values: () => [
            [Math.fround(-0.3), -1, 'a'],
            [Math.fround(-0.2), 1, 'b'],
            [Math.fround(-0.1), -1, 'c'],
            [Math.fround(0), 1, 'a'],
            [Math.fround(0.1), -1, 'b'],
            [Math.fround(0.2), 1, 'c'],
            [Math.fround(0.3), -1, 'a']
        ]
    },
];

function compareBatchAndTable(source: Table, offset: number, batch: RecordBatch, table: Table) {
    expect(batch.numRows).toEqual(table.numRows);
    expect(table.numCols).toEqual(source.numCols);
    expect(batch.numCols).toEqual(source.numCols);
    for (let i = -1, n = source.numCols; ++i < n;) {
        const v0 = source.getChildAt(i)!.slice(offset, offset + batch.numRows);
        const v1 = batch.getChildAt(i);
        const v2 = table.getChildAt(i);
        const name = source.schema.fields[i].name;
        expect([v1, `batch`, name]).toEqualVector([v0, `source`]);
        expect([v2, `table`, name]).toEqualVector([v0, `source`]);
    }
}

describe(`Table`, () => {
    test(`can create an empty table`, () => {
        expect(new Table().numRows).toBe(0);
    });

    describe(`constructor`, () => {
        test(`creates an empty Table with Columns`, () => {
            let i32 = new Vector([makeData({ type: new Int32 })]);
            let f32 = new Vector([makeData({ type: new Float32 })]);
            const table = new Table({ i32, f32 });
            i32 = table.getChild('i32')!;
            f32 = table.getChild('f32')!;
            expect(table.numRows).toBe(0);
            expect(i32).toHaveLength(0);
            expect(f32).toHaveLength(0);
            expect(i32.toArray()).toBeInstanceOf(Int32Array);
            expect(f32.toArray()).toBeInstanceOf(Float32Array);
        });

        test(`creates a new Table from a Column`, () => {

            const i32s = new Int32Array(arange(new Array<number>(10)));
            const i32 = makeVector([i32s]);
            expect(i32).toHaveLength(i32s.length);
            expect(i32.nullCount).toBe(0);

            const table = new Table({ i32 });
            const i32Field = table.schema.fields[0];

            expect(i32Field.name).toBe('i32');
            expect(i32).toHaveLength(i32s.length);
            expect(i32Field.nullable).toBe(false);
            expect(i32.nullCount).toBe(0);

            expect(i32).toEqualVector(makeVector(i32s));
        });

        test(`creates a new Table from Columns`, () => {

            const i32s = new Int32Array(arange(new Array<number>(10)));
            const f32s = new Float32Array(arange(new Array<number>(10)));

            const i32 = makeVector(i32s);
            const f32 = makeVector(f32s);
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const table = new Table({ i32, f32 });
            const i32Field = table.schema.fields[0];
            const f32Field = table.schema.fields[1];

            expect(i32Field.name).toBe('i32');
            expect(f32Field.name).toBe('f32');
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32Field.nullable).toBe(false);
            expect(f32Field.nullable).toBe(false);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            expect(i32).toEqualVector(makeVector(i32s));
            expect(f32).toEqualVector(makeVector(f32s));
        });

        test(`creates a new Table from Columns with different lengths`, () => {

            const i32s = new Int32Array(arange(new Array<number>(20)));
            const f32s = new Float32Array(arange(new Array<number>(8)));

            const i32 = makeVector(i32s);
            const f32 = makeVector(f32s);

            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const table = new Table({ i32, f32 });
            const i32Field = table.schema.fields[0];
            const f32Field = table.schema.fields[1];

            expect(table.numRows).toBe(20);

            expect(i32Field.name).toBe('i32');
            expect(f32Field.name).toBe('f32');

            const i32Vector = table.getChild('i32')!;
            const f32Vector = table.getChild('f32')!;

            expect(i32Vector).toHaveLength(i32s.length);
            expect(f32Vector).toHaveLength(i32s.length); // new length should be the same as the longest sibling
            expect(i32Field.nullable).toBe(false);
            expect(f32Field.nullable).toBe(true); // true, with 12 additional nulls
            expect(i32Vector.nullCount).toBe(0);
            expect(f32Vector.nullCount).toBe(i32s.length - f32s.length);

            const f32Expected = makeData({
                type: f32.type,
                data: f32s,
                offset: 0,
                length: i32s.length,
                nullCount: i32s.length - f32s.length,
                nullBitmap: new Uint8Array(8).fill(255, 0, 1),
            });

            expect(i32Vector).toEqualVector(makeVector(i32s));
            expect(f32Vector).toEqualVector(new Vector([f32Expected]));
        });

        test(`creates a new Table from Columns with different lengths and number of inner chunks`, () => {

            const i32s = new Int32Array(arange(new Array<number>(20)));
            const f32s = new Float32Array(arange(new Array<number>(16)));

            const i32 = makeVector(i32s);
            const f32 = makeVector(f32s.slice(0, 8)).concat(makeVector(f32s.slice(8, 16)));

            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const table = new Table({ i32Renamed: i32, f32Renamed: f32 });
            const i32RenamedField = table.schema.fields[0];
            const f32RenamedField = table.schema.fields[1];
            const i32Renamed = table.getChild('i32Renamed')!;
            const f32Renamed = table.getChild('f32Renamed')!;

            expect(table.numRows).toBe(20);

            expect(i32RenamedField.name).toBe('i32Renamed');
            expect(f32RenamedField.name).toBe('f32Renamed');
            expect(i32Renamed).toHaveLength(i32s.length);
            expect(f32Renamed).toHaveLength(i32s.length); // new length should be the same as the longest sibling
            expect(i32RenamedField.nullable).toBe(false);
            expect(f32RenamedField.nullable).toBe(true); // true, with 4 additional nulls
            expect(i32Renamed.nullCount).toBe(0);
            expect(f32Renamed.nullCount).toBe(i32s.length - f32s.length);

            const f32Expected = makeData({
                data: f32s,
                type: f32.type,
                length: i32s.length,
                nullCount: i32s.length - f32s.length,
                nullBitmap: new Uint8Array(8).fill(255, 0, 2),
            });

            expect(i32Renamed).toEqualVector(makeVector(i32s));
            expect(f32Renamed).toEqualVector(new Vector([f32Expected]));
        });
    });

    test(`tableToIPC() serializes sliced RecordBatches`, () => {

        const table = getSingleRecordBatchTable();
        const batch = table.batches[0];
        const n = batch.numRows;
        const m = Math.trunc(n / 2);

        // First compare what happens when slicing from the batch level
        let batch1 = batch.slice(0, m);
        let batch2 = batch.slice(m, n);

        compareBatchAndTable(table, 0, batch1, deepCopy(table.slice(0, m)));
        compareBatchAndTable(table, m, batch2, deepCopy(table.slice(m, n)));

        // Then compare what happens when creating a RecordBatch by slicing the data
        batch1 = new RecordBatch(batch1.schema, batch.data.slice(0, m));
        batch2 = new RecordBatch(batch2.schema, batch.data.slice(m, m));

        compareBatchAndTable(table, 0, batch1, deepCopy(new Table([batch1])));
        compareBatchAndTable(table, m, batch2, deepCopy(new Table([batch2])));
    });

    for (const datum of test_data) {
        describe(datum.name, () => {
            test(`has the correct length`, () => {
                const table = datum.table();
                const values = datum.values();
                expect(table.numRows).toEqual(values.length);
            });
            test(`gets expected values`, () => {
                const table = datum.table();
                const values = datum.values();
                for (let i = -1; ++i < values.length;) {
                    const row = table.get(i)!;
                    expect(row).not.toBeNull();
                    const expected = values[i];
                    expect(row.f32).toEqual(expected[F32]);
                    expect(row.i32).toEqual(expected[I32]);
                    expect(row.dictionary).toEqual(expected[DICT]);
                }
            });
            test(`iterates expected values`, () => {
                let i = 0;
                const table = datum.table();
                const values = datum.values();
                for (const row of table) {
                    const expected = values[i++];
                    expect(row).not.toBeNull();
                    expect(row!.f32).toEqual(expected[F32]);
                    expect(row!.i32).toEqual(expected[I32]);
                    expect(row!.dictionary).toEqual(expected[DICT]);
                }
            });
            test(`serialize and de-serialize is a no-op`, () => {
                const table = datum.table();
                const clone = deepCopy(table);
                expect(clone).toEqualTable(table);
            });

            test(`count() returns the correct length`, () => {
                const table = datum.table();
                const values = datum.values();
                expect(table.numRows).toEqual(values.length);
            });

            test(`table.select() basic tests`, () => {
                const table = datum.table();
                const values = datum.values();
                const selected = table.select(['f32', 'dictionary']);
                expect(selected.schema.fields).toHaveLength(2);
                expect(selected.schema.fields[0]).toEqual(table.schema.fields[0]);
                expect(selected.schema.fields[1]).toEqual(table.schema.fields[2]);

                expect(selected.numRows).toEqual(values.length);
                let idx = 0, expected_row;
                for (const row of selected) {
                    expected_row = values[idx++];
                    if (!row) {
                        expect(row).toEqual(expected_row);
                    } else {
                        expect(row.f32).toEqual(expected_row[F32]);
                        expect(row.dictionary).toEqual(expected_row[DICT]);
                    }
                }
            });

            test(`table.getByteLength() returns the byteLength of each row`, () => {
                const table = datum.table();
                for (let i = -1, n = table.numRows; ++i < n;) {
                    expect(table.getByteLength(i)).toBeGreaterThan(0);
                }
            });
        });
    }
});

type TestDataSchema = { f32: Float32; i32: Int32; dictionary: Dictionary<Utf8, Int8> };

function getTestData(f32: number[], i32: number[], keys: number[]) {

    const i32Data = makeData({ type: new Int32, data: i32 });
    const f32Data = makeData({ type: new Float32, data: f32 });
    const [dictionary] = builderThroughIterable({ type: new Utf8 })(['a', 'b', 'c']);
    const dictionaryData = makeData({ type: new Dictionary(dictionary.type, new Int8), data: keys, dictionary });

    return {
        f32: f32Data,
        i32: i32Data,
        dictionary: dictionaryData,
    };
}

function getSingleRecordBatchTable() {
    const data = getTestData(
        [-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3],
        [-1, 1, -1, 1, -1, 1, -1],
        [0, 1, 2, 0, 1, 2, 0]
    );
    return new Table<TestDataSchema>({
        f32: new Vector([data.f32]),
        i32: new Vector([data.i32]),
        dictionary: new Vector([data.dictionary]),
    });
}

function getMultipleRecordBatchesTable() {

    const {
        f32: { type: f32Type },
        i32: { type: i32Type },
        dictionary: { type: dictionaryType },
    } = getTestData([], [], []);

    const schema = new Schema<TestDataSchema>([
        Field.new({ name: 'f32', type: f32Type }),
        Field.new({ name: 'i32', type: i32Type }),
        Field.new({ name: 'dictionary', type: dictionaryType })
    ]);

    const b1 = new RecordBatch(getTestData(
        [-0.3, -0.2, -0.1],
        [-1, 1, -1],
        [0, 1, 2]
    ));

    const b2 = new RecordBatch(getTestData(
        [0, 0.1, 0.2],
        [1, -1, 1],
        [0, 1, 2]
    ));

    const b3 = new RecordBatch(getTestData(
        [0.3, 0.2, 0.1],
        [-1, 1, -1],
        [0, 1, 2]
    ));

    return new Table<TestDataSchema>(schema, [
        new RecordBatch(schema, b1.data),
        new RecordBatch(schema, b2.data),
        new RecordBatch(schema, b3.data),
    ]);
}

function getStructTable() {
    const table = getSingleRecordBatchTable();
    return new Table({ struct: new Vector(table.data) });
}
