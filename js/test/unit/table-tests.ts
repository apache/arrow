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

import '../jest-extensions';
import {
    Data, Schema, Field, Table, RecordBatch, Column,
    Vector, Int32Vector, Float32Vector, Utf8Vector, DictionaryVector,
    Struct, Float32, Int32, Dictionary, Utf8, Int8
} from 'apache-arrow';
import { arange } from './utils';

const NAMES = ['f32', 'i32', 'dictionary'] as (keyof TestDataSchema)[];
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
        table: () => Table.fromStruct(getStructTable().getColumn('struct')!),
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
    expect(batch).toHaveLength(table.length);
    expect(table.numCols).toEqual(source.numCols);
    expect(batch.numCols).toEqual(source.numCols);
    for (let i = -1, n = source.numCols; ++i < n;) {
        const v0 = source.getColumnAt(i)!.slice(offset, offset + batch.length);
        const v1 = batch.getChildAt(i);
        const v2 = table.getColumnAt(i);
        const name = source.schema.fields[i].name;
        expect([v1, `batch`, name]).toEqualVector([v0, `source`]);
        expect([v2, `table`, name]).toEqualVector([v0, `source`]);
    }
}

describe(`Table`, () => {
    test(`can create an empty table`, () => {
        expect(Table.empty()).toHaveLength(0);
    });
    test(`Table.from([]) creates an empty table`, () => {
        expect(Table.from([])).toHaveLength(0);
    });
    test(`Table.from() creates an empty table`, () => {
        expect(Table.from()).toHaveLength(0);
    });

    describe(`new()`, () => {
        test(`creates an empty Table with Columns`, () => {
            let i32 = Column.new('i32', Data.new(new Int32(), 0, 0));
            let f32 = Column.new('f32', Data.new(new Float32(), 0, 0));
            const table = Table.new(i32, f32);
            i32 = table.getColumn('i32')!;
            f32 = table.getColumn('f32')!;
            expect(table).toHaveLength(0);
            expect(i32).toHaveLength(0);
            expect(f32).toHaveLength(0);
            expect(i32.toArray()).toBeInstanceOf(Int32Array);
            expect(f32.toArray()).toBeInstanceOf(Float32Array);
        });

        test(`creates a new Table from a Column`, () => {

            const i32s = new Int32Array(arange(new Array<number>(10)));

            let i32 = Column.new('i32', Data.Int(new Int32(), 0, i32s.length, 0, null, i32s));
            expect(i32.name).toBe('i32');
            expect(i32).toHaveLength(i32s.length);
            expect(i32.nullable).toBe(true);
            expect(i32.nullCount).toBe(0);

            const table = Table.new(i32);
            i32 = table.getColumnAt(0)!;

            expect(i32.name).toBe('i32');
            expect(i32).toHaveLength(i32s.length);
            expect(i32.nullable).toBe(true);
            expect(i32.nullCount).toBe(0);

            expect(i32).toEqualVector(Int32Vector.from(i32s));
        });

        test(`creates a new Table from Columns`, () => {

            const i32s = new Int32Array(arange(new Array<number>(10)));
            const f32s = new Float32Array(arange(new Array<number>(10)));

            let i32 = Column.new('i32', Data.Int(new Int32(), 0, i32s.length, 0, null, i32s));
            let f32 = Column.new('f32', Data.Float(new Float32(), 0, f32s.length, 0, null, f32s));
            expect(i32.name).toBe('i32');
            expect(f32.name).toBe('f32');
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullable).toBe(true);
            expect(f32.nullable).toBe(true);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const table = Table.new(i32, f32);
            i32 = table.getColumnAt(0)!;
            f32 = table.getColumnAt(1)!;

            expect(i32.name).toBe('i32');
            expect(f32.name).toBe('f32');
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullable).toBe(true);
            expect(f32.nullable).toBe(true);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            expect(i32).toEqualVector(Int32Vector.from(i32s));
            expect(f32).toEqualVector(Float32Vector.from(f32s));
        });

        test(`creates a new Table from Columns with different lengths`, () => {

            const i32s = new Int32Array(arange(new Array<number>(20)));
            const f32s = new Float32Array(arange(new Array<number>(8)));

            let i32 = Column.new('i32', Int32Vector.from(i32s));
            let f32 = Column.new('f32', Float32Vector.from(f32s));

            expect(i32.name).toBe('i32');
            expect(f32.name).toBe('f32');
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullable).toBe(true);
            expect(f32.nullable).toBe(true);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const table = Table.new([i32, f32]);
            i32 = table.getColumnAt(0)!;
            f32 = table.getColumnAt(1)!;

            expect(i32.name).toBe('i32');
            expect(f32.name).toBe('f32');
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(i32s.length); // new length should be the same as the longest sibling
            expect(i32.nullable).toBe(true);
            expect(f32.nullable).toBe(true); // true, with 12 additional nulls
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(i32s.length - f32s.length);

            const f32Expected = Data.Float(
                f32.type, 0, i32s.length,
                i32s.length - f32s.length,
                new Uint8Array(8).fill(255, 0, 1), f32s);

            expect(i32).toEqualVector(Int32Vector.from(i32s));
            expect(f32).toEqualVector(new Float32Vector(f32Expected));
        });

        test(`creates a new Table from Columns with different lengths and number of inner chunks`, () => {

            const i32s = new Int32Array(arange(new Array<number>(20)));
            const f32s = new Float32Array(arange(new Array<number>(16)));

            let i32 = Column.new('i32', Int32Vector.from(i32s));
            let f32 = Column.new('f32', Float32Vector.from(f32s.slice(0, 8)), Float32Vector.from(f32s.slice(8, 16)));

            expect(i32.name).toBe('i32');
            expect(f32.name).toBe('f32');
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(f32s.length);
            expect(i32.nullable).toBe(true);
            expect(f32.nullable).toBe(true);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const table = Table.new({ i32Renamed: i32, f32Renamed: f32 });
            i32 = table.getColumn('i32Renamed');
            f32 = table.getColumn('f32Renamed');

            expect(i32.name).toBe('i32Renamed');
            expect(f32.name).toBe('f32Renamed');
            expect(i32).toHaveLength(i32s.length);
            expect(f32).toHaveLength(i32s.length); // new length should be the same as the longest sibling
            expect(i32.nullable).toBe(true);
            expect(f32.nullable).toBe(true); // true, with 4 additional nulls
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(i32s.length - f32s.length);

            const f32Expected = Data.Float(
                f32.type, 0, i32s.length,
                i32s.length - f32s.length,
                new Uint8Array(8).fill(255, 0, 2), f32s);

            expect(i32).toEqualVector(Int32Vector.from(i32s));
            expect(f32).toEqualVector(new Float32Vector(f32Expected));
        });

        test(`creates a new Table from Typed Arrays`, () => {
            let i32s = Int32Array.from({length: 10}, (_, i) => i);
            let f32s = Float32Array.from({length: 10}, (_, i) => i);
            const table = Table.new({ i32s, f32s });
            const i32 = table.getColumn('i32s')!;
            const f32 = table.getColumn('f32s')!;

            expect(table).toHaveLength(10);
            expect(i32).toHaveLength(10);
            expect(f32).toHaveLength(10);
            expect(i32.toArray()).toBeInstanceOf(Int32Array);
            expect(f32.toArray()).toBeInstanceOf(Float32Array);
            expect(i32.toArray()).toEqual(i32s);
            expect(f32.toArray()).toEqual(f32s);
        });
    });

    test(`Table.serialize() serializes sliced RecordBatches`, () => {

        const table = getSingleRecordBatchTable();
        const batch = table.chunks[0], half = batch.length / 2 | 0;

        // First compare what happens when slicing from the batch level
        let [batch1, batch2] = [batch.slice(0, half), batch.slice(half)];

        compareBatchAndTable(table,    0, batch1, Table.from(new Table(batch1).serialize()));
        compareBatchAndTable(table, half, batch2, Table.from(new Table(batch2).serialize()));

        // Then compare what happens when creating a RecordBatch by slicing each child individually
        batch1 = new RecordBatch(batch1.schema, batch1.length, batch1.schema.fields.map((_, i) => {
            return batch.getChildAt(i)!.slice(0, half);
        }));

        batch2 = new RecordBatch(batch2.schema, batch2.length, batch2.schema.fields.map((_, i) => {
            return batch.getChildAt(i)!.slice(half);
        }));

        compareBatchAndTable(table,    0, batch1, Table.from(new Table(batch1).serialize()));
        compareBatchAndTable(table, half, batch2, Table.from(new Table(batch2).serialize()));
    });

    for (let datum of test_data) {
        describe(datum.name, () => {
            test(`has the correct length`, () => {
                const table = datum.table();
                const values = datum.values();
                expect(table).toHaveLength(values.length);
            });
            test(`gets expected values`, () => {
                const table = datum.table();
                const values = datum.values();
                for (let i = -1; ++i < values.length;) {
                    const row = table.get(i);
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
                for (let row of table) {
                    const expected = values[i++];
                    expect(row.f32).toEqual(expected[F32]);
                    expect(row.i32).toEqual(expected[I32]);
                    expect(row.dictionary).toEqual(expected[DICT]);
                }
            });
            test(`serialize and de-serialize is a no-op`, () => {
                const table = datum.table();
                const clone = Table.from(table.serialize());
                expect(clone).toEqualTable(table);
            });

            test(`count() returns the correct length`, () => {
                const table = datum.table();
                const values = datum.values();
                expect(table.count()).toEqual(values.length);
            });
            test(`getColumnIndex`, () => {
                const table = datum.table();
                expect(table.getColumnIndex('i32')).toEqual(I32);
                expect(table.getColumnIndex('f32')).toEqual(F32);
                expect(table.getColumnIndex('dictionary')).toEqual(DICT);
            });

            const table = datum.table();
            const values = datum.values();

            test(`table.select() basic tests`, () => {
                let selected = table.select('f32', 'dictionary');
                expect(selected.schema.fields).toHaveLength(2);
                expect(selected.schema.fields[0]).toEqual(table.schema.fields[0]);
                expect(selected.schema.fields[1]).toEqual(table.schema.fields[2]);

                expect(selected).toHaveLength(values.length);
                let idx = 0, expected_row;
                for (let row of selected) {
                    expected_row = values[idx++];
                    expect(row.f32).toEqual(expected_row[F32]);
                    expect(row.dictionary).toEqual(expected_row[DICT]);
                }
            });
        });
    }
});

type TestDataSchema = { f32: Float32; i32: Int32; dictionary: Dictionary<Utf8, Int8> };

function getTestVectors(f32Values: number[], i32Values: number[], dictIndices: number[]) {

    const values = Utf8Vector.from(['a', 'b', 'c']);
    const i32Data = Data.Int(new Int32(), 0, i32Values.length, 0, null, i32Values);
    const f32Data = Data.Float(new Float32(), 0, f32Values.length, 0, null, f32Values);

    return [Vector.new(f32Data), Vector.new(i32Data), DictionaryVector.from(values, new Int8(), dictIndices)];
}

function getSingleRecordBatchTable() {
    const vectors = getTestVectors(
        [-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3],
        [-1, 1, -1, 1, -1, 1, -1],
        [0, 1, 2, 0, 1, 2, 0]
    );

    return Table.new<TestDataSchema>(vectors, NAMES);
}

function getMultipleRecordBatchesTable() {

    const types = getTestVectors([], [], []).map((vec) => vec.type);
    const fields = NAMES.map((name, i) => Field.new(name, types[i]));
    const schema = new Schema<TestDataSchema>(fields);

    const b1 = new RecordBatch(schema, 3, getTestVectors(
        [-0.3, -0.2, -0.1],
        [-1, 1, -1],
        [0, 1, 2]
    ));

    const b2 = new RecordBatch(schema, 3, getTestVectors(
        [0, 0.1, 0.2],
        [1, -1, 1],
        [0, 1, 2]
    ));

    const b3 = new RecordBatch(schema, 3, getTestVectors(
        [0.3, 0.2, 0.1],
        [-1, 1, -1],
        [0, 1, 2]
    ));

    return new Table<TestDataSchema>([b1, b2, b3]);
}

function getStructTable() {
    const table = getSingleRecordBatchTable();
    const struct = new Struct<TestDataSchema>(table.schema.fields);
    const children = table.schema.fields.map((_, i) => table.getColumnAt(i)!);
    const structVec = Vector.new(Data.Struct(struct, 0, table.length, 0, null, children));
    return Table.new<{ struct: Struct<TestDataSchema> }>([structVec], ['struct']);
}
