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
    predicate,
    Data, Schema, Table, RecordBatch, Column,
    Vector, Int32Vector, Float32Vector, Utf8Vector, DictionaryVector,
    Struct, Float32, Int32, Dictionary, Utf8, Int8
} from '../Arrow';

const { col, lit, custom, and, or, And, Or } = predicate;

const NAMES = ['f32', 'i32', 'dictionary'] as (keyof TestDataSchema)[];
const F32 = 0, I32 = 1, DICT = 2;
const test_data = [
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
    expect(batch.length).toEqual(table.length);
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
        expect(Table.empty().length).toEqual(0);
    });
    test(`Table.from([]) creates an empty table`, () => {
        expect(Table.from([]).length).toEqual(0);
    });
    test(`Table.from() creates an empty table`, () => {
        expect(Table.from().length).toEqual(0);
    });

    describe(`new()`, () => {

        const arange = <T extends { length: number; [n: number]: number; }>(arr: T, n = arr.length) => {
            for (let i = -1; ++i < n; arr[i] = i) { }
            return arr;
        };

        test(`creates an empty Table with Columns`, () => {
            let i32 = Column.new('i32', Data.new(new Int32(), 0, 0));
            let f32 = Column.new('f32', Data.new(new Float32(), 0, 0));
            const table = Table.new(i32, f32);
            i32 = table.getColumn('i32')!;
            f32 = table.getColumn('f32')!;
            expect(table.length).toBe(0);
            expect(i32.length).toBe(0);
            expect(f32.length).toBe(0);
            expect(i32.toArray()).toBeInstanceOf(Int32Array);
            expect(f32.toArray()).toBeInstanceOf(Float32Array);
        });

        test(`creates a new Table from a Column`, () => {

            const i32s = new Int32Array(arange(new Array<number>(10)));

            let i32 = Column.new('i32', Data.Int(new Int32(), 0, i32s.length, 0, null, i32s));
            expect(i32.name).toBe('i32');
            expect(i32.length).toBe(i32s.length);
            expect(i32.nullable).toBe(false);
            expect(i32.nullCount).toBe(0);

            const table = Table.new(i32);
            i32 = table.getColumnAt(0)!;

            expect(i32.name).toBe('i32');
            expect(i32.length).toBe(i32s.length);
            expect(i32.nullable).toBe(false);
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
            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(f32s.length);
            expect(i32.nullable).toBe(false);
            expect(f32.nullable).toBe(false);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const table = Table.new(i32, f32);
            i32 = table.getColumnAt(0)!;
            f32 = table.getColumnAt(1)!;

            expect(i32.name).toBe('i32');
            expect(f32.name).toBe('f32');
            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(f32s.length);
            expect(i32.nullable).toBe(false);
            expect(f32.nullable).toBe(false);
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
            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(f32s.length);
            expect(i32.nullable).toBe(false);
            expect(f32.nullable).toBe(false);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const table = Table.new([i32, f32]);
            i32 = table.getColumnAt(0)!;
            f32 = table.getColumnAt(1)!;

            expect(i32.name).toBe('i32');
            expect(f32.name).toBe('f32');
            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(i32s.length); // new length should be the same as the longest sibling
            expect(i32.nullable).toBe(false);
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
            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(f32s.length);
            expect(i32.nullable).toBe(false);
            expect(f32.nullable).toBe(false);
            expect(i32.nullCount).toBe(0);
            expect(f32.nullCount).toBe(0);

            const table = Table.new({ i32Renamed: i32, f32Renamed: f32 });
            i32 = table.getColumn('i32Renamed');
            f32 = table.getColumn('f32Renamed');

            expect(i32.name).toBe('i32Renamed');
            expect(f32.name).toBe('f32Renamed');
            expect(i32.length).toBe(i32s.length);
            expect(f32.length).toBe(i32s.length); // new length should be the same as the longest sibling
            expect(i32.nullable).toBe(false);
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
                expect(table.length).toEqual(values.length);
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

            describe(`scan()`, () => {
                test(`yields all values`, () => {
                    const table = datum.table();
                    let expected_idx = 0;
                    table.scan((idx, batch) => {
                        const columns = batch.schema.fields.map((_, i) => batch.getChildAt(i)!);
                        expect(columns.map((c) => c.get(idx))).toEqual(values[expected_idx++]);
                    });
                });
                test(`calls bind function with every batch`, () => {
                    const table = datum.table();
                    let bind = jest.fn();
                    table.scan(() => { }, bind);
                    for (let batch of table.chunks) {
                        expect(bind).toHaveBeenCalledWith(batch);
                    }
                });
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
            let get_i32: (idx: number) => number, get_f32: (idx: number) => number;
            const filter_tests = [
                {
                    name: `filter on f32 >= 0`,
                    filtered: table.filter(col('f32').ge(0)),
                    expected: values.filter((row) => row[F32] >= 0)
                }, {
                    name: `filter on 0 <= f32`,
                    filtered: table.filter(lit(0).le(col('f32'))),
                    expected: values.filter((row) => 0 <= row[F32])
                }, {
                    name: `filter on i32 <= 0`,
                    filtered: table.filter(col('i32').le(0)),
                    expected: values.filter((row) => row[I32] <= 0)
                }, {
                    name: `filter on 0 >= i32`,
                    filtered: table.filter(lit(0).ge(col('i32'))),
                    expected: values.filter((row) => 0 >= row[I32])
                }, {
                    name: `filter on f32 < 0`,
                    filtered: table.filter(col('f32').lt(0)),
                    expected: values.filter((row) => row[F32] < 0)
                }, {
                    name: `filter on i32 > 1 (empty)`,
                    filtered: table.filter(col('i32').gt(0)),
                    expected: values.filter((row) => row[I32] > 0)
                }, {
                    name: `filter on f32 <= -.25 || f3 >= .25`,
                    filtered: table.filter(col('f32').le(-.25).or(col('f32').ge(.25))),
                    expected: values.filter((row) => row[F32] <= -.25 || row[F32] >= .25)
                }, {
                    name: `filter on !(f32 <= -.25 || f3 >= .25) (not)`,
                    filtered: table.filter(col('f32').le(-.25).or(col('f32').ge(.25)).not()),
                    expected: values.filter((row) => !(row[F32] <= -.25 || row[F32] >= .25))
                }, {
                    name: `filter method combines predicates (f32 >= 0 && i32 <= 0)`,
                    filtered: table.filter(col('i32').le(0)).filter(col('f32').ge(0)),
                    expected: values.filter((row) => row[I32] <= 0 && row[F32] >= 0)
                }, {
                    name: `filter on dictionary == 'a'`,
                    filtered: table.filter(col('dictionary').eq('a')),
                    expected: values.filter((row) => row[DICT] === 'a')
                }, {
                    name: `filter on 'a' == dictionary (commutativity)`,
                    filtered: table.filter(lit('a').eq(col('dictionary'))),
                    expected: values.filter((row) => row[DICT] === 'a')
                }, {
                    name: `filter on dictionary != 'b'`,
                    filtered: table.filter(col('dictionary').ne('b')),
                    expected: values.filter((row) => row[DICT] !== 'b')
                }, {
                    name: `filter on f32 >= i32`,
                    filtered: table.filter(col('f32').ge(col('i32'))),
                    expected: values.filter((row) => row[F32] >= row[I32])
                }, {
                    name: `filter on f32 <= i32`,
                    filtered: table.filter(col('f32').le(col('i32'))),
                    expected: values.filter((row) => row[F32] <= row[I32])
                }, {
                    name: `filter on f32*i32 > 0 (custom predicate)`,
                    filtered: table.filter(custom(
                        (idx: number) => (get_f32(idx) * get_i32(idx) > 0),
                        (batch: RecordBatch) => {
                            get_f32 = col('f32').bind(batch);
                            get_i32 = col('i32').bind(batch);
                        })),
                    expected: values.filter((row) => (row[F32] as number) * (row[I32] as number) > 0)
                }
            ];
            for (let this_test of filter_tests) {
                const { name, filtered, expected } = this_test;
                describe(name, () => {
                    test(`count() returns the correct length`, () => {
                        expect(filtered.count()).toEqual(expected.length);
                    });
                    describe(`scan()`, () => {
                        test(`iterates over expected values`, () => {
                            let expected_idx = 0;
                            filtered.scan((idx, batch) => {
                                const columns = batch.schema.fields.map((_, i) => batch.getChildAt(i)!);
                                expect(columns.map((c) => c.get(idx))).toEqual(expected[expected_idx++]);
                            });
                        });
                        test(`calls bind function on every batch`, () => {
                            // Techincally, we only need to call bind on
                            // batches with data that match the predicate, so
                            // this test may fail in the future if we change
                            // that - and that's ok!
                            let bind = jest.fn();
                            filtered.scan(() => { }, bind);
                            for (let batch of table.chunks) {
                                expect(bind).toHaveBeenCalledWith(batch);
                            }
                        });
                    });
                });
            }
            test(`countBy on dictionary returns the correct counts`, () => {
                // Make sure countBy works both with and without the Col wrapper
                // class
                let expected: { [key: string]: number } = { 'a': 0, 'b': 0, 'c': 0 };
                for (let row of values) {
                    expected[row[DICT]] += 1;
                }

                expect(table.countBy(col('dictionary')).toJSON()).toEqual(expected);
                expect(table.countBy('dictionary').toJSON()).toEqual(expected);
            });
            test(`countBy on dictionary with filter returns the correct counts`, () => {
                let expected: { [key: string]: number } = { 'a': 0, 'b': 0, 'c': 0 };
                for (let row of values) {
                    if (row[I32] === 1) { expected[row[DICT]] += 1; }
                }

                expect(table.filter(col('i32').eq(1)).countBy('dictionary').toJSON()).toEqual(expected);
            });
            test(`countBy on non dictionary column throws error`, () => {
                expect(() => { table.countBy('i32'); }).toThrow();
                expect(() => { table.filter(col('dict').eq('a')).countBy('i32'); }).toThrow();
            });
            test(`countBy on non-existent column throws error`, () => {
                expect(() => { table.countBy('FAKE' as any); }).toThrow();
            });
            test(`table.select() basic tests`, () => {
                let selected = table.select('f32', 'dictionary');
                expect(selected.schema.fields.length).toEqual(2);
                expect(selected.schema.fields[0]).toEqual(table.schema.fields[0]);
                expect(selected.schema.fields[1]).toEqual(table.schema.fields[2]);

                expect(selected.length).toEqual(values.length);
                let idx = 0, expected_row;
                for (let row of selected) {
                    expected_row = values[idx++];
                    expect(row.f32).toEqual(expected_row[F32]);
                    expect(row.dictionary).toEqual(expected_row[DICT]);
                }
            });
            // test(`table.toString()`, () => {
            //     let selected = table.select('i32', 'dictionary');
            //     let headers = [`"row_id"`, `"i32: Int32"`, `"dictionary: Dictionary<Int8, Utf8>"`];
            //     let expected = [headers.join(' | '), ...values.map((row, idx) => {
            //         return [`${idx}`, `${row[I32]}`, `"${row[DICT]}"`].map((str, col) => {
            //             return leftPad(str, ' ', headers[col].length);
            //         }).join(' | ');
            //     })].join('\n') + '\n';
            //     expect(selected.toString()).toEqual(expected);
            // });
            test(`table.filter(..).count() on always false predicates returns 0`, () => {
                expect(table.filter(col('i32').ge(100)).count()).toEqual(0);
                expect(table.filter(col('dictionary').eq('z')).count()).toEqual(0);
            });
            describe(`lit-lit comparison`, () => {
                test(`always-false count() returns 0`, () => {
                    expect(table.filter(lit('abc').eq('def')).count()).toEqual(0);
                    expect(table.filter(lit(0).ge(1)).count()).toEqual(0);
                });
                test(`always-true count() returns length`, () => {
                    expect(table.filter(lit('abc').eq('abc')).count()).toEqual(table.length);
                    expect(table.filter(lit(-100).le(0)).count()).toEqual(table.length);
                });
            });
            describe(`col-col comparison`, () => {
                test(`always-false count() returns 0`, () => {
                    expect(table.filter(col('dictionary').eq(col('i32'))).count()).toEqual(0);
                });
                test(`always-true count() returns length`, () => {
                    expect(table.filter(col('dictionary').eq(col('dictionary'))).count()).toEqual(table.length);
                });
            });
        });
    }
});

describe(`Predicate`, () => {
    const p1 = col('a').gt(100);
    const p2 = col('a').lt(1000);
    const p3 = col('b').eq('foo');
    const p4 = col('c').eq('bar');
    const expected = [p1, p2, p3, p4];
    test(`and flattens children`, () => {
        expect(and(p1, p2, p3, p4).children).toEqual(expected);
        expect(and(p1.and(p2), new And(p3, p4)).children).toEqual(expected);
        expect(and(p1.and(p2, p3, p4)).children).toEqual(expected);
    });
    test(`or flattens children`, () => {
        expect(or(p1, p2, p3, p4).children).toEqual(expected);
        expect(or(p1.or(p2), new Or(p3, p4)).children).toEqual(expected);
        expect(or(p1.or(p2, p3, p4)).children).toEqual(expected);
    });
});

// function leftPad(str: string, fill: string, n: number) {
//     return (new Array(n + 1).join(fill) + str).slice(-1 * n);
// }

type TestDataSchema = { f32: Float32; i32: Int32; dictionary: Dictionary<Utf8, Int8>; };

function getTestVectors(f32Values: number[], i32Values: number[], dictIndices: number[]) {

    const values = Utf8Vector.from(['a', 'b', 'c']);
    const i32Data = Data.Int(new Int32(), 0, i32Values.length, 0, null, i32Values);
    const f32Data = Data.Float(new Float32(), 0, f32Values.length, 0, null, f32Values);

    return [Vector.new(f32Data), Vector.new(i32Data), DictionaryVector.from(values, new Int8(), dictIndices)];
}

export function getSingleRecordBatchTable() {
    const vectors = getTestVectors(
        [-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3],
        [-1, 1, -1, 1, -1, 1, -1],
        [0, 1, 2, 0, 1, 2, 0]
    );

    return Table.new<TestDataSchema>(vectors, NAMES);
}

function getMultipleRecordBatchesTable() {

    const schema = Schema.from<TestDataSchema>(getTestVectors([], [], []), NAMES);

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
