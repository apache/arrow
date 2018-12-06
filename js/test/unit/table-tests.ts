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
import { TextEncoder } from 'text-encoding-utf-8';

import Arrow, { vector, RecordBatch } from '../Arrow';

const { predicate, Table } = Arrow;

const { DictionaryVector, IntVector, FloatVector, Utf8Vector } = Arrow.vector;
const { Dictionary, Utf8, Int } = Arrow.type;

const { col, lit, custom, and, or, And, Or } = predicate;

const utf8Encoder = new TextEncoder('utf-8');

const NAMES = ['f32', 'i32', 'dictionary'];
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
        table: () => Table.fromStruct(getStructTable().getColumn('struct') as vector.StructVector),
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

function compareTables(t1: Table, t2, Table) {
    expect(t1.length).toEqual(t2.length);
    expect(t1.numCols).toEqual(t2.numCols);
    for (let i = -1, n = t1.numCols; ++i < n;) {
        const v1 = t1.getColumnAt(i);
        const v2 = t2.getColumnAt(i);
        (expect([v1, `left`, t1.schema.fields[i].name]) as any).toEqualVector([v2, `right`, t2.schema.fields[i].name]);
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
    for (let datum of test_data) {
        describe(datum.name, () => {
            const table = datum.table();
            const values = datum.values();

            test(`has the correct length`, () => {
                expect(table.length).toEqual(values.length);
            });
            test(`gets expected values`, () => {
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
                for (let row of table) {
                    const expected = values[i++];
                    expect(row.f32).toEqual(expected[F32]);
                    expect(row.i32).toEqual(expected[I32]);
                    expect(row.dictionary).toEqual(expected[DICT]);
                }
            });
            describe(`scan()`, () => {
                test(`yields all values`, () => {
                    let expected_idx = 0;
                    table.scan((idx, batch) => {
                        const columns = batch.schema.fields.map((_, i) => batch.getChildAt(i)!);
                        expect(columns.map((c) => c.get(idx))).toEqual(values[expected_idx++]);
                    });
                });
                test(`calls bind function with every batch`, () => {
                    let bind = jest.fn();
                    table.scan(() => { }, bind);
                    for (let batch of table.batches) {
                        expect(bind).toHaveBeenCalledWith(batch);
                    }
                });
            });
            test(`count() returns the correct length`, () => {
                expect(table.count()).toEqual(values.length);
            });
            test(`getColumnIndex`, () => {
                expect(table.getColumnIndex('i32')).toEqual(I32);
                expect(table.getColumnIndex('f32')).toEqual(F32);
                expect(table.getColumnIndex('dictionary')).toEqual(DICT);
            });
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
                            for (let batch of table.batches) {
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
                expect(() => { table.countBy('FAKE'); }).toThrow();
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
            test(`table.toString()`, () => {
                let selected = table.select('i32', 'dictionary');
                let headers = [`"row_id"`, `"i32: Int32"`, `"dictionary: Dictionary<Int8, Utf8>"`];
                let expected = [headers.join(' | '), ...values.map((row, idx) => {
                    return [`${idx}`, `${row[I32]}`, `"${row[DICT]}"`].map((str, col) => {
                        return leftPad(str, ' ', headers[col].length);
                    }).join(' | ');
                })].join('\n') + '\n';
                expect(selected.toString()).toEqual(expected);
            });
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
            describe(`serialize and de-serialize is a no-op`, () => {
                compareTables(Table.from(table.serialize()), table);
            });
        });
    }
});

describe(`Predicate`, () => {
    const p1 = col('a').gt(100);
    const p2 = col('a').lt(1000);
    const p3 = col('b').eq('foo');
    const p4 = col('c').eq('bar');
    const expected = [p1, p2, p3, p4]
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

function leftPad(str: string, fill: string, n: number) {
    return (new Array(n + 1).join(fill) + str).slice(-1 * n);
}

function makeUtf8Vector(values) {
    const n = values.length;
    let offset = 0;
    const offsets = Uint32Array.of(0, ...values.map((d) => { offset += d.length; return offset; }));
    return new Utf8Vector(new Arrow.data.FlatListData(new Utf8(), n, null, offsets, utf8Encoder.encode(values.join(''))));
}

function getTestVectors(f32Values, i32Values, dictionaryValues) {
    const f32Vec = FloatVector.from(
        Float32Array.from(f32Values)
    );

    const i32Vec = IntVector.from(
        Int32Array.from(i32Values)
    );

    const dictionaryVec = new DictionaryVector(
        new Arrow.data.DictionaryData(
            new Dictionary(new Utf8(), new Int(true, 8)),
            makeUtf8Vector(['a', 'b', 'c']),
            IntVector.from(Int8Array.from(dictionaryValues)).data
        )
    );

    return [f32Vec, i32Vec, dictionaryVec];
}

export function getSingleRecordBatchTable() {
    const vectors = getTestVectors(
        [-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3],
        [-1, 1, -1, 1, -1, 1, -1],
        [0, 1, 2, 0, 1, 2, 0]
    );

    return Table.fromVectors(
        vectors,
        NAMES
    );
}

function getMultipleRecordBatchesTable() {
    const b1 = Arrow.RecordBatch.from(getTestVectors(
        [-0.3, -0.2, -0.1],
        [-1, 1, -1],
        [0, 1, 2]
    ), NAMES);

    const b2 = Arrow.RecordBatch.from(getTestVectors(
        [0, 0.1, 0.2],
        [1, -1, 1],
        [0, 1, 2]
    ), NAMES);

    const b3 = Arrow.RecordBatch.from(getTestVectors(
        [0.3, 0.2, 0.1],
        [-1, 1, -1],
        [0, 1, 2]
    ), NAMES);

    return new Table([b1, b2, b3])
}

function getStructTable() {
    const structVec = getSingleRecordBatchTable().batchesUnion
    return Table.fromVectors([structVec], ['struct'])
}
