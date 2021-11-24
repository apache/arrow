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
    predicate, DataFrame, RecordBatch
} from 'apache-arrow';
import { test_data } from './table-tests';
import { jest } from '@jest/globals';

const { col, lit, custom, and, or, And, Or } = predicate;

const F32 = 0, I32 = 1, DICT = 2;

describe(`DataFrame`, () => {

    for (let datum of test_data) {
        describe(datum.name, () => {

            describe(`scan()`, () => {
                test(`yields all values`, () => {
                    const df = new DataFrame(datum.table());
                    let expected_idx = 0;
                    df.scan((idx, batch) => {
                        const columns = batch.schema.fields.map((_, i) => batch.getChildAt(i)!);
                        expect(columns.map((c) => c.get(idx))).toEqual(values[expected_idx++]);
                    });
                });
                test(`calls bind function with every batch`, () => {
                    const df = new DataFrame(datum.table());
                    let bind = jest.fn();
                    df.scan(() => { }, bind);
                    for (let batch of df.chunks) {
                        expect(bind).toHaveBeenCalledWith(batch);
                    }
                });
            });
            describe(`scanReverse()`, () => {
                test(`yields all values`, () => {
                    const df = new DataFrame(datum.table());
                    let expected_idx = values.length;
                    df.scanReverse((idx, batch) => {
                        const columns = batch.schema.fields.map((_, i) => batch.getChildAt(i)!);
                        expect(columns.map((c) => c.get(idx))).toEqual(values[--expected_idx]);
                    });
                });
                test(`calls bind function with every batch`, () => {
                    const df = new DataFrame(datum.table());
                    let bind = jest.fn();
                    df.scanReverse(() => { }, bind);
                    for (let batch of df.chunks) {
                        expect(bind).toHaveBeenCalledWith(batch);
                    }
                });
            });
            test(`count() returns the correct length`, () => {
                const df = new DataFrame(datum.table());
                const values = datum.values();
                expect(df.count()).toEqual(values.length);
            });
            test(`getColumnIndex`, () => {
                const df = new DataFrame(datum.table());
                expect(df.getColumnIndex('i32')).toEqual(I32);
                expect(df.getColumnIndex('f32')).toEqual(F32);
                expect(df.getColumnIndex('dictionary')).toEqual(DICT);
            });
            const df = new DataFrame(datum.table());
            const values = datum.values();
            let get_i32: (idx: number) => number, get_f32: (idx: number) => number;
            const filter_tests = [
                {
                    name: `filter on f32 >= 0`,
                    filtered: df.filter(col('f32').ge(0)),
                    expected: values.filter((row) => row[F32] >= 0)
                }, {
                    name: `filter on 0 <= f32`,
                    filtered: df.filter(lit(0).le(col('f32'))),
                    expected: values.filter((row) => 0 <= row[F32])
                }, {
                    name: `filter on i32 <= 0`,
                    filtered: df.filter(col('i32').le(0)),
                    expected: values.filter((row) => row[I32] <= 0)
                }, {
                    name: `filter on 0 >= i32`,
                    filtered: df.filter(lit(0).ge(col('i32'))),
                    expected: values.filter((row) => 0 >= row[I32])
                }, {
                    name: `filter on f32 < 0`,
                    filtered: df.filter(col('f32').lt(0)),
                    expected: values.filter((row) => row[F32] < 0)
                }, {
                    name: `filter on i32 > 1 (empty)`,
                    filtered: df.filter(col('i32').gt(0)),
                    expected: values.filter((row) => row[I32] > 0)
                }, {
                    name: `filter on f32 <= -.25 || f3 >= .25`,
                    filtered: df.filter(col('f32').le(-.25).or(col('f32').ge(.25))),
                    expected: values.filter((row) => row[F32] <= -.25 || row[F32] >= .25)
                }, {
                    name: `filter on !(f32 <= -.25 || f3 >= .25) (not)`,
                    filtered: df.filter(col('f32').le(-.25).or(col('f32').ge(.25)).not()),
                    expected: values.filter((row) => !(row[F32] <= -.25 || row[F32] >= .25))
                }, {
                    name: `filter method combines predicates (f32 >= 0 && i32 <= 0)`,
                    filtered: df.filter(col('i32').le(0)).filter(col('f32').ge(0)),
                    expected: values.filter((row) => row[I32] <= 0 && row[F32] >= 0)
                }, {
                    name: `filter on dictionary == 'a'`,
                    filtered: df.filter(col('dictionary').eq('a')),
                    expected: values.filter((row) => row[DICT] === 'a')
                }, {
                    name: `filter on 'a' == dictionary (commutativity)`,
                    filtered: df.filter(lit('a').eq(col('dictionary'))),
                    expected: values.filter((row) => row[DICT] === 'a')
                }, {
                    name: `filter on dictionary != 'b'`,
                    filtered: df.filter(col('dictionary').ne('b')),
                    expected: values.filter((row) => row[DICT] !== 'b')
                }, {
                    name: `filter on f32 >= i32`,
                    filtered: df.filter(col('f32').ge(col('i32'))),
                    expected: values.filter((row) => row[F32] >= row[I32])
                }, {
                    name: `filter on f32 <= i32`,
                    filtered: df.filter(col('f32').le(col('i32'))),
                    expected: values.filter((row) => row[F32] <= row[I32])
                }, {
                    name: `filter on f32*i32 > 0 (custom predicate)`,
                    filtered: df.filter(custom(
                        (idx: number) => (get_f32(idx) * get_i32(idx) > 0),
                        (batch: RecordBatch) => {
                            get_f32 = col('f32').bind(batch);
                            get_i32 = col('i32').bind(batch);
                        })),
                    expected: values.filter((row) => (row[F32] as number) * (row[I32] as number) > 0)
                }, {
                    name: `filter out all records`,
                    filtered: df.filter(lit(1).eq(0)),
                    expected: []
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
                        test(`calls bind function lazily`, () => {
                            let bind = jest.fn();
                            filtered.scan(() => { }, bind);
                            if (expected.length) {
                                expect(bind).toHaveBeenCalled();
                            } else {
                                expect(bind).not.toHaveBeenCalled();
                            }
                        });
                    });
                    describe(`scanReverse()`, () => {
                        test(`iterates over expected values in reverse`, () => {
                            let expected_idx = expected.length;
                            filtered.scanReverse((idx, batch) => {
                                const columns = batch.schema.fields.map((_, i) => batch.getChildAt(i)!);
                                expect(columns.map((c) => c.get(idx))).toEqual(expected[--expected_idx]);
                            });
                        });
                        test(`calls bind function lazily`, () => {
                            let bind = jest.fn();
                            filtered.scanReverse(() => { }, bind);
                            if (expected.length) {
                                expect(bind).toHaveBeenCalled();
                            } else {
                                expect(bind).not.toHaveBeenCalled();
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

                expect(df.countBy(col('dictionary')).toJSON()).toEqual(expected);
                expect(df.countBy('dictionary').toJSON()).toEqual(expected);
            });
            test(`countBy on dictionary with filter returns the correct counts`, () => {
                let expected: { [key: string]: number } = { 'a': 0, 'b': 0, 'c': 0 };
                for (let row of values) {
                    if (row[I32] === 1) { expected[row[DICT]] += 1; }
                }

                expect(df.filter(col('i32').eq(1)).countBy('dictionary').toJSON()).toEqual(expected);
            });
            test(`countBy on non dictionary column throws error`, () => {
                expect(() => { df.countBy('i32'); }).toThrow();
                expect(() => { df.filter(col('dict').eq('a')).countBy('i32'); }).toThrow();
            });
            test(`countBy on non-existent column throws error`, () => {
                expect(() => { df.countBy('FAKE' as any); }).toThrow();
            });
            test(`table.select() basic tests`, () => {
                let selected = df.select('f32', 'dictionary');
                expect(selected.schema.fields).toHaveLength(2);
                expect(selected.schema.fields[0]).toEqual(df.schema.fields[0]);
                expect(selected.schema.fields[1]).toEqual(df.schema.fields[2]);

                expect(selected).toHaveLength(values.length);
                let idx = 0, expected_row;
                for (let row of selected) {
                    expected_row = values[idx++];
                    expect(row.f32).toEqual(expected_row[F32]);
                    expect(row.dictionary).toEqual(expected_row[DICT]);
                }
            });
            test(`table.filter(..).count() on always false predicates returns 0`, () => {
                expect(df.filter(col('i32').ge(100)).count()).toEqual(0);
                expect(df.filter(col('dictionary').eq('z')).count()).toEqual(0);
            });
            describe(`lit-lit comparison`, () => {
                test(`always-false count() returns 0`, () => {
                    expect(df.filter(lit('abc').eq('def')).count()).toEqual(0);
                    expect(df.filter(lit(0).ge(1)).count()).toEqual(0);
                });
                test(`always-true count() returns length`, () => {
                    expect(df.filter(lit('abc').eq('abc')).count()).toEqual(df.length);
                    expect(df.filter(lit(-100).le(0)).count()).toEqual(df.length);
                });
            });
            describe(`col-col comparison`, () => {
                test(`always-false count() returns 0`, () => {
                    expect(df.filter(col('dictionary').eq(col('i32'))).count()).toEqual(0);
                });
                test(`always-true count() returns length`, () => {
                    expect(df.filter(col('dictionary').eq(col('dictionary'))).count()).toEqual(df.length);
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
