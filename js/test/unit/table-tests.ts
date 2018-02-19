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

const { predicate, Table } = Arrow;

const { col, lit } = predicate;

const F32 = 0, I32 = 1, DICT = 2;
const test_data = [
    {name: `single record batch`,
     table: Table.from({
          'schema': {
            'fields': [
              {
                'name': 'f32',
                'type': {
                  'name': 'floatingpoint',
                  'precision': 'SINGLE'
                },
                'nullable': false,
                'children': [],
              },
              {
                'name': 'i32',
                'type': {
                  'name': 'int',
                  'isSigned': true,
                  'bitWidth': 32
                },
                'nullable': false,
                'children': [],
              },
              {
                'name': 'dictionary',
                'type': {
                  'name': 'utf8'
                },
                'nullable': false,
                'children': [],
                'dictionary': {
                  'id': 0,
                  'indexType': {
                    'name': 'int',
                    'isSigned': true,
                    'bitWidth': 8
                  },
                  'isOrdered': false
                }
              }
            ]
          },
          'dictionaries': [{
            'id': 0,
            'data': {
              'count': 3,
              'columns': [
                {
                  'name': 'DICT0',
                  'count': 3,
                  'VALIDITY': [],
                  'OFFSET': [
                    0,
                    1,
                    2,
                    3
                  ],
                  'DATA': [
                    'a',
                    'b',
                    'c',
                  ]
                }
              ]
            }
          }],
          'batches': [{
            'count': 7,
            'columns': [
              {
                'name': 'f32',
                'count': 7,
                'VALIDITY': [],
                'DATA': [-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3]
              },
              {
                'name': 'i32',
                'count': 7,
                'VALIDITY': [],
                'DATA': [-1, 1, -1, 1, -1, 1, -1]
              },
              {
                'name': 'dictionary',
                'count': 7,
                'VALIDITY': [],
                'DATA': [0, 1, 2, 0, 1, 2, 0]
              }
            ]
          }]
        }),
        // Use Math.fround to coerce to float32
     values: [
         [Math.fround(-0.3), -1, 'a'],
         [Math.fround(-0.2),  1, 'b'],
         [Math.fround(-0.1), -1, 'c'],
         [Math.fround( 0  ),  1, 'a'],
         [Math.fround( 0.1), -1, 'b'],
         [Math.fround( 0.2),  1, 'c'],
         [Math.fround( 0.3), -1, 'a']
     ]},
     {name: `multiple record batches`,
      table: Table.from({
          'schema': {
            'fields': [
              {
                'name': 'f32',
                'type': {
                  'name': 'floatingpoint',
                  'precision': 'SINGLE'
                },
                'nullable': false,
                'children': [],
              },
              {
                'name': 'i32',
                'type': {
                  'name': 'int',
                  'isSigned': true,
                  'bitWidth': 32
                },
                'nullable': false,
                'children': [],
              },
              {
                'name': 'dictionary',
                'type': {
                  'name': 'utf8'
                },
                'nullable': false,
                'children': [],
                'dictionary': {
                  'id': 0,
                  'indexType': {
                    'name': 'int',
                    'isSigned': true,
                    'bitWidth': 8
                  },
                  'isOrdered': false
                }
              }
            ]
          },
          'dictionaries': [{
            'id': 0,
            'data': {
              'count': 3,
              'columns': [
                {
                  'name': 'DICT0',
                  'count': 3,
                  'VALIDITY': [],
                  'OFFSET': [
                    0,
                    1,
                    2,
                    3
                  ],
                  'DATA': [
                    'a',
                    'b',
                    'c',
                  ]
                }
              ]
            }
          }],
          'batches': [{
            'count': 3,
            'columns': [
              {
                'name': 'f32',
                'count': 3,
                'VALIDITY': [],
                'DATA': [-0.3, -0.2, -0.1]
              },
              {
                'name': 'i32',
                'count': 3,
                'VALIDITY': [],
                'DATA': [-1, 1, -1]
              },
              {
                'name': 'dictionary',
                'count': 3,
                'VALIDITY': [],
                'DATA': [0, 1, 2]
              }
            ]
          }, {
            'count': 3,
            'columns': [
              {
                'name': 'f32',
                'count': 3,
                'VALIDITY': [],
                'DATA': [0, 0.1, 0.2]
              },
              {
                'name': 'i32',
                'count': 3,
                'VALIDITY': [],
                'DATA': [1, -1, 1]
              },
              {
                'name': 'dictionary',
                'count': 3,
                'VALIDITY': [],
                'DATA': [0, 1, 2]
              }
            ]
          }, {
            'count': 3,
            'columns': [
              {
                'name': 'f32',
                'count': 3,
                'VALIDITY': [],
                'DATA': [0.3, 0.2, 0.1]
              },
              {
                'name': 'i32',
                'count': 3,
                'VALIDITY': [],
                'DATA': [-1, 1, -1]
              },
              {
                'name': 'dictionary',
                'count': 3,
                'VALIDITY': [],
                'DATA': [0, 1, 2]
              }
            ]
          }]
      }),
      values: [
            [Math.fround(-0.3), -1, 'a'],
            [Math.fround(-0.2),  1, 'b'],
            [Math.fround(-0.1), -1, 'c'],
            [Math.fround( 0  ),  1, 'a'],
            [Math.fround( 0.1), -1, 'b'],
            [Math.fround( 0.2),  1, 'c'],
            [Math.fround( 0.3), -1, 'a'],
            [Math.fround( 0.2),  1, 'b'],
            [Math.fround( 0.1), -1, 'c'],
      ]}
];

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
            const table = datum.table;
            const values = datum.values;

            test(`has the correct length`, () => {
                expect(table.length).toEqual(values.length);
            });
            test(`gets expected values`, () => {
                for (let i = -1; ++i < values.length;) {
                    expect(table.get(i).toArray()).toEqual(values[i]);
                }
            });
            test(`iterates expected values`, () => {
                let i = 0;
                for (let row of table) {
                    expect(row.toArray()).toEqual(values[i++]);
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
                    table.scan(() => {}, bind);
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
            const filter_tests = [
                {
                    name:     `filter on f32 >= 0`,
                    filtered: table.filter(col('f32').gteq(0)),
                    expected: values.filter((row) => row[F32] >= 0)
                }, {
                    name:     `filter on 0 <= f32`,
                    filtered: table.filter(lit(0).lteq(col('f32'))),
                    expected: values.filter((row) => 0 <= row[F32])
                }, {
                    name:     `filter on i32 <= 0`,
                    filtered: table.filter(col('i32').lteq(0)),
                    expected: values.filter((row) => row[I32] <= 0)
                }, {
                    name:     `filter on 0 >= i32`,
                    filtered: table.filter(lit(0).gteq(col('i32'))),
                    expected: values.filter((row) => 0 >= row[I32])
                }, {
                    name:     `filter on f32 <= -.25 || f3 >= .25`,
                    filtered: table.filter(col('f32').lteq(-.25).or(col('f32').gteq(.25))),
                    expected: values.filter((row) => row[F32] <= -.25 || row[F32] >= .25)
                }, {
                    name:     `filter method combines predicates (f32 >= 0 && i32 <= 0)`,
                    filtered: table.filter(col('i32').lteq(0)).filter(col('f32').gteq(0)),
                    expected: values.filter((row) => row[I32] <= 0 && row[F32] >= 0)
                }, {
                    name:     `filter on dictionary == 'a'`,
                    filtered: table.filter(col('dictionary').eq('a')),
                    expected: values.filter((row) => row[DICT] === 'a')
                }, {
                    name:     `filter on 'a' == dictionary (commutativity)`,
                    filtered: table.filter(lit('a').eq(col('dictionary'))),
                    expected: values.filter((row) => row[DICT] === 'a')
                }, {
                    name:     `filter on f32 >= i32`,
                    filtered: table.filter(col('f32').gteq(col('i32'))),
                    expected: values.filter((row) => row[F32] >= row[I32])
                }, {
                    name:     `filter on f32 <= i32`,
                    filtered: table.filter(col('f32').lteq(col('i32'))),
                    expected: values.filter((row) => row[F32] <= row[I32])
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
                            filtered.scan(() => {}, bind);
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
                let expected: {[key: string]: number} = {'a': 0, 'b': 0, 'c': 0};
                for (let row of values) {
                    expected[row[DICT]] += 1;
                }

                expect(table.countBy(col('dictionary')).toJSON()).toEqual(expected);
                expect(table.countBy('dictionary').toJSON()).toEqual(expected);
            });
            test(`countBy on dictionary with filter returns the correct counts`, () => {
                let expected: {[key: string]: number} = {'a': 0, 'b': 0, 'c': 0};
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
                    expect(row.get(0)).toEqual(expected_row[F32]);
                    expect(row.get(1)).toEqual(expected_row[DICT]);
                }
            });
            test(`table.toString()`, () => {
                let selected = table.select('i32', 'dictionary');
                let headers  = [`"row_id"`, `"i32: Int32"`, `"dictionary: Dictionary<Int8, Utf8>"`];
                let expected = [headers.join(' | '), ...values.map((row, idx) => {
                    return [`${idx}`, `${row[I32]}`, `"${row[DICT]}"`].map((str, col) => {
                                return leftPad(str, ' ', headers[col].length);
                            }).join(' | ');
                })].join('\n') + '\n';
                expect(selected.toString()).toEqual(expected);
            });
            test(`table.filter(..).count() on always false predicates returns 0`, () => {
                expect(table.filter(col('i32').gteq(100)).count()).toEqual(0);
                expect(table.filter(col('dictionary').eq('z')).count()).toEqual(0);
            });
            describe(`lit-lit comparison`, () => {
                test(`always-false count() returns 0`, () => {
                    expect(table.filter(lit('abc').eq('def')).count()).toEqual(0);
                    expect(table.filter(lit(0).gteq(1)).count()).toEqual(0);
                });
                test(`always-true count() returns length`, () => {
                    expect(table.filter(lit('abc').eq('abc')).count()).toEqual(table.length);
                    expect(table.filter(lit(-100).lteq(0)).count()).toEqual(table.length);
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

function leftPad(str: string, fill: string, n: number) {
    return (new Array(n + 1).join(fill) + str).slice(-1 * n);
}
