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

import Arrow, {
} from '../Arrow';

const {
    col,
    Table,
} = Arrow;

describe(`Table`, () => {
    describe(`single record batch`, () => {
        const table = Table.from({
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
        });

        // Wrap floating point values in a Float32Array and take them back out to
        // make sure that equality checks will pass
        const values = [
            [new Float32Array([-0.3])[0], -1, 'a'],
            [new Float32Array([-0.2])[0],  1, 'b'],
            [new Float32Array([-0.1])[0], -1, 'c'],
            [new Float32Array([ 0  ])[0],  1, 'a'],
            [new Float32Array([ 0.1])[0], -1, 'b'],
            [new Float32Array([ 0.2])[0],  1, 'c'],
            [new Float32Array([ 0.3])[0], -1, 'a']
        ];
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
        test(`scans expected values`, () => {
            let expected_idx = 0;
            table.scan((idx, cols) => {
                expect(cols.map((c) => c.get(idx))).toEqual(values[expected_idx++]);
            });
        });
        test(`count() returns the correct length`, () => {
            expect(table.count()).toEqual(values.length);
        });
        test(`filter on f32 >= 0 returns the correct length`, () => {
            expect(table.filter(col('f32').gteq(0)).count()).toEqual(4);
        });
        test(`filter on i32 <= 0 returns the correct length`, () => {
            expect(table.filter(col('i32').lteq(0)).count()).toEqual(4);
        });
        test(`filter on dictionary == 'a' returns the correct length`, () => {
            expect(table.filter(col('dictionary').eq('a')).count()).toEqual(3);
        });
        test(`countBy on dictionary returns the correct counts`, () => {
            // Make sure countBy works both with and without the Col wrapper
            // class
            expect(table.countBy(col('dictionary')).toJSON()).toEqual({
                'a': 3,
                'b': 2,
                'c': 2,
            });
            expect(table.countBy('dictionary').toJSON()).toEqual({
                'a': 3,
                'b': 2,
                'c': 2,
            });
        });
        test(`countBy on dictionary with filter returns the correct counts`, () => {
            expect(table.filter(col('i32').eq(1)).countBy('dictionary').toJSON()).toEqual({
                'a': 1,
                'b': 1,
                'c': 1,
            });
        });
        test(`countBy on non dictionary column throws error`, () => {
            expect(() => { table.countBy('i32'); }).toThrow();
        });
    });
    describe(`multiple record batches`, () => {
        const table = Table.from({
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
        });

        // Wrap floating point values in a Float32Array and take them back out to
        // make sure that equality checks will pass
        const values = [
            [new Float32Array([-0.3])[0], -1, 'a'],
            [new Float32Array([-0.2])[0],  1, 'b'],
            [new Float32Array([-0.1])[0], -1, 'c'],
            [new Float32Array([ 0  ])[0],  1, 'a'],
            [new Float32Array([ 0.1])[0], -1, 'b'],
            [new Float32Array([ 0.2])[0],  1, 'c'],
            [new Float32Array([ 0.3])[0], -1, 'a'],
            [new Float32Array([ 0.2])[0],  1, 'b'],
            [new Float32Array([ 0.1])[0], -1, 'c'],
        ];
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
        test(`scans expected values`, () => {
            let expected_idx = 0;
            table.scan((idx, cols) => {
                expect(cols.map((c) => c.get(idx))).toEqual(values[expected_idx++]);
            });
        });
        test(`count() returns the correct length`, () => {
            expect(table.count()).toEqual(values.length);
        });
        test(`filter on f32 >= 0 returns the correct length`, () => {
            expect(table.filter(col('f32').gteq(0)).count()).toEqual(6);
        });
        test(`filter on i32 <= 0 returns the correct length`, () => {
            expect(table.filter(col('i32').lteq(0)).count()).toEqual(5);
        });
        test(`filter on dictionary == 'a' returns the correct length`, () => {
            expect(table.filter(col('dictionary').eq('a')).count()).toEqual(3);
        });
        test(`countBy on dictionary returns the correct counts`, () => {
            // Make sure countBy works both with and without the Col wrapper
            // class
            expect(table.countBy(col('dictionary')).toJSON()).toEqual({
                'a': 3,
                'b': 3,
                'c': 3,
            });
            expect(table.countBy('dictionary').toJSON()).toEqual({
                'a': 3,
                'b': 3,
                'c': 3,
            });
        });
        test(`countBy on dictionary with filter returns the correct counts`, () => {
            expect(table.filter(col('i32').eq(1)).countBy(col('dictionary')).toJSON()).toEqual({
                'a': 1,
                'b': 2,
                'c': 1,
            });
        });
        test(`countBy on non dictionary column throws error`, () => {
            expect(() => { table.countBy('i32'); }).toThrow();
        });
    });
});
