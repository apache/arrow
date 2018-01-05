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

// Use the ES5 UMD target as perf baseline
// const { Table, readVectors } = require('../targets/es5/umd');
// const { Table, readVectors } = require('../targets/es5/cjs');
const { Table, readVectors } = require('../targets/es2015/umd');
// const { Table, readVectors } = require('../targets/es2015/cjs');

const config = require('./config');
const Benchmark = require('benchmark');

const suites = [];

for (let { name, buffers} of config) {
    const parseSuite = new Benchmark.Suite(`Parse ${name}`, { async: true });
    const sliceSuite = new Benchmark.Suite(`Slice ${name} vectors`, { async: true });
    const iterateSuite = new Benchmark.Suite(`Iterate ${name} vectors`, { async: true });
    const getByIndexSuite = new Benchmark.Suite(`Get ${name} values by index`, { async: true });
    parseSuite.add(createFromTableTest(name, buffers));
    parseSuite.add(createReadVectorsTest(name, buffers));
    for (const vector of Table.from(buffers).columns) {
        sliceSuite.add(createSliceTest(vector));
        iterateSuite.add(createIterateTest(vector));
        getByIndexSuite.add(createGetByIndexTest(vector));
    }
    suites.push(getByIndexSuite, iterateSuite, sliceSuite, parseSuite);
}

for (let {name, buffers, tests} of require('./table_config')) {
    const tableIterateSuite = new Benchmark.Suite(`Table Iterate ${name}`, { async: true });
    const tableCountBySuite = new Benchmark.Suite(`Table Count By ${name}`, { async: true });
    const vectorCountBySuite = new Benchmark.Suite(`Vector Count By ${name}`, { async: true });
    const table = Table.from(buffers);

    tableIterateSuite.add(createTableIterateTest(table));
    for (test of tests) {
        tableCountBySuite.add(createTableCountByTest(table, test.col, test.test, test.value))
        vectorCountBySuite.add(createVectorCountByTest(table.columns[test.col], test.test, test.value))
    }

    suites.push(tableIterateSuite, tableCountBySuite, vectorCountBySuite)
}

console.log('Running apache-arrow performance tests...\n');

run();

function run() {
    var suite = suites.shift();
    suite && suite.on('complete', function() {
        console.log(suite.name + ':\n' + this.map(function(x) {
            var str = x.toString();
            var meanMsPerOp = Math.round(x.stats.mean * 100000)/100;
            var sliceOf60FPS = Math.round((meanMsPerOp / (1000/60)) * 100000)/1000;
            return `${str} (avg: ${meanMsPerOp}ms, or ${sliceOf60FPS}% of a frame @ 60FPS) ${x.suffix || ''}`;
        }).join('\n') + '\n');
        if (suites.length > 0) {
            setTimeout(run, 1000);
        }
    })
    .run({ async: true });
}

function createFromTableTest(name, buffers) {
    let table;
    return {
        async: true,
        name: `Table.from`,
        fn() { table = Table.from(buffers); }
    };
}

function createReadVectorsTest(name, buffers) {
    let vectors;
    return {
        async: true,
        name: `readVectors`,
        fn() { for (vectors of readVectors(buffers)) {} }
    };
}

function createSliceTest(vector) {
    let xs;
    return {
        async: true,
        name: `name: '${vector.name}', length: ${vector.length}, type: ${vector.type}`,
        fn() { xs = vector.slice(); }
    };
}

function createIterateTest(vector) {
    let value;
    return {
        async: true,
        name: `name: '${vector.name}', length: ${vector.length}, type: ${vector.type}`,
        fn() { for (value of vector) {} }
    };
}

function createGetByIndexTest(vector) {
    let value;
    return {
        async: true,
        name: `name: '${vector.name}', length: ${vector.length}, type: ${vector.type}`,
        fn() {
            for (let i = -1, n = vector.length; ++i < n;) {
                value = vector.get(i);
            }
        }
    };
}

function createVectorCountByTest(vector, test, value) {
    let op;
    if (test == 'gteq') {
        op = function () {
            sum = 0;
            for (cell of vector) {
                sum += (cell >= value)
            }
        }
    } else if (test == 'eq') {
        op = function () {
            sum = 0;
            for (cell of vector) {
                sum += (cell == value)
            }
        }
    } else {
        throw new Error(`Unrecognized test "$test"`);
    }

    return {
        async: true,
        name: `name: '${vector.name}', length: ${vector.length}, type: ${vector.type}, test: ${test}, value: ${value}`,
        fn: op
    };
}

function createTableIterateTest(table) {
    let row;
    return {
        async: true,
        name: `length: ${table.length}`,
        fn() { for (row of table) {} }
    };
}

function createTableCountByTest(table, column, test, value) {
    let op;
    if (test == 'gteq') {
        op = function () {
            sum = 0;
            for (row of table) {
                sum += (row.get(column) >= value)
            }
        }
    } else if (test == 'eq') {
        op = function() {
            sum = 0;
            for (row of table) {
                sum += (row.get(column) == value)
            }
        }
    } else {
        throw new Error(`Unrecognized test "${test}"`);
    }

    return {
        async: true,
        name: `name: '${table.columns[column].name}', length: ${table.length}, type: ${table.columns[column].type}, test: ${test}, value: ${value}`,
        fn: op
    };
}
