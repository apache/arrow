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
// const { col, Table, read: readBatches } = require('../targets/es5/umd');
// const { col, Table, read: readBatches } = require('../targets/es5/cjs');
// const { col, Table, read: readBatches } = require('../targets/es2015/umd');
const { col, Table, read: readBatches } = require('../targets/es2015/cjs');

const Benchmark = require('benchmark');

const suites = [];

for (let { name, buffers } of require('./table_config')) {
    const parseSuiteName = `Parse "${name}"`;
    const sliceSuiteName = `Slice "${name}" vectors`;
    const iterateSuiteName = `Iterate "${name}" vectors`;
    const getByIndexSuiteName = `Get "${name}" values by index`;
    const sliceToArraySuiteName = `Slice toArray "${name}" vectors`;
    suites.push(createTestSuite(parseSuiteName, createFromTableTest(name, buffers)));
    suites.push(createTestSuite(parseSuiteName, createReadBatchesTest(name, buffers)));
    const table = Table.from(buffers);
    suites.push(...table.columns.map((vector, i) => createTestSuite(getByIndexSuiteName, createGetByIndexTest(vector, table.schema.fields[i].name))));
    suites.push(...table.columns.map((vector, i) => createTestSuite(iterateSuiteName, createIterateTest(vector, table.schema.fields[i].name))));
    suites.push(...table.columns.map((vector, i) => createTestSuite(sliceToArraySuiteName, createSliceToArrayTest(vector, table.schema.fields[i].name))));
    suites.push(...table.columns.map((vector, i) => createTestSuite(sliceSuiteName, createSliceTest(vector, table.schema.fields[i].name))));
}

for (let {name, buffers, countBys, counts} of require('./table_config')) {
    const table = Table.from(buffers);

    const dfCountBySuiteName = `DataFrame Count By "${name}"`;
    const dfFilterCountSuiteName = `DataFrame Filter-Scan Count "${name}"`;
    const dfDirectCountSuiteName = `DataFrame Direct Count "${name}"`;

    suites.push(...countBys.map((countBy) => createTestSuite(dfCountBySuiteName, createDataFrameCountByTest(table, countBy))));
    suites.push(...counts.map(({ col, test, value }) => createTestSuite(dfFilterCountSuiteName, createDataFrameFilterCountTest(table, col, test, value))));
    suites.push(...counts.map(({ col, test, value }) => createTestSuite(dfDirectCountSuiteName, createDataFrameDirectCountTest(table, col, test, value))));
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
            return `${str}\n   avg: ${meanMsPerOp}ms\n   ${sliceOf60FPS}% of a frame @ 60FPS ${x.suffix || ''}`;
        }).join('\n') + '\n');
        if (suites.length > 0) {
            setTimeout(run, 1000);
        }
    })
    .run({ async: true });
}

function createTestSuite(name, test) {
    return new Benchmark.Suite(name, { async: true }).add(test);
}

function createFromTableTest(name, buffers) {
    let table;
    return {
        async: true,
        name: `Table.from\n`,
        fn() { table = Table.from(buffers); }
    };
}

function createReadBatchesTest(name, buffers) {
    let recordBatch;
    return {
        async: true,
        name: `readBatches\n`,
        fn() { for (recordBatch of readBatches(buffers)) {} }
    };
}

function createSliceTest(vector, name) {
    let xs;
    return {
        async: true,
        name: `name: '${name}', length: ${vector.length}, type: ${vector.type}\n`,
        fn() { xs = vector.slice(); }
    };
}

function createSliceToArrayTest(vector, name) {
    let xs;
    return {
        async: true,
        name: `name: '${name}', length: ${vector.length}, type: ${vector.type}\n`,
        fn() { xs = vector.slice().toArray(); }
    };
}

function createIterateTest(vector, name) {
    let value;
    return {
        async: true,
        name: `name: '${name}', length: ${vector.length}, type: ${vector.type}\n`,
        fn() { for (value of vector) {} }
    };
}

function createGetByIndexTest(vector, name) {
    let value;
    return {
        async: true,
        name: `name: '${name}', length: ${vector.length}, type: ${vector.type}\n`,
        fn() {
            for (let i = -1, n = vector.length; ++i < n;) {
                value = vector.get(i);
            }
        }
    };
}

function createDataFrameDirectCountTest(table, column, test, value) {
    let sum, colidx = table.schema.fields.findIndex((c)=>c.name === column);

    if (test == 'gteq') {
        op = function () {
            sum = 0;
            let batches = table.batches;
            let numBatches = batches.length;
            for (let batchIndex = -1; ++batchIndex < numBatches;) {
                // load batches
                const { numRows, columns } = batches[batchIndex];
                const vector = columns[colidx];
                // yield all indices
                for (let index = -1; ++index < numRows;) {
                    sum += (vector.get(index) >= value);
                }
            }
        }
    } else if (test == 'eq') {
        op = function() {
            sum = 0;
            let batches = table.batches;
            let numBatches = batches.length;
            for (let batchIndex = -1; ++batchIndex < numBatches;) {
                // load batches
                const { numRows, columns } = batches[batchIndex];
                const vector = columns[colidx];
                // yield all indices
                for (let index = -1; ++index < numRows;) {
                    sum += (vector.get(index) === value);
                }
            }
        }
    } else {
        throw new Error(`Unrecognized test "${test}"`);
    }

    return {
        async: true,
        name: `name: '${column}', length: ${table.numRows}, type: ${table.columns[colidx].type}, test: ${test}, value: ${value}\n`,
        fn: op
    };
}

function createDataFrameCountByTest(table, column) {
    let colidx = table.schema.fields.findIndex((c)=> c.name === column);

    return {
        async: true,
        name: `name: '${column}', length: ${table.numRows}, type: ${table.columns[colidx].type}\n`,
        fn() {
            table.countBy(column);
        }
    };
}

function createDataFrameFilterCountTest(table, column, test, value) {
    let colidx = table.schema.fields.findIndex((c)=> c.name === column);
    let df;

    if (test == 'gteq') {
        df = table.filter(col(column).gteq(value));
    } else if (test == 'eq') {
        df = table.filter(col(column).eq(value));
    } else {
        throw new Error(`Unrecognized test "${test}"`);
    }

    return {
        async: true,
        name: `name: '${column}', length: ${table.numRows}, type: ${table.columns[colidx].type}, test: ${test}, value: ${value}\n`,
        fn() {
            df.count();
        }
    };
}
