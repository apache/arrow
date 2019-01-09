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
// const { predicate, Table, RecordBatchReader } = require('../targets/es5/umd');
// const { predicate, Table, RecordBatchReader } = require('../targets/es5/cjs');
// const { predicate, Table, RecordBatchReader } = require('../targets/es2015/umd');
const { predicate, Table, RecordBatchReader } = require('../targets/es2015/cjs');
const { col } = predicate;

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
    const table = Table.from(buffers), schema = table.schema;
    suites.push(...schema.fields.map((f, i) => createTestSuite(getByIndexSuiteName, createGetByIndexTest(table.getColumnAt(i), f.name))));
    suites.push(...schema.fields.map((f, i) => createTestSuite(iterateSuiteName, createIterateTest(table.getColumnAt(i), f.name))));
    suites.push(...schema.fields.map((f, i) => createTestSuite(sliceToArraySuiteName, createSliceToArrayTest(table.getColumnAt(i), f.name))));
    suites.push(...schema.fields.map((f, i) => createTestSuite(sliceSuiteName, createSliceTest(table.getColumnAt(i), f.name))));
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
        fn() { for (recordBatch of RecordBatchReader.from(buffers)) {} }
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

    if (test == 'gt') {
        op = () => {
            sum = 0;
            let batches = table.chunks;
            let numBatches = batches.length;
            for (let batchIndex = -1; ++batchIndex < numBatches;) {
                // load batches
                const batch = batches[batchIndex];
                const vector = batch.getChildAt(colidx);
                // yield all indices
                for (let index = -1, length = batch.length; ++index < length;) {
                    sum += (vector.get(index) >= value);
                }
            }
            return sum;
        }
    } else if (test == 'eq') {
        op = () => {
            sum = 0;
            let batches = table.chunks;
            let numBatches = batches.length;
            for (let batchIndex = -1; ++batchIndex < numBatches;) {
                // load batches
                const batch = batches[batchIndex];
                const vector = batch.getChildAt(colidx);
                // yield all indices
                for (let index = -1, length = batch.length; ++index < length;) {
                    sum += (vector.get(index) === value);
                }
            }
            return sum;
        }
    } else {
        throw new Error(`Unrecognized test "${test}"`);
    }

    return {
        async: true,
        name: `name: '${column}', length: ${table.length}, type: ${table.getColumnAt(colidx).type}, test: ${test}, value: ${value}\n`,
        fn: op
    };
}

function createDataFrameCountByTest(table, column) {
    let colidx = table.schema.fields.findIndex((c)=> c.name === column);

    return {
        async: true,
        name: `name: '${column}', length: ${table.length}, type: ${table.getColumnAt(colidx).type}\n`,
        fn() {
            table.countBy(column);
        }
    };
}

function createDataFrameFilterCountTest(table, column, test, value) {
    let colidx = table.schema.fields.findIndex((c)=> c.name === column);
    let df;

    if (test == 'gt') {
        df = table.filter(col(column).gt(value));
    } else if (test == 'eq') {
        df = table.filter(col(column).eq(value));
    } else {
        throw new Error(`Unrecognized test "${test}"`);
    }

    return {
        async: true,
        name: `name: '${column}', length: ${table.length}, type: ${table.getColumnAt(colidx).type}, test: ${test}, value: ${value}\n`,
        fn() {
            df.count();
        }
    };
}
