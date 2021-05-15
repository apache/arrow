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
const { predicate, Table, DataFrame, RecordBatchReader } = require('../targets/es2015/cjs');
const kleur = require('kleur');
const b = require('benny');
const { col } = predicate;

const args = process.argv.slice(2);
const json = args[0] === '--json';

const formatter = new Intl.NumberFormat();
function formatNumber(number, precision) {
    const rounded = number > precision * 10 ? Math.round(number) : parseFloat((number).toPrecision(precision));
    return formatter.format(rounded)
}

const results = []

function cycle(result, _summary) {
    const duration = result.details.median * 1000;
    if (json) {
        results.push(result);
    }
    console.log(
        `${kleur.cyan(result.name)} ${formatNumber(result.ops, 3)} ops/s ±${result.margin.toPrecision(2)}%, ${formatNumber(duration, 2)} ms, ${kleur.gray(result.samples + ' samples')}`,
    );
}

for (const { name, buffers } of require('./config')) {
    b.suite(
        `Parse "${name}"`,

        b.add(`Table.from`, () => {
            Table.from(buffers);
        }),

        b.add(`readBatches`, () => {
            for (recordBatch of RecordBatchReader.from(buffers)) {}
        }),

        b.cycle(cycle)
    );

    const table = Table.from(buffers)
    const schema = table.schema;

    const suites = [{
            name: `Get "${name}" values by index`,
            fn(vector) {
                for (let i = -1, n = vector.length; ++i < n;) {
                    value = vector.get(i);
                }
            }
        }, {
            name: `Iterate "${name}" vectors`,
            fn(vector) { for (value of vector) {} }
        }, {
            name: `Slice toArray "${name}" vectors`,
            fn(vector) { xs = vector.slice().toArray(); }
        }, {
            name: `Slice "${name}" vectors`,
            fn(vector) { xs = vector.slice(); }
        }];

    for (const {name, fn} of suites) {
        b.suite(
            name,

            ...schema.fields.map((f, i) => {
                const vector = table.getColumnAt(i);
                return b.add(`name: '${f.name}', length: ${formatNumber(vector.length)}, type: ${vector.type}`, () => {
                    fn(vector)
                })
            }),

            b.cycle(cycle)
        );
    }
}


for (const { name, buffers, countBys, counts } of require('./config')) {
    const df = DataFrame.from(buffers);

    b.suite(
        `DataFrame Iterate "${name}"`,

        b.add(`length: ${formatNumber(df.length)}`, () => {
            for (value of df) {}
        }),

        b.cycle(cycle)
    );

    b.suite(
        `DataFrame Count By "${name}"`,

        ...countBys.map((column) => b.add(
            `name: '${column}', length: ${formatNumber(df.length)}, type: ${df.schema.fields.find((c)=> c.name === column).type}`,
            () => df.countBy(column)
        )),

        b.cycle(cycle)
    );

    b.suite(
        `DataFrame Filter-Scan Count "${name}"`,

        ...counts.map(({ column, test, value }) => b.add(
            `name: '${column}', length: ${formatNumber(df.length)}, type: ${df.schema.fields.find((c)=> c.name === column).type}, test: ${test}, value: ${value}`,
            () => {
                let filteredDf;
                if (test == 'gt') {
                    filteredDf = df.filter(col(column).gt(value));
                } else if (test == 'eq') {
                    filteredDf = df.filter(col(column).eq(value));
                } else {
                    throw new Error(`Unrecognized test "${test}"`);
                }

                return () => filteredDf.count();
            }
        )),

        b.cycle(cycle)
    );

    b.suite(
        `DataFrame Filter-Iterate "${name}"`,

        ...counts.map(({ column, test, value }) => b.add(
            `name: '${column}', length: ${formatNumber(df.length)}, type: ${df.schema.fields.find((c)=> c.name === column).type}, test: ${test}, value: ${value}`,
            () => {
                let filteredDf;
                if (test == 'gt') {
                    filteredDf = df.filter(col(column).gt(value));
                } else if (test == 'eq') {
                    filteredDf = df.filter(col(column).eq(value));
                } else {
                    throw new Error(`Unrecognized test "${test}"`);
                }

                return () => {
                    for (value of filteredDf) {}
                }
            }
        )),

        b.cycle(cycle)
    );

    b.suite(
        `DataFrame Direct Count "${name}"`,

        ...counts.map(({ column, test, value }) => b.add(
            `name: '${column}', length: ${formatNumber(df.length)}, type: ${df.schema.fields.find((c)=> c.name === column).type}, test: ${test}, value: ${value}`,
            () => {
                let colidx = df.schema.fields.findIndex((c)=> c.name === column);

                if (test == 'gt') {
                    return () => {
                        sum = 0;
                        let batches = df.chunks;
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
                    return () => {
                        sum = 0;
                        let batches = df.chunks;
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
            }
        )),

        b.cycle(cycle),

        b.complete(() => {
            // last benchmark finished
            json && process.stderr.write(JSON.stringify(results, null, 2))
        })
    );
}
