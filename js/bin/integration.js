#! /usr/bin/env node

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

// @ts-nocheck

const fs = require('fs');
const Path = require('path');
const { promisify } = require('util');
const glob = promisify(require('glob'));
const { zip } = require('ix/iterable/zip');
const { parse: bignumJSONParse } = require('json-bignum');
const argv = require(`command-line-args`)(cliOpts(), { partial: true });
const {
    Table,
    RecordBatchReader,
    util: { createElementComparator }
} = require('../targets/apache-arrow/Arrow.es5.min');

const exists = async (p) => {
    try {
        return !!(await fs.promises.stat(p));
    } catch (e) { return false; }
}

(async () => {

    if (!argv.mode) { return print_usage(); }

    let mode = argv.mode.toUpperCase();
    let jsonPaths = [...(argv.json || [])];
    let arrowPaths = [...(argv.arrow || [])];

    if (mode === 'VALIDATE' && !jsonPaths.length) {
        [jsonPaths, arrowPaths] = await loadLocalJSONAndArrowPathsForDebugging(jsonPaths, arrowPaths);
    }

    if (!jsonPaths.length) { return print_usage(); }

    switch (mode) {
        case 'VALIDATE':
            for (let [jsonPath, arrowPath] of zip(jsonPaths, arrowPaths)) {
                await validate(jsonPath, arrowPath);
            }
            break;
        default:
            return print_usage();
    }
})()
.then((x) => +x || 0, (e) => {
    e && process.stderr.write(`${e && e.stack || e}\n`);
    return process.exitCode || 1;
}).then((code) => process.exit(code));

function cliOpts() {
    return [
        {
            type: String,
            name: 'mode',
            description: 'The integration test to run'
        },
        {
            type: String,
            name: 'arrow', alias: 'a',
            multiple: true, defaultValue: [],
            description: 'The Arrow file[s] to read/write'
        },
        {
            type: String,
            name: 'json', alias: 'j',
            multiple: true, defaultValue: [],
            description: 'The JSON file[s] to read/write'
        }
    ];
}

function print_usage() {
    console.log(require('command-line-usage')([
        {
            header: 'integration',
            content: 'Script for running Arrow integration tests'
        },
        {
            header: 'Synopsis',
            content: [
                '$ integration.js -j file.json -a file.arrow --mode validate'
            ]
        },
        {
            header: 'Options',
            optionList: [
                ...cliOpts(),
                {
                    name: 'help',
                    description: 'Print this usage guide.'
                }
            ]
        },
    ]));
    return 1;
}

async function validate(jsonPath, arrowPath) {

    const files = await Promise.all([
        fs.promises.readFile(arrowPath),
        fs.promises.readFile(jsonPath, 'utf8'),
    ]);

    const arrowData = files[0];
    const jsonData = bignumJSONParse(files[1]);

    validateReaderIntegration(jsonData, arrowData);
    validateTableFromBuffersIntegration(jsonData, arrowData);
    validateTableToBuffersIntegration('json', 'file')(jsonData, arrowData);
    validateTableToBuffersIntegration('json', 'file')(jsonData, arrowData);
    validateTableToBuffersIntegration('binary', 'file')(jsonData, arrowData);
    validateTableToBuffersIntegration('binary', 'file')(jsonData, arrowData);
}

function validateReaderIntegration(jsonData, arrowBuffer) {
    const msg = `json and arrow record batches report the same values`;
    try {
        const jsonReader = RecordBatchReader.from(jsonData);
        const binaryReader = RecordBatchReader.from(arrowBuffer);
        for (const [jsonRecordBatch, binaryRecordBatch] of zip(jsonReader, binaryReader)) {
            compareTableIsh(jsonRecordBatch, binaryRecordBatch);
        }
    } catch (e) { throw new Error(`${msg}: fail \n ${e && e.stack || e}`); }
    process.stdout.write(`${msg}: pass\n`);
}

function validateTableFromBuffersIntegration(jsonData, arrowBuffer) {
    const msg = `json and arrow tables report the same values`;
    try {
        const jsonTable = Table.from(jsonData);
        const binaryTable = Table.from(arrowBuffer);
        compareTableIsh(jsonTable, binaryTable);
    } catch (e) { throw new Error(`${msg}: fail \n ${e && e.stack || e}`); }
    process.stdout.write(`${msg}: pass\n`);
}

function validateTableToBuffersIntegration(srcFormat, arrowFormat) {
    const refFormat = srcFormat === `json` ? `binary` : `json`;
    return function testTableToBuffersIntegration(jsonData, arrowBuffer) {
        const msg = `serialized ${srcFormat} ${arrowFormat} reports the same values as the ${refFormat} ${arrowFormat}`;
        try {
            const refTable = Table.from(refFormat === `json` ? jsonData : arrowBuffer);
            const srcTable = Table.from(srcFormat === `json` ? jsonData : arrowBuffer);
            const dstTable = Table.from(srcTable.serialize(`binary`, arrowFormat === `stream`));
            compareTableIsh(dstTable, refTable);
        } catch (e) { throw new Error(`${msg}: fail \n ${e && e.stack || e}`); }
        process.stdout.write(`${msg}: pass\n`);
    };
}

function compareTableIsh(actual, expected) {
    if (actual.length !== expected.length) {
        throw new Error(`length: ${actual.length} !== ${expected.length}`);
    }
    if (actual.numCols !== expected.numCols) {
        throw new Error(`numCols: ${actual.numCols} !== ${expected.numCols}`);
    }
    (() => {
        const getChildAtFn = expected instanceof Table ? 'getColumnAt' : 'getChildAt';
        for (let i = -1, n = actual.numCols; ++i < n;) {
            const v1 = actual[getChildAtFn](i);
            const v2 = expected[getChildAtFn](i);
            compareVectors(v1, v2);
        }
    })();
}

function compareVectors(actual, expected) {

    if ((actual == null && expected != null) || (expected == null && actual != null)) {
        throw new Error(`${actual == null ? `actual` : `expected`} is null, was expecting ${actual == null ? expected : actual} to be that also`);
    }

    let props = ['type', 'length', 'nullCount'];

    (() => {
        for (let i = -1, n = props.length; ++i < n;) {
            const prop = props[i];
            if (`${actual[prop]}` !== `${expected[prop]}`) {
                throw new Error(`${prop}: ${actual[prop]} !== ${expected[prop]}`);
            }
        }
    })();

    (() => {
        for (let i = -1, n = actual.length; ++i < n;) {
            let x1 = actual.get(i), x2 = expected.get(i);
            if (!createElementComparator(x2)(x1)) {
                throw new Error(`${i}: ${x1} !== ${x2}`);
            }
        }
    })();

    (() => {
        let i = -1;
        for (let [x1, x2] of zip(actual, expected)) {
            ++i;
            if (!createElementComparator(x2)(x1)) {
                throw new Error(`${i}: ${x1} !== ${x2}`);
            }
        }
    })();
}

async function loadLocalJSONAndArrowPathsForDebugging(jsonPaths, arrowPaths) {

    const sourceJSONPaths = await glob(Path.resolve(__dirname, `../test/data/json/`, `*.json`));

    if (!arrowPaths.length) {
        await loadJSONAndArrowPaths(sourceJSONPaths, jsonPaths, arrowPaths, 'cpp', 'file');
        await loadJSONAndArrowPaths(sourceJSONPaths, jsonPaths, arrowPaths, 'java', 'file');
        await loadJSONAndArrowPaths(sourceJSONPaths, jsonPaths, arrowPaths, 'cpp', 'stream');
        await loadJSONAndArrowPaths(sourceJSONPaths, jsonPaths, arrowPaths, 'java', 'stream');
    }

    for (let [jsonPath, arrowPath] of zip(jsonPaths, arrowPaths)) {
        console.log(`jsonPath: ${jsonPath}`);
        console.log(`arrowPath: ${arrowPath}`);
    }

    return [jsonPaths, arrowPaths];

    async function loadJSONAndArrowPaths(sourceJSONPaths, jsonPaths, arrowPaths, source, format) {
        for (const jsonPath of sourceJSONPaths) {
            const { name } = Path.parse(jsonPath);
            const arrowPath = Path.resolve(__dirname, `../test/data/${source}/${format}/${name}.arrow`);
            if (await exists(arrowPath)) {
                jsonPaths.push(jsonPath);
                arrowPaths.push(arrowPath);
            }
        }
        return [jsonPaths, arrowPaths];
    }
}
