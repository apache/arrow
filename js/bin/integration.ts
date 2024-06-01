#! /usr/bin/env -S node --no-warnings --loader ts-node/esm/transpile-only

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

import * as fs from 'node:fs';
import * as Path from 'node:path';
import { glob } from 'glob';
import { zip } from 'ix/iterable/zip';
import commandLineArgs from 'command-line-args';
// @ts-ignore
import { parse as bignumJSONParse } from 'json-bignum';

import {
    Table,
    Vector,
    RecordBatch,
    ArrowJSONLike,
    RecordBatchReader,
    RecordBatchStreamWriter,
    util,
} from '../index.ts';

const { createElementComparator } = util;
const argv = commandLineArgs(cliOpts(), { partial: true });

const exists = async (p: string) => {
    try {
        return !!(await fs.promises.stat(p));
    } catch { return false; }
};

(async () => {

    if (!argv.mode) { return print_usage(); }

    const mode = argv.mode.toUpperCase();
    let jsonPaths = [...(argv.json || [])];
    let arrowPaths = [...(argv.arrow || [])];

    if (mode === 'VALIDATE' && jsonPaths.length === 0) {
        [jsonPaths, arrowPaths] = await loadLocalJSONAndArrowPathsForDebugging(jsonPaths, arrowPaths);
    }

    if (jsonPaths.length === 0) { return print_usage(); }

    let threw = false;

    switch (mode) {
        case 'VALIDATE':
            for (const [jsonPath, arrowPath] of zip(jsonPaths, arrowPaths)) {
                try {
                    await validate(jsonPath, arrowPath);
                } catch (e: any) {
                    threw = true;
                    e && process.stderr.write(`${e?.stack || e}\n`);
                }
            }
            break;
        default:
            return print_usage();
    }

    return threw ? 1 : 0;
})()
    .then((x) => +x || 0, (e) => {
        e && process.stderr.write(`${e?.stack || e}\n`);
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
                '$ integration.ts -j file.json -a file.arrow --mode validate'
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

async function validate(jsonPath: string, arrowPath: string) {

    const files = await Promise.all([
        fs.promises.readFile(arrowPath),
        fs.promises.readFile(jsonPath, 'utf8'),
    ]);

    const arrowData = files[0];
    const jsonData = bignumJSONParse(files[1]);

    process.stdout.write(`\n`);
    process.stdout.write(` json: ${jsonPath}\n`);
    process.stdout.write(`arrow: ${arrowPath}\n`);

    validateReaderIntegration(jsonData, arrowData);
    validateTableFromBuffersIntegration(jsonData, arrowData);
    validateTableToBuffersIntegration('json', 'file')(jsonData, arrowData);
    validateTableToBuffersIntegration('json', 'file')(jsonData, arrowData);
    validateTableToBuffersIntegration('binary', 'file')(jsonData, arrowData);
    validateTableToBuffersIntegration('binary', 'file')(jsonData, arrowData);
}

function validateReaderIntegration(jsonData: ArrowJSONLike, arrowBuffer: Uint8Array) {
    const msg = `json and arrow record batches report the same values`;
    try {
        const jsonReader = RecordBatchReader.from(jsonData);
        const binaryReader = RecordBatchReader.from(arrowBuffer);
        for (const [jsonRecordBatch, binaryRecordBatch] of zip(jsonReader, binaryReader)) {
            compareTableIsh(jsonRecordBatch, binaryRecordBatch);
        }
    } catch (e: any) { throw new Error(`${msg}: fail \n ${e?.stack || e}`); }
    process.stdout.write(`${msg}: pass\n`);
}

function validateTableFromBuffersIntegration(jsonData: ArrowJSONLike, arrowBuffer: Uint8Array) {
    const msg = `json and arrow tables report the same values`;
    try {
        const jsonTable = new Table(RecordBatchReader.from(jsonData));
        const binaryTable = new Table(RecordBatchReader.from(arrowBuffer));
        compareTableIsh(jsonTable, binaryTable);
    } catch (e: any) { throw new Error(`${msg}: fail \n ${e?.stack || e}`); }
    process.stdout.write(`${msg}: pass\n`);
}

function validateTableToBuffersIntegration(srcFormat: 'json' | 'binary', arrowFormat: 'file' | 'stream') {
    const refFormat = srcFormat === `json` ? `binary` : `json`;
    return function testTableToBuffersIntegration(jsonData: ArrowJSONLike, arrowBuffer: Uint8Array) {
        const msg = `serialized ${srcFormat} ${arrowFormat} reports the same values as the ${refFormat} ${arrowFormat}`;
        try {
            const refTable = new Table(refFormat === `json` ? RecordBatchReader.from(jsonData) : RecordBatchReader.from(arrowBuffer));
            const srcTable = new Table(srcFormat === `json` ? RecordBatchReader.from(jsonData) : RecordBatchReader.from(arrowBuffer));
            const dstTable = new Table(RecordBatchReader.from(RecordBatchStreamWriter.writeAll(srcTable).toUint8Array(true)));
            compareTableIsh(dstTable, refTable);
        } catch (e: any) { throw new Error(`${msg}: fail \n ${e?.stack || e}`); }
        process.stdout.write(`${msg}: pass\n`);
    };
}

function compareTableIsh(actual: Table | RecordBatch, expected: Table | RecordBatch) {
    if (actual.numRows !== expected.numRows) {
        throw new Error(`numRows: ${actual.numRows} !== ${expected.numRows}`);
    }
    if (actual.numCols !== expected.numCols) {
        throw new Error(`numCols: ${actual.numCols} !== ${expected.numCols}`);
    }
    (() => {
        for (let i = -1, n = actual.numCols; ++i < n;) {
            const v1 = actual.getChildAt(i)!;
            const v2 = expected.getChildAt(i)!;
            compareVectors(v1, v2);
        }
    })();
}

function compareVectors(actual: Vector, expected: Vector) {

    if ((actual == null && expected != null) || (expected == null && actual != null)) {
        throw new Error(`${actual == null ? `actual` : `expected`} is null, was expecting ${actual ?? expected} to be that also`);
    }

    const props = ['type', 'length', 'nullCount'] as (keyof Vector & string)[];

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
            const x1 = actual.get(i), x2 = expected.get(i);
            if (!createElementComparator(x2)(x1)) {
                throw new Error(`${i}: ${x1} !== ${x2}`);
            }
        }
    })();

    (() => {
        let i = -1;
        for (const [x1, x2] of zip(actual, expected)) {
            ++i;
            if (!createElementComparator(x2)(x1)) {
                throw new Error(`${i}: ${x1} !== ${x2}`);
            }
        }
    })();
}

async function loadLocalJSONAndArrowPathsForDebugging(jsonPaths: string[], arrowPaths: string[]) {

    const sourceJSONPaths = await glob(Path.resolve(__dirname, `../test/data/json/`, `*.json`));

    if (arrowPaths.length === 0) {
        await loadJSONAndArrowPaths(sourceJSONPaths, jsonPaths, arrowPaths, 'cpp', 'file');
        await loadJSONAndArrowPaths(sourceJSONPaths, jsonPaths, arrowPaths, 'java', 'file');
        await loadJSONAndArrowPaths(sourceJSONPaths, jsonPaths, arrowPaths, 'cpp', 'stream');
        await loadJSONAndArrowPaths(sourceJSONPaths, jsonPaths, arrowPaths, 'java', 'stream');
    }

    for (const [jsonPath, arrowPath] of zip(jsonPaths, arrowPaths)) {
        console.log(`jsonPath: ${jsonPath}`);
        console.log(`arrowPath: ${arrowPath}`);
    }

    return [jsonPaths, arrowPaths];

    async function loadJSONAndArrowPaths(
        sourceJSONPaths: string[],
        jsonPaths: string[],
        arrowPaths: string[],
        source: 'cpp' | 'java',
        format: 'file' | 'stream'
    ) {
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
