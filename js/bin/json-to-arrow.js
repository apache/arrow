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

// @ts-check

const fs = require('fs');
const Path = require('path');
const { parse } = require('json-bignum');
const eos = require('util').promisify(require('stream').finished);
const extension = process.env.ARROW_JS_DEBUG === 'src' ? '.ts' : '';
const argv = require(`command-line-args`)(cliOpts(), { partial: true });
const { RecordBatchReader, RecordBatchFileWriter, RecordBatchStreamWriter } = require(`../index${extension}`);

const jsonPaths = [...(argv.json || [])];
const arrowPaths = [...(argv.arrow || [])];

(async () => {

    if (!jsonPaths.length || !arrowPaths.length || (jsonPaths.length !== arrowPaths.length)) {
        return print_usage();
    }

    await Promise.all(jsonPaths.map(async (path, i) => {
        
        const RecordBatchWriter = argv.format !== 'stream'
            ? RecordBatchFileWriter
            : RecordBatchStreamWriter;

        const reader = RecordBatchReader.from(parse(
            await fs.promises.readFile(Path.resolve(path), 'utf8')));

        const jsonToArrow = reader
            .pipe(RecordBatchWriter.throughNode())
            .pipe(fs.createWriteStream(arrowPaths[i]));

        await eos(jsonToArrow);

    }));
})()
.then((x) => +x || 0, (e) => {
    e && process.stderr.write(`${e}`);
    return process.exitCode || 1;
}).then((code = 0) => process.exit(code));

function cliOpts() {
    return [
        {
            type: String,
            name: 'format', alias: 'f',
            multiple: false, defaultValue: 'file',
            description: 'The Arrow format to write, either "file" or "stream"'
        },
        {
            type: String,
            name: 'arrow', alias: 'a',
            multiple: true, defaultValue: [],
            description: 'The Arrow file[s] to write'
        },
        {
            type: String,
            name: 'json', alias: 'j',
            multiple: true, defaultValue: [],
            description: 'The JSON file[s] to read'
        }
    ];
}

function print_usage() {
    console.log(require('command-line-usage')([
        {
            header: 'json-to-arrow',
            content: 'Script for converting an JSON Arrow file to a binary Arrow file'
        },
        {
            header: 'Synopsis',
            content: [
                '$ json-to-arrow.js -j in.json -a out.arrow -f stream'
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
