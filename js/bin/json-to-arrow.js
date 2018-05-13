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

const fs = require('fs');
const glob = require('glob');
const path = require('path');
const { promisify } = require('util');
const { parse } = require('json-bignum');
const argv = require(`command-line-args`)(cliOpts(), { partial: true });

const ext = process.env.ARROW_JS_DEBUG === 'src' ? '.ts' : '';
const { Table } = require(`../index${ext}`);

const encoding = 'binary';
const stream = argv.format === 'stream';
const jsonPaths = [...(argv.json || [])];
const arrowPaths = [...(argv.arrow || [])];

if (!jsonPaths.length || !arrowPaths.length || (jsonPaths.length !== arrowPaths.length)) {
    return print_usage();
}

const readFile = callResolved(promisify(fs.readFile));
const writeFile = callResolved(promisify(fs.writeFile));

(async () => await Promise.all(jsonPaths.map(async (jPath, i) => {
    const aPath = arrowPaths[i];
    const arrowTable = Table.from(parse('' + (await readFile(jPath))));
    await writeFile(aPath, arrowTable.serialize(encoding, stream), encoding);
})))().catch((e) => { console.error(e); process.exit(1); });

function callResolved(fn) {
    return async (path_, ...xs) => await fn(path.resolve(path_), ...xs);
}

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
    process.exit(1);
}
