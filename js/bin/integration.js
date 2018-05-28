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
const child_process = require(`child_process`);
const argv = require(`command-line-args`)(cliOpts(), { partial: true });
const gulpPath = require.resolve(path.join(`..`, `node_modules/gulp/bin/gulp.js`));

let jsonPaths = [...(argv.json || [])];
let arrowPaths = [...(argv.arrow || [])];

if (!argv.mode) {
    return print_usage();
}

let mode = argv.mode.toUpperCase();
if (mode === 'VALIDATE' && !jsonPaths.length) {
    jsonPaths = glob.sync(path.resolve(__dirname, `../test/data/json/`, `*.json`));
    if (!arrowPaths.length) {
        [jsonPaths, arrowPaths] = jsonPaths.reduce(([jsonPaths, arrowPaths], jsonPath) => {
            const { name } = path.parse(jsonPath);
            for (const source of ['cpp', 'java']) {
                for (const format of ['file', 'stream']) {
                    const arrowPath = path.resolve(__dirname, `../test/data/${source}/${format}/${name}.arrow`);
                    if (fs.existsSync(arrowPath)) {
                        jsonPaths.push(jsonPath);
                        arrowPaths.push(arrowPath);
                    }
                }
            }
            return [jsonPaths, arrowPaths];
        }, [[], []]);
        console.log(`jsonPaths: [\n\t${jsonPaths.join('\n\t')}\n]`);
        console.log(`arrowPaths: [\n\t${arrowPaths.join('\n\t')}\n]`);
    }
} else if (!jsonPaths.length) {
    return print_usage();
}

switch (mode) {
    case 'VALIDATE':
        const args = [`test`, `-i`].concat(argv._unknown || []);
        jsonPaths.forEach((p, i) => {
            args.push('-j', p, '-a', arrowPaths[i]);
        });
        process.exitCode = child_process.spawnSync(
            gulpPath, args,
            {
                cwd: path.resolve(__dirname, '..'),
                stdio: ['ignore', 'inherit', 'inherit']
            }
        ).status || process.exitCode || 0;
        break;
    default:
        print_usage();
}

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
    process.exit(1);
}
