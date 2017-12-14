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

var path = require('path');
var gulp = require.resolve(path.join(`..`, `node_modules/gulp/bin/gulp.js`));
var child_process = require(`child_process`);
var optionList = [
    {
        type: String,
        name: 'mode',
        description: 'The integration test to run'
    },
    {
        type: String,
        name: 'arrow', alias: 'a',
        description: 'The Arrow file to read/write'
    },
    {
        type: String,
        name: 'json', alias: 'j',
        description: 'The JSON file to read/write'
    }
];

var argv = require(`command-line-args`)(optionList, { partial: true });

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
                ...optionList,
                {
                    name: 'help',
                    description: 'Print this usage guide.'
                }
            ]
        },
    ]));
    process.exit(1);
}

if (!argv.arrow || !argv.json || !argv.mode) {
    return print_usage();
}

switch (argv.mode.toUpperCase()) {
    case 'VALIDATE':
        child_process.spawnSync(
            gulp,
            [`test`, `-i`].concat(process.argv.slice(2)),
            {
                cwd: path.resolve(__dirname, '..'),
                stdio: ['ignore', 'inherit', 'inherit']
            }
        );
        break;
    default:
        print_usage();
}
