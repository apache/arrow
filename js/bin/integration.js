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

var fs = require('fs');
var arrow = require('../targets/es5/cjs/Arrow.js')
var readJSON = arrow.readJSON;
var Table = arrow.Table;
var optionList = [
    {
        type: String,
        name: 'mode', alias: 'm',
        description: 'The Arrow file to read/write'
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
                '$ integration.js -j file.json -a file.arrow -m validate'
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

if (!argv.arrow || !argv.json || !argv.mode) print_usage();

switch (argv.mode) {
    case 'VALIDATE':
        var json_table = Table.fromJSON(fs.readFileSync(argv.json));
        var arrow_table = Table.from([fs.readFileSync(argv.arrow)]);
        var string_equals = (json_table.toString() == arrow_table.toString());

        //console.log(json_table.toString());
        //console.log(arrow_table.toString());

        if (string_equals) console.log("JSON file and Arrow buffer are equivalent")
        else               console.log("JSON file and Arrow buffer don't match")
        process.exit(!string_equals);
    default:
        print_usage();
}
