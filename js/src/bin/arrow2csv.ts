// #! /usr/bin/env node

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

/* tslint:disable */

import * as Arrow from '../Arrow';

(function() {

const fs = require('fs');
const { parse } = require('json-bignum');
const optionList = [
    {
        type: String,
        name: 'schema', alias: 's',
        optional: true, multiple: true,
        typeLabel: '[underline]{columns}',
        description: 'A space-delimited list of column names'
    },
    {
        type: String,
        name: 'file', alias: 'f',
        description: 'The Arrow file to read'
    }
];

const argv = require(`command-line-args`)(optionList, { partial: true });
const files = [argv.file, ...(argv._unknown || [])].filter(Boolean);

if (!files.length) {
    console.log(require('command-line-usage')([
        {
            header: 'arrow2csv',
            content: 'Print a CSV from an Arrow file'
        },
        {
            header: 'Synopsis',
            content: [
                '$ arrow2csv [underline]{file.arrow} [[bold]{--schema} column_name ...]',
                '$ arrow2csv [[bold]{--schema} column_name ...] [[bold]{--file} [underline]{file.arrow}]',
                '$ arrow2csv [bold]{-s} column_1 [bold]{-s} column_2 [[bold]{-f} [underline]{file.arrow}]',
                '$ arrow2csv [[bold]{--help}]'
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
        {
            header: 'Example',
            content: [
                '$ arrow2csv --schema foo baz -f simple.arrow',
                '>  foo,  baz',
                '>    1,   aa',
                '> null, null',
                '>    3, null',
                '>    4,  bbb',
                '>    5, cccc',
            ]
        }
    ]));
    process.exit(1);
}

files.forEach((source) => {
    let table: any, input = fs.readFileSync(source);
    try {
        table = Arrow.Table.from([input]);
    } catch (e) {
        table = Arrow.Table.from(parse(input + ''));
    }
    if (argv.schema && argv.schema.length) {
        table = table.select(...argv.schema);
    }
    printTable(table);
});

function printTable(table: Arrow.Table<any>) {
    let header = [...table.columns.map((_, i) => table.key(i))].map(stringify);
    let maxColumnWidths = header.map(x => x.length);
    // Pass one to convert to strings and count max column widths
    for (let i = -1, n = table.length - 1; ++i < n;) {
        let val,
            row = [i, ...table.get(i)];
        for (let j = -1, k = row.length; ++j < k; ) {
            val = stringify(row[j]);
            maxColumnWidths[j] = Math.max(maxColumnWidths[j], val.length);
        }
    }
    console.log(header.map((x, j) => leftPad(x, ' ', maxColumnWidths[j])).join(' | '));
    // Pass two to pad each one to max column width
    for (let i = -1, n = table.length; ++i < n; ) {
        console.log(
            [...table.get(i)]
                .map(stringify)
                .map((x, j) => leftPad(x, ' ', maxColumnWidths[j]))
                .join(' | ')
        );
    }
}

function leftPad(str: string, fill: string, n: number) {
    return (new Array(n + 1).join(fill) + str).slice(-1 * n);
}

function stringify(x: any) {
    return typeof x === 'string' ? `"${x}"`
              : Array.isArray(x) ? JSON.stringify(x)
              : ArrayBuffer.isView(x) ? `[${x}]`
                                      : `${x}`;
}

})();