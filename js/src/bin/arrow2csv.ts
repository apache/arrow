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

/* tslint:disable */

import * as fs from 'fs';
import * as stream from 'stream';
import { valueToString } from '../util/pretty';
import { RecordBatch, RecordBatchReader, AsyncByteQueue } from '../Arrow.node';

const padLeft = require('pad-left');
const bignumJSONParse = require('json-bignum').parse;
const pipeline = require('util').promisify(stream.pipeline);
const argv = require(`command-line-args`)(cliOpts(), { partial: true });
const files = argv.help ? [] : [...(argv.file || []), ...(argv._unknown || [])].filter(Boolean);

const state = { ...argv, closed: false, hasRecords: false };

(async () => {

    const sources = argv.help ? [] : [
        ...files.map((file) => () => fs.createReadStream(file)),
        ...(process.stdin.isTTY ? [] : [() => process.stdin])
    ].filter(Boolean) as (() => NodeJS.ReadableStream)[];

    let reader: RecordBatchReader | null;

    for (const source of sources) {
        if (state.closed) { break; }
        if (reader = await createRecordBatchReader(source)) {
            await pipeline(
                reader.toNodeStream(),
                recordBatchRowsToString(state),
                process.stdout
            ).catch(() => state.closed = true);
        }
        if (state.closed) { break; }
    }

    return state.hasRecords ? 0 : print_usage();
})()
.then((x) => +x || 0, (err) => {
    if (err) {
        console.error(`${err && err.stack || err}`);
    }
    return process.exitCode || 1;
}).then((code) => process.exit(code));

async function createRecordBatchReader(createSourceStream: () => NodeJS.ReadableStream) {

    let json = new AsyncByteQueue();
    let stream = new AsyncByteQueue();
    let source = createSourceStream();
    let reader: RecordBatchReader | null = null;
    // tee the input source, just in case it's JSON
    source.on('end', () => [stream, json].forEach((y) => y.close()))
        .on('data', (x) => [stream, json].forEach((y) => y.write(x)))
       .on('error', (e) => [stream, json].forEach((y) => y.abort(e)));

    try {
        reader = await (await RecordBatchReader.from(stream)).open();
    } catch (e) { reader = null; }

    if (!reader || reader.closed) {
        reader = null;
        await json.closed;
        if (source instanceof fs.ReadStream) { source.close(); }
        // If the data in the `json` ByteQueue parses to JSON, then assume it's Arrow JSON from a file or stdin
        try {
            reader = await (await RecordBatchReader.from(bignumJSONParse(await json.toString()))).open();
        } catch (e) { reader = null; }
    }

    return (reader && !reader.closed) ? reader : null;
}

function recordBatchRowsToString(state: { closed: boolean, schema: any, separator: string, hasRecords: boolean }) {

    let rowId = 0, maxColWidths = [15], separator = `${state.separator || ' |'} `;

    return new stream.Transform({ transform, encoding: 'utf8', writableObjectMode: true, readableObjectMode: false });

    function transform(this: stream.Transform, batch: RecordBatch, _enc: string, cb: (error?: Error, data?: any) => void) {
        batch = !(state.schema && state.schema.length) ? batch : batch.select(...state.schema);
        if (batch.length <= 0 || batch.numCols <= 0 || state.closed) {
            state.hasRecords || (state.hasRecords = false);
            return cb(undefined, null);
        }

        state.hasRecords = true;
        const header = ['row_id', ...batch.schema.fields.map((f) => `${f}`)].map(valueToString);

        // Pass one to convert to strings and count max column widths
        const newMaxWidths = measureColumnWidths(rowId, batch, header.map((x, i) => Math.max(maxColWidths[i] || 0, x.length)));

        // If any of the column widths changed, print the header again
        if ((rowId % 350) && JSON.stringify(newMaxWidths) !== JSON.stringify(maxColWidths)) {
            this.push(`\n${formatRow(header, newMaxWidths, separator)}`);
        }

        maxColWidths = newMaxWidths;

        for (const row of batch) {
            if (state.closed) { break; }
            else if (!row) { continue; }
            if (!(rowId % 350)) { this.push(`\n${formatRow(header, maxColWidths, separator)}`); }
            this.push(formatRow([rowId++, ...row].map(valueToString), maxColWidths, separator));
        }
        cb();
    }
}

function formatRow(row: string[] = [], maxColWidths: number[] = [], separator: string = ' |') {
    return row.map((x, j) => padLeft(x, maxColWidths[j])).join(separator) + '\n';
}

function measureColumnWidths(rowId: number, batch: RecordBatch, maxColWidths: number[] = []) {
    for (const row of batch) {
        if (!row) { continue; }
        maxColWidths[0] = Math.max(maxColWidths[0] || 0, (`${rowId++}`).length);
        for (let val: any, j = -1, k = row.length; ++j < k;) {
            if (ArrayBuffer.isView(val = row[j]) && (typeof val[Symbol.toPrimitive] !== 'function')) {
                // If we're printing a column of TypedArrays, ensure the column is wide enough to accommodate
                // the widest possible element for a given byte size, since JS omits leading zeroes. For example:
                // 1 |  [1137743649,2170567488,244696391,2122556476]
                // 2 |                                          null
                // 3 |   [637174007,2142281880,961736230,2912449282]
                // 4 |    [1035112265,21832886,412842672,2207710517]
                // 5 |                                          null
                // 6 |                                          null
                // 7 |     [2755142991,4192423256,2994359,467878370]
                const elementWidth = typedArrayElementWidths.get(val.constructor)!;

                maxColWidths[j + 1] = Math.max(maxColWidths[j + 1] || 0,
                    2 + // brackets on each end
                    (val.length - 1) + // commas between elements
                    (val.length * elementWidth) // width of stringified 2^N-1
                );
            } else {
                maxColWidths[j + 1] = Math.max(maxColWidths[j + 1] || 0, valueToString(val).length);
            }
        }
    }
    return maxColWidths;
}

// Measure the stringified representation of 2^N-1 for each TypedArray variant
const typedArrayElementWidths = (() => {
    const maxElementWidth = (ArrayType: any) => {
        const octets = Array.from({ length: ArrayType.BYTES_PER_ELEMENT - 1 }, _ => 255);
        return `${new ArrayType(new Uint8Array([...octets, 254]).buffer)[0]}`.length;
    };
    return new Map<any, number>([
        [Int8Array, maxElementWidth(Int8Array)],
        [Int16Array, maxElementWidth(Int16Array)],
        [Int32Array, maxElementWidth(Int32Array)],
        [Uint8Array, maxElementWidth(Uint8Array)],
        [Uint16Array, maxElementWidth(Uint16Array)],
        [Uint32Array, maxElementWidth(Uint32Array)],
        [Float32Array, maxElementWidth(Float32Array)],
        [Float64Array, maxElementWidth(Float64Array)],
        [Uint8ClampedArray, maxElementWidth(Uint8ClampedArray)]
    ])
})();

function cliOpts() {
    return [
        {
            type: String,
            name: 'schema', alias: 's',
            optional: true, multiple: true,
            typeLabel: '{underline columns}',
            description: 'A space-delimited list of column names'
        },
        {
            type: String,
            name: 'file', alias: 'f',
            optional: true, multiple: true,
            description: 'The Arrow file to read'
        },
        {
            type: String,
            name: 'sep', optional: true, default: '|',
            description: 'The column separator character'
        },
        {
            type: Boolean,
            name: 'help', optional: true, default: false,
            description: 'Print this usage guide.'
        }
    ];    
}

function print_usage() {
    console.log(require('command-line-usage')([
        {
            header: 'arrow2csv',
            content: 'Print a CSV from an Arrow file'
        },
        {
            header: 'Synopsis',
            content: [
                '$ arrow2csv {underline file.arrow} [{bold --schema} column_name ...]',
                '$ arrow2csv [{bold --schema} column_name ...] [{bold --file} {underline file.arrow}]',
                '$ arrow2csv {bold -s} column_1 {bold -s} column_2 [{bold -f} {underline file.arrow}]',
                '$ arrow2csv [{bold --help}]'
            ]
        },
        {
            header: 'Options',
            optionList: cliOpts()
        },
        {
            header: 'Example',
            content: [
                '$ arrow2csv --schema foo baz -f simple.arrow --sep ","',
                '                                                      ',
                '> "row_id", "foo: Int32", "bar: Float64", "baz: Utf8"',
                '>        0,            1,              1,        "aa"',
                '>        1,         null,           null,        null',
                '>        2,            3,           null,        null',
                '>        3,            4,              4,       "bbb"',
                '>        4,            5,              5,      "cccc"',
            ]
        }
    ]));
    return 1;
}
