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

/* eslint-disable unicorn/no-array-for-each */

import * as fs from 'node:fs';
import * as stream from 'node:stream';
import { Schema, RecordBatch, RecordBatchReader, AsyncByteQueue, util } from '../Arrow.js';

import * as commandLineUsage from 'command-line-usage';
import * as commandLineArgs from 'command-line-args';
// @ts-ignore
import { parse as bignumJSONParse } from 'json-bignum';

const argv = commandLineArgs(cliOpts(), { partial: true });
const files = argv.help ? [] : [...(argv.file || []), ...(argv._unknown || [])].filter(Boolean);

const state = { ...argv, closed: false, maxColWidths: [10] };

type ToStringState = {
    hr?: string;
    sep?: string;
    schema?: any;
    closed?: boolean;
    metadata?: boolean;
    maxColWidths: number[];
};

(async () => {

    const sources = argv.help ? [] : [
        ...files.map((file) => () => fs.createReadStream(file)),
        ...(process.stdin.isTTY ? [] : [() => process.stdin])
    ].filter(Boolean) as (() => NodeJS.ReadableStream)[];

    let reader: RecordBatchReader | null;
    let hasReaders = false;

    for (const source of sources) {
        if (state.closed) { break; }
        for await (reader of recordBatchReaders(source)) {
            hasReaders = true;
            const batches = stream.Readable.from(reader);
            const toString = batchesToString(state, reader.schema);
            await pipeTo(
                batches.pipe(toString),
                process.stdout, { end: false }
            ).catch(() => state.closed = true); // Handle EPIPE errors
        }
        if (state.closed) { break; }
    }

    return hasReaders ? 0 : print_usage();
})()
    .then((x) => +x || 0, (err) => {
        if (err) {
            console.error(`${err?.stack || err}`);
        }
        return process.exitCode || 1;
    }).then((code) => process.exit(code));

function pipeTo(source: NodeJS.ReadableStream, sink: NodeJS.WritableStream, opts?: { end: boolean }) {
    return new Promise((resolve, reject) => {

        source.on('end', onEnd).pipe(sink, opts).on('error', onErr);

        function onEnd() { done(undefined, resolve); }
        function onErr(err: any) { done(err, reject); }
        function done(e: any, cb: (e?: any) => void) {
            source.removeListener('end', onEnd);
            sink.removeListener('error', onErr);
            cb(e);
        }
    });
}

async function* recordBatchReaders(createSourceStream: () => NodeJS.ReadableStream): AsyncGenerator<RecordBatchReader, void, void> {

    const json = new AsyncByteQueue();
    const stream = new AsyncByteQueue();
    const source = createSourceStream();
    let reader: RecordBatchReader | null = null;
    let readers: AsyncIterable<RecordBatchReader> | null = null;
    // tee the input source, just in case it's JSON
    source.on('end', () => [stream, json].forEach((y) => y.close()))
        .on('data', (x) => [stream, json].forEach((y) => y.write(x)))
        .on('error', (e) => [stream, json].forEach((y) => y.abort(e)));

    try {
        for await (reader of RecordBatchReader.readAll(stream)) {
            reader && (yield reader);
        }
        if (reader) return;
    } catch { readers = null; }

    if (!readers) {
        await json.closed;
        if (source instanceof fs.ReadStream) { source.close(); }
        // If the data in the `json` ByteQueue parses to JSON, then assume it's Arrow JSON from a file or stdin
        try {
            for await (reader of RecordBatchReader.readAll(bignumJSONParse(await json.toString()))) {
                reader && (yield reader);
            }
        } catch { readers = null; }
    }
}

function batchesToString(state: ToStringState, schema: Schema) {

    let rowId = 0;
    let batchId = -1;
    let maxColWidths = [10];
    const { hr, sep, metadata } = state;

    const header = ['row_id', ...schema.fields.map((f) => `${f}`)].map(val => util.valueToString(val));

    state.maxColWidths = header.map((x, i) => Math.max(maxColWidths[i] || 0, x.length));

    return new stream.Transform({
        encoding: 'utf8',
        writableObjectMode: true,
        readableObjectMode: false,
        final(cb: (error?: Error | null) => void) {
            // if there were no batches, then print the Schema, and metadata
            if (batchId === -1) {
                hr && this.push(`${horizontalRule(state.maxColWidths, hr, sep)}\n\n`);
                this.push(`${formatRow(header, maxColWidths, sep)}\n`);
                if (metadata && schema.metadata.size > 0) {
                    this.push(`metadata:\n${formatMetadata(schema.metadata)}\n`);
                }
            }
            hr && this.push(`${horizontalRule(state.maxColWidths, hr, sep)}\n\n`);
            cb();
        },
        transform(batch: RecordBatch, _enc: string, cb: (error?: Error, data?: any) => void) {

            batch = !state.schema?.length ? batch : batch.select(state.schema);

            if (state.closed) { return cb(undefined, null); }

            // Pass one to convert to strings and count max column widths
            state.maxColWidths = measureColumnWidths(rowId, batch, header.map((x, i) => Math.max(maxColWidths[i] || 0, x.length)));

            // If this is the first batch in a stream, print a top horizontal rule, schema metadata, and
            if (++batchId === 0) {
                hr && this.push(`${horizontalRule(state.maxColWidths, hr, sep)}\n`);
                if (metadata && batch.schema.metadata.size > 0) {
                    this.push(`metadata:\n${formatMetadata(batch.schema.metadata)}\n`);
                    hr && this.push(`${horizontalRule(state.maxColWidths, hr, sep)}\n`);
                }
                if (batch.numRows <= 0 || batch.numCols <= 0) {
                    this.push(`${formatRow(header, maxColWidths = state.maxColWidths, sep)}\n`);
                }
            }

            if (batch.numRows > 0 && batch.numCols > 0) {
                // If any of the column widths changed, print the header again
                if (rowId % 350 !== 0 && JSON.stringify(state.maxColWidths) !== JSON.stringify(maxColWidths)) {
                    this.push(`${formatRow(header, state.maxColWidths, sep)}\n`);
                }
                maxColWidths = state.maxColWidths;
                for (const row of batch) {
                    if (state.closed) { break; } else if (!row) { continue; }
                    if (rowId % 350 === 0) {
                        this.push(`${formatRow(header, maxColWidths, sep)}\n`);
                    }
                    this.push(`${formatRow([rowId++, ...row.toArray()].map(v => util.valueToString(v)), maxColWidths, sep)}\n`);
                }
            }
            cb();
        }
    });
}

function horizontalRule(maxColWidths: number[], hr = '', sep = ' | ') {
    return ` ${''.padStart(maxColWidths.reduce((x, y) => x + y, -2 + maxColWidths.length * sep.length), hr)}`;
}

function formatRow(row: string[] = [], maxColWidths: number[] = [], sep = ' | ') {
    return row.map((x, j) => x.padStart(maxColWidths[j])).join(sep);
}

function formatMetadataValue(value = '') {
    let parsed = value;
    try {
        parsed = JSON.stringify(JSON.parse(value), null, 2);
    } catch { parsed = value; }
    return util.valueToString(parsed).split('\n').join('\n  ');
}

function formatMetadata(metadata: Map<string, string>) {

    return [...metadata].map(([key, val]) =>
        `  ${key}: ${formatMetadataValue(val)}`
    ).join(',  \n');
}

function measureColumnWidths(rowId: number, batch: RecordBatch, maxColWidths: number[] = []) {
    let val: any, j = 0;
    for (const row of batch) {
        if (!row) { continue; }
        maxColWidths[j = 0] = Math.max(maxColWidths[0] || 0, (`${rowId++}`).length);
        for (val of row) {
            if (val && typedArrayElementWidths.has(val.constructor) && (typeof val[Symbol.toPrimitive] !== 'function')) {
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
                maxColWidths[j + 1] = Math.max(maxColWidths[j + 1] || 0, util.valueToString(val).length);
            }
            ++j;
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
    ]);
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
            name: 'sep', optional: true, default: ' | ',
            description: 'The column separator character (default: " | ")'
        },
        {
            type: String,
            name: 'hr', optional: true, default: '',
            description: 'The horizontal border character (default: "")'
        },
        {
            type: Boolean,
            name: 'metadata', alias: 'm',
            optional: true, default: false,
            description: 'Flag to print Schema metadata (default: false)'
        },
        {
            type: Boolean,
            name: 'help', optional: true, default: false,
            description: 'Print this usage guide.'
        }
    ];
}

function print_usage() {
    console.log(commandLineUsage([
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
                '$ arrow2csv --schema foo baz --sep " , " -f simple.arrow',
                '>   "row_id", "foo: Int32", "baz: Utf8"',
                '>          0,            1,        "aa"',
                '>          1,         null,        null',
                '>          2,            3,        null',
                '>          3,            4,       "bbb"',
                '>          4,            5,      "cccc"',
            ]
        }
    ]));
    return 1;
}
