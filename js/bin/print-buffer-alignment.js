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
const path = require('path');
const extension = process.env.ARROW_JS_DEBUG === 'src' ? '.ts' : '';
const { RecordBatch, AsyncMessageReader } = require(`../index${extension}`);
const { VectorLoader } = require(`../targets/apache-arrow/visitor/vectorloader`);

(async () => {

    const readable = process.argv.length < 3 ? process.stdin : fs.createReadStream(path.resolve(process.argv[2]));
    const reader = new AsyncMessageReader(readable);

    let schema, recordBatchIndex = 0, dictionaryBatchIndex = 0;

    for await (let message of reader) {

        let bufferRegions = [];

        if (message.isSchema()) {
            schema = message.header();
            continue;
        } else if (message.isRecordBatch()) {
            const header = message.header();
            bufferRegions = header.buffers;
            const body = await reader.readMessageBody(message.bodyLength);
            const recordBatch = loadRecordBatch(schema, header, body);
            console.log(`record batch ${++recordBatchIndex}: ${JSON.stringify({
                offset: body.byteOffset,
                length: body.byteLength,
                numRows: recordBatch.length,
            })}`);
        } else if (message.isDictionaryBatch()) {
            const header = message.header();
            bufferRegions = header.data.buffers;
            const type = schema.dictionaries.get(header.id);
            const body = await reader.readMessageBody(message.bodyLength);
            const recordBatch = loadDictionaryBatch(header.data, body, type);
            console.log(`dictionary batch ${++dictionaryBatchIndex}: ${JSON.stringify({
                offset: body.byteOffset,
                length: body.byteLength,
                numRows: recordBatch.length,
                dictionaryId: header.id,
            })}`);
        }

        bufferRegions.forEach(({ offset, length }, i) => {
            console.log(`\tbuffer ${i + 1}: { offset: ${offset},  length: ${length} }`);
        });
    }

    await reader.return();

})().catch((e) => { console.error(e); process.exit(1); });

function loadRecordBatch(schema, header, body) {
    return new RecordBatch(schema, header.length, new VectorLoader(body, header.nodes, header.buffers, new Map()).visitMany(schema.fields));
}

function loadDictionaryBatch(header, body, dictionaryType) {
    return RecordBatch.new(new VectorLoader(body, header.nodes, header.buffers, new Map()).visitMany([dictionaryType]));
}
