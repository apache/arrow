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
const { AsyncMessageReader } = require(`../index${extension}`);

(async () => {

    const readable = process.argv.length < 3 ? process.stdin : fs.createReadStream(path.resolve(process.argv[2]));
    const reader = new AsyncMessageReader(readable);

    let recordBatchIndex = 0, dictionaryBatchIndex = 0;

    for await (let message of reader) {

        let bufferRegions = [];

        if (message.isSchema()) {
            continue;
        } else if (message.isRecordBatch()) {
            bufferRegions = message.header().buffers;
            const body = await reader.readMessageBody(message.bodyLength);
            console.log(`record batch ${++recordBatchIndex}, byteOffset ${body.byteOffset}`);
        } else if (message.isDictionaryBatch()) {
            bufferRegions = message.header().data.buffers;
            const body = await reader.readMessageBody(message.bodyLength);
            console.log(`dictionary batch ${++dictionaryBatchIndex}, byteOffset ${body.byteOffset}`);
        }

        bufferRegions.forEach(({ offset, length }, i) => {
            console.log(`\tbuffer ${i + 1}: { offset: ${offset},  length: ${length} }`);
        });
    }

    await reader.return();

})().catch((e) => { console.error(e); process.exit(1); });
