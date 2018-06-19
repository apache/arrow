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
const path = require('path');

const ext = process.env.ARROW_JS_DEBUG === 'src' ? '.ts' : '';
const base = process.env.ARROW_JS_DEBUG === 'src' ? '../src' : '../targets/apache-arrow';
const { Message } = require(`${base}/ipc/metadata${ext}`);
const { readBuffersAsync } = require(`${base}/ipc/reader/binary${ext}`);
const { Table, VectorVisitor, fromReadableStream } = require(`../index${ext}`);

(async () => {
    const in_ = process.argv.length < 3
        ? process.stdin : fs.createReadStream(path.resolve(process.argv[2]));
    
    let recordBatchIndex = 0;
    let dictionaryBatchIndex = 0;

    for await (let { message, loader } of readBuffersAsync(fromReadableStream(in_))) {

        if (Message.isRecordBatch(message)) {
            console.log(`record batch ${++recordBatchIndex}, offset ${loader.messageOffset}`);
        } else if (Message.isDictionaryBatch(message)) {
            message = message.data;
            console.log(`dictionary batch ${++dictionaryBatchIndex}, offset ${loader.messageOffset}`);
        } else { continue; }
        
        message.buffers.forEach(({offset, length}, i) => {
            console.log(`\tbuffer ${i+1}: { offset: ${offset},  length: ${length} }`);
        });
    }

})().catch((e) => { console.error(e); process.exit(1); });
