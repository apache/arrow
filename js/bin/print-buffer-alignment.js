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
const extension = process.env.ARROW_JS_DEBUG === 'src' ? '.ts' : '.cjs';
const { VectorLoader } = require(`../targets/apache-arrow/visitor/vectorloader`);
const { RecordBatch, AsyncMessageReader, makeData, Struct, Schema, Field } = require(`../index${extension}`);

(async () => {

    const readable = process.argv.length < 3 ? process.stdin : fs.createReadStream(path.resolve(process.argv[2]));
    const reader = new AsyncMessageReader(readable);

    let schema, metadataLength, message;
    let byteOffset = 0;
    let recordBatchCount = 0;
    let dictionaryBatchCount = 0;

    while (1) {
        if ((metadataLength = (await reader.readMetadataLength())).done) { break; }
        if (metadataLength.value === -1) {
            if ((metadataLength = (await reader.readMetadataLength())).done) { break; }
        }
        if ((message = (await reader.readMetadata(metadataLength.value))).done) { break; }

        if (message.value.isSchema()) {
            console.log(
                `Schema:`,
                {
                    byteOffset,
                    metadataLength: metadataLength.value,
                });
            schema = message.value.header();
            byteOffset += metadataLength.value;
        } else if (message.value.isRecordBatch()) {
            const header = message.value.header();
            const bufferRegions = header.buffers;
            const body = await reader.readMessageBody(message.value.bodyLength);
            const recordBatch = loadRecordBatch(schema, header, body);
            console.log(
                `RecordBatch ${++recordBatchCount}:`,
                {
                    numRows: recordBatch.numRows,
                    byteOffset,
                    metadataLength: metadataLength.value,
                    bodyByteLength: body.byteLength,
                });
            byteOffset += metadataLength.value;
            bufferRegions.forEach(({ offset, length: byteLength }, i) => {
                console.log(`\tbuffer ${i + 1}:`, { byteOffset: byteOffset + offset, byteLength });
            });
            byteOffset += body.byteLength;
        } else if (message.value.isDictionaryBatch()) {
            const header = message.value.header();
            const bufferRegions = header.data.buffers;
            const type = schema.dictionaries.get(header.id);
            const body = await reader.readMessageBody(message.value.bodyLength);
            const recordBatch = loadDictionaryBatch(header.data, body, type);
            console.log(
                `DictionaryBatch ${++dictionaryBatchCount}:`,
                {
                    id: header.id,
                    numRows: recordBatch.numRows,
                    byteOffset,
                    metadataLength: metadataLength.value,
                    bodyByteLength: body.byteLength,
                });
            byteOffset += metadataLength.value;
            bufferRegions.forEach(({ offset, length: byteLength }, i) => {
                console.log(`\tbuffer ${i + 1}:`, { byteOffset: byteOffset + offset, byteLength });
            });
            byteOffset += body.byteLength;
        }
    }

    await reader.return();

})().catch((e) => { console.error(e); process.exit(1); });

function loadRecordBatch(schema, header, body) {
    const children = new VectorLoader(body, header.nodes, header.buffers, new Map()).visitMany(schema.fields);
    return new RecordBatch(
        schema,
        makeData({
            type: new Struct(schema.fields),
            length: header.length,
            children: children
        })
    );
}

function loadDictionaryBatch(header, body, dictionaryType) {
    const schema = new Schema([new Field('', dictionaryType)]);
    const children = new VectorLoader(body, header.nodes, header.buffers, new Map()).visitMany([dictionaryType]);
    return new RecordBatch(
        schema,
        makeData({
            type: new Struct(schema.fields),
            length: header.length,
            children: children
        })
    );
}
