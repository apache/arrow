#! /usr/bin/env -S node --no-warnings --loader ts-node/esm/transpile-only

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

import * as fs from 'node:fs';
import * as path from 'node:path';
import { finished as eos } from 'node:stream/promises';
import { RecordBatchReader, RecordBatchFileWriter } from '../index.ts';

(async () => {

    const readable = process.argv.length < 3 ? process.stdin : fs.createReadStream(path.resolve(process.argv[2]));
    const writable = process.argv.length < 4 ? process.stdout : fs.createWriteStream(path.resolve(process.argv[3]));

    const streamToFile = readable
        .pipe(RecordBatchReader.throughNode())
        .pipe(RecordBatchFileWriter.throughNode())
        .pipe(writable);

    await eos(streamToFile);

})().catch((e) => { console.error(e); process.exit(1); });
