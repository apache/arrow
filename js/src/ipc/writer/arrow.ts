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

import { Table } from '../../table';
import { serializeStream, serializeFile } from './binary';

export function writeTableBinary(table: Table, stream = true) {
    return concatBuffers(stream ? serializeStream(table) : serializeFile(table));
}

function concatBuffers(messages: Iterable<Uint8Array | Buffer>) {

    let buffers = [], byteLength = 0;

    for (const message of messages) {
        buffers.push(message);
        byteLength += message.byteLength;
    }

    const { buffer } = buffers.reduce(({ buffer, byteOffset }, bytes) => {
        buffer.set(bytes, byteOffset);
        return { buffer, byteOffset: byteOffset + bytes.byteLength };
    }, { buffer: new Uint8Array(byteLength), byteOffset: 0 });

    return buffer;
}
