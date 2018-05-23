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

import { flatbuffers } from 'flatbuffers';
import * as Message_ from '../../fb/Message';
import ByteBuffer = flatbuffers.ByteBuffer;
import _Message = Message_.org.apache.arrow.flatbuf.Message;
import { PADDING, isValidArrowFile, checkForMagicArrowString } from '../magic';

export async function* fromReadableStream(stream: NodeJS.ReadableStream) {

    let bb: ByteBuffer;
    let bytesRead = 0, bytes = new Uint8Array(0);
    let messageLength = 0, message: _Message | null = null;

    for await (let chunk of (stream as any as AsyncIterable<Uint8Array | Buffer | string>)) {

        const grown = new Uint8Array(bytes.byteLength + chunk.length);

        if (typeof chunk !== 'string') {
            grown.set(bytes, 0) || grown.set(chunk, bytes.byteLength);
        } else {
            for (let i = -1, j = bytes.byteLength, n = chunk.length; ++i < n;) {
                grown[i + j] = chunk.charCodeAt(i);
            }
        }

        bytes = grown;

        // If we're reading in an Arrow File, just concatenate the bytes until
        // the file is fully read in
        if (checkForMagicArrowString(bytes)) {
            if (!isValidArrowFile(new ByteBuffer(bytes))) {
                continue;
            }
            return yield bytes;
        }

        if (messageLength <= 0) {
            messageLength = new DataView(bytes.buffer).getInt32(0, true);
        }

        while (messageLength < bytes.byteLength) {
            if (!message) {
                (bb = new ByteBuffer(bytes)).setPosition(4);
                if (message = _Message.getRootAsMessage(bb)) {
                    messageLength += message.bodyLength().low;
                    continue;
                }
                throw new Error(`Invalid message at position ${bytesRead}`);
            }
            bytesRead += messageLength + PADDING;
            yield bytes.subarray(0, messageLength + PADDING);
            bytes = bytes.subarray(messageLength + PADDING);
            messageLength = bytes.byteLength <= 0 ? 0 :
                new DataView(bytes.buffer).getInt32(bytes.byteOffset, true);
            message = null;
        }
    }
}
