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
import * as Message_ from '../format/Message_generated';
import ByteBuffer = flatbuffers.ByteBuffer;
import Message = Message_.org.apache.arrow.flatbuf.Message;
import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;
import RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;
import DictionaryBatch = Message_.org.apache.arrow.flatbuf.DictionaryBatch;

export const PADDING = 4;
export type MessageBatch = {
    id?: string;
    offset: number;
    bytes: Uint8Array;
    data: RecordBatch;
};

export function* readMessages(bb: ByteBuffer) {
    let message, length;
    while (bb.position() < bb.capacity() &&
          (length = bb.readInt32(bb.position())) > 0) {
        bb.setPosition(bb.position() + PADDING);
        message = Message.getRootAsMessage(bb);
        bb.setPosition(bb.position() + length);
        yield message;
    }
}

export function* readMessageBatches(bb: ByteBuffer) {
    let bytes = bb.bytes();
    for (let message of readMessages(bb)) {
        let type = message.headerType();
        let id: string, data: RecordBatch;
        if (type === MessageHeader.RecordBatch) {
            data = message.header(new RecordBatch());
        } else if (type === MessageHeader.DictionaryBatch) {
            let header = message.header(new DictionaryBatch());
            id = header.id().toFloat64().toString();
            data = header.data();
        } else {
            continue;
        }
        yield <MessageBatch> { id, data, bytes, offset: bytes.byteOffset + bb.position() };
        // position the buffer after the body to read the next message
        bb.setPosition(bb.position() + message.bodyLength().low);
    }
}
