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
import * as File_ from '../format/File';
import * as Schema_ from '../format/Schema';
import * as Message_ from '../format/Message';
import ByteBuffer = flatbuffers.ByteBuffer;
import Footer = File_.org.apache.arrow.flatbuf.Footer;
import Schema = Schema_.org.apache.arrow.flatbuf.Schema;
import Message = Message_.org.apache.arrow.flatbuf.Message;
import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;

const PADDING = 4;
const MAGIC_STR = 'ARROW1';
const MAGIC = new Uint8Array(MAGIC_STR.length);
for (let i = 0; i < MAGIC_STR.length; i += 1 | 0) {
    MAGIC[i] = MAGIC_STR.charCodeAt(i);
}

export function _checkMagic(buffer: Uint8Array, index = 0) {
    for (let i = -1, n = MAGIC.length; ++i < n;) {
        if (MAGIC[i] !== buffer[index + i]) {
            return false;
        }
    }
    return true;
}

const magicLength = MAGIC.length;
const magicAndPadding = magicLength + PADDING;
const magicX2AndPadding = magicLength * 2 + PADDING;

export function readStreamSchema(bb: ByteBuffer) {
    if (!_checkMagic(bb.bytes(), 0)) {
        for (const message of readMessages(bb)) {
            if (message.headerType() === MessageHeader.Schema) {
                return message.header(new Schema());
            }
        }
    }
    return null;
}

export function readFileFooter(bb: ByteBuffer) {
    let fileLength = bb.capacity();
    let footerLength: number, footerOffset: number;
    if ((fileLength < magicX2AndPadding /*                     Arrow buffer too small */) ||
        (!_checkMagic(bb.bytes(), 0) /*                        Missing magic start    */) ||
        (!_checkMagic(bb.bytes(), fileLength - magicLength) /* Missing magic end      */) ||
        (/*                                                    Invalid footer length  */
        (footerLength = bb.readInt32(footerOffset = fileLength - magicAndPadding)) < 1 &&
        (footerLength + magicX2AndPadding > fileLength))) {
        return null;
    }
    bb.setPosition(footerOffset - footerLength);
    return Footer.getRootAsFooter(bb);
}

export function* readFileMessages(bb: ByteBuffer, footer: Footer) {
    for (let i = -1, n = footer.dictionariesLength(); ++i < n;) {
        bb.setPosition(footer.dictionaries(i)!.offset().low);
        yield readMessage(bb, bb.readInt32(bb.position()));
    }
    for (let i = -1, n = footer.recordBatchesLength(); ++i < n;) {
        bb.setPosition(footer.recordBatches(i)!.offset().low);
        yield readMessage(bb, bb.readInt32(bb.position()));
    }
}

export function readMessage(bb: ByteBuffer, length: number) {
    bb.setPosition(bb.position() + PADDING);
    const message = Message.getRootAsMessage(bb);
    bb.setPosition(bb.position() + length);
    return message;
}

export function* readMessages(bb: ByteBuffer) {
    let length;
    while (bb.position() < bb.capacity() &&
          (length = bb.readInt32(bb.position())) > 0) {
        yield readMessage(bb, length);
    }
}

export function* readStreamMessages(bb: ByteBuffer) {
    for (const message of readMessages(bb)) {
        switch (message.headerType()) {
            case MessageHeader.RecordBatch:
            case MessageHeader.DictionaryBatch:
                yield message;
                break;
            default: continue;
        }
        // position the buffer after the body to read the next message
        bb.setPosition(bb.position() + message.bodyLength().low);
    }
}
