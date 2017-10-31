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
import * as File_ from '../format/File_generated';
import * as Schema_ from '../format/Schema_generated';
import * as Message_ from '../format/Message_generated';
import { PADDING, readMessageBatches } from './message';

import ByteBuffer = flatbuffers.ByteBuffer;
import Footer = File_.org.apache.arrow.flatbuf.Footer;
export import Schema = Schema_.org.apache.arrow.flatbuf.Schema;
export import RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;

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

export function* readFile(...bbs: ByteBuffer[]) {
    for (let bb of bbs) {
        let fileLength = bb.capacity();
        let footerLength: number, footerOffset: number;
        if ((fileLength < magicX2AndPadding /*                     Arrow buffer too small */) ||
            (!_checkMagic(bb.bytes(), 0) /*                        Missing magic start    */) ||
            (!_checkMagic(bb.bytes(), fileLength - magicLength) /* Missing magic end      */) ||
            (/*                                                    Invalid footer length  */
            (footerLength = bb.readInt32(footerOffset = fileLength - magicAndPadding)) < 1 &&
            (footerLength + magicX2AndPadding > fileLength))) {
            throw new Error('Invalid file');
        }
        bb.setPosition(footerOffset - footerLength);
        let schema, footer = Footer.getRootAsFooter(bb);
        if (!(schema = footer.schema()!)) {
            return;
        }
        for (let i = -1, n = footer.dictionariesLength(); ++i < n;) {
            let block = footer.dictionaries(i)!;
            bb.setPosition(block.offset().low);
            for (let batch of readMessageBatches(bb)) {
                yield { schema, batch };
                break;
            }
        }
        for (let i = -1, n = footer.recordBatchesLength(); ++i < n;) {
            const block = footer.recordBatches(i)!;
            bb.setPosition(block.offset().low);
            for (let batch of readMessageBatches(bb)) {
                yield { schema, batch };
                break;
            }
        }
    }
}
