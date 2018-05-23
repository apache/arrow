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
import ByteBuffer = flatbuffers.ByteBuffer;

export const PADDING = 4;
export const MAGIC_STR = 'ARROW1';
export const MAGIC = new Uint8Array(MAGIC_STR.length);

for (let i = 0; i < MAGIC_STR.length; i += 1 | 0) {
    MAGIC[i] = MAGIC_STR.charCodeAt(i);
}

export function checkForMagicArrowString(buffer: Uint8Array, index = 0) {
    for (let i = -1, n = MAGIC.length; ++i < n;) {
        if (MAGIC[i] !== buffer[index + i]) {
            return false;
        }
    }
    return true;
}

export function isValidArrowFile(bb: ByteBuffer) {
    let fileLength = bb.capacity(), footerLength: number, lengthOffset: number;
    if ((fileLength < magicX2AndPadding /*                     Arrow buffer too small */) ||
        (!checkForMagicArrowString(bb.bytes(), 0) /*                        Missing magic start    */) ||
        (!checkForMagicArrowString(bb.bytes(), fileLength - magicLength) /* Missing magic end      */) ||
        (/*                                                    Invalid footer length  */
        (footerLength = bb.readInt32(lengthOffset = fileLength - magicAndPadding)) < 1 &&
        (footerLength + lengthOffset > fileLength))) {
        return false;
    }
    return true;
}

export const magicLength = MAGIC.length;
export const magicAndPadding = magicLength + PADDING;
export const magicX2AndPadding = magicLength * 2 + PADDING;
