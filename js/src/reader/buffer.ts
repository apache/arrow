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
import { VectorLayoutReader } from './vector';
import { TypedArray, TypedArrayConstructor } from '../vector/types';
import { footerFromByteBuffer, messageFromByteBuffer } from '../format/fb';
import { Footer, Schema, RecordBatch, DictionaryBatch, Field, Buffer, FieldNode } from '../format/arrow';
import ByteBuffer = flatbuffers.ByteBuffer;

export function* readBuffers<T extends Uint8Array | NodeBuffer | string>(sources: Iterable<T>) {
    let schema: Schema | null = null;
    let readMessages: ((bb: ByteBuffer) => IterableIterator<RecordBatch | DictionaryBatch>) | null = null;
    for (const source of sources) {
        const bb = toByteBuffer(source);
        if ((!schema && ({ schema, readMessages } = readSchema(bb))) && schema && readMessages) {
            for (const message of readMessages(bb)) {
                yield {
                    schema, message, reader: new BufferVectorLayoutReader(
                        bb,
                        (function* (fieldNodes) { yield* fieldNodes; })(message.fieldNodes),
                        (function* (buffers) { yield* buffers; })(message.buffers)
                    ) as VectorLayoutReader
                };
            }
        }
    }
}

export async function* readBuffersAsync<T extends Uint8Array | NodeBuffer | string>(sources: AsyncIterable<T>) {
    let schema: Schema | null = null;
    let readMessages: ((bb: ByteBuffer) => IterableIterator<RecordBatch | DictionaryBatch>) | null = null;
    for await (const source of sources) {
        const bb = toByteBuffer(source);
        if ((!schema && ({ schema, readMessages } = readSchema(bb))) && schema && readMessages) {
            for (const message of readMessages(bb)) {
                yield {
                    schema, message, reader: new BufferVectorLayoutReader(
                        bb,
                        (function* (fieldNodes) { yield* fieldNodes; })(message.fieldNodes),
                        (function* (buffers) { yield* buffers; })(message.buffers)
                    ) as VectorLayoutReader
                };
            }
        }
    }
}

function toByteBuffer(bytes?: Uint8Array | NodeBuffer | string) {
    let arr: Uint8Array = bytes as any || new Uint8Array(0);
    if (typeof bytes === 'string') {
        arr = new Uint8Array(bytes.length);
        for (let i = -1, n = bytes.length; ++i < n;) {
            arr[i] = bytes.charCodeAt(i);
        }
        return new ByteBuffer(arr);
    }
    return new ByteBuffer(arr);
}

function readSchema(bb: ByteBuffer) {
    let schema: Schema, readMessages, footer: Footer | null;
    if (footer = readFileSchema(bb)) {
        schema = footer.schema!;
        readMessages = readFileMessages(footer);
    } else if (schema = readStreamSchema(bb)!) {
        readMessages = readStreamMessages;
    } else {
        throw new Error('Invalid Arrow buffer');
    }
    return { schema, readMessages };
}

const PADDING = 4;
const MAGIC_STR = 'ARROW1';
const MAGIC = new Uint8Array(MAGIC_STR.length);
for (let i = 0; i < MAGIC_STR.length; i += 1 | 0) {
    MAGIC[i] = MAGIC_STR.charCodeAt(i);
}

function checkForMagicArrowString(buffer: Uint8Array, index = 0) {
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

function readStreamSchema(bb: ByteBuffer) {
    if (!checkForMagicArrowString(bb.bytes(), 0)) {
        for (const message of readMessages(bb)) {
            if (message.isSchema()) {
                return message as Schema;
            }
        }
    }
    return null;
}

function* readStreamMessages(bb: ByteBuffer) {
    for (const message of readMessages(bb)) {
        if (message.isRecordBatch()) {
            yield message;
        } else if (message.isDictionaryBatch()) {
            yield message;
        } else {
            continue;
        }
        // position the buffer after the body to read the next message
        bb.setPosition(bb.position() + message.bodyLength.low);
    }
}

function readFileSchema(bb: ByteBuffer) {
    let fileLength = bb.capacity(), footerLength: number, footerOffset: number;
    if ((fileLength < magicX2AndPadding /*                     Arrow buffer too small */) ||
        (!checkForMagicArrowString(bb.bytes(), 0) /*                        Missing magic start    */) ||
        (!checkForMagicArrowString(bb.bytes(), fileLength - magicLength) /* Missing magic end      */) ||
        (/*                                                    Invalid footer length  */
        (footerLength = bb.readInt32(footerOffset = fileLength - magicAndPadding)) < 1 &&
        (footerLength + magicX2AndPadding > fileLength))) {
        return null;
    }
    bb.setPosition(footerOffset - footerLength);
    return footerFromByteBuffer(bb);
}

function readFileMessages(footer: Footer) {
    return function* (bb: ByteBuffer) {
        for (let i = -1, batches = footer.dictionaryBatches, n = batches.length; ++i < n;) {
            bb.setPosition(batches[i].offset.low);
            yield readMessage(bb, bb.readInt32(bb.position())) as DictionaryBatch;
        }
        for (let i = -1, batches = footer.recordBatches, n = batches.length; ++i < n;) {
            bb.setPosition(batches[i].offset.low);
            yield readMessage(bb, bb.readInt32(bb.position())) as RecordBatch;
        }
    };
}

function* readMessages(bb: ByteBuffer) {
    let length: number, message: Schema | RecordBatch | DictionaryBatch;
    while (bb.position() < bb.capacity() &&
          (length = bb.readInt32(bb.position())) > 0) {
        if (message = readMessage(bb, length)!) {
            yield message;
        }
    }
}

function readMessage(bb: ByteBuffer, length: number) {
    bb.setPosition(bb.position() + PADDING);
    const message = messageFromByteBuffer(bb);
    bb.setPosition(bb.position() + length);
    return message;
}

class BufferVectorLayoutReader implements VectorLayoutReader {
    private offset: number;
    private bytes: Uint8Array;
    constructor(bb: ByteBuffer, private fieldNodes: Iterator<FieldNode>, private buffers: Iterator<Buffer>) {
        this.bytes = bb.bytes();
        this.offset = bb.position();
    }
    readContainerLayout(field: Field) {
        const { bytes, offset, buffers } = this, fieldNode = this.fieldNodes.next().value;
        return {
            field, fieldNode,
            validity: createValidityArray(bytes, field, fieldNode, offset, buffers.next().value)
        };
    }
    readFixedWidthLayout<T extends TypedArray>(field: Field, dataType: TypedArrayConstructor<T>) {
        const { bytes, offset, buffers } = this, fieldNode = this.fieldNodes.next().value;
        return {
            field, fieldNode,
            validity: createValidityArray(bytes, field, fieldNode, offset, buffers.next().value),
            data: createTypedArray(bytes, field, fieldNode, offset, buffers.next().value, dataType)
        };
    }
    readBinaryLayout(field: Field) {
        const { bytes, offset, buffers } = this, fieldNode = this.fieldNodes.next().value;
        return {
            field, fieldNode,
            validity: createValidityArray(bytes, field, fieldNode, offset, buffers.next().value),
            offsets: createTypedArray(bytes, field, fieldNode, offset, buffers.next().value, Int32Array),
            data: createTypedArray(bytes, field, fieldNode, offset, buffers.next().value, Uint8Array)
        };
    }
    readVariableWidthLayout(field: Field) {
        const { bytes, offset, buffers } = this, fieldNode = this.fieldNodes.next().value;
        return {
            field, fieldNode,
            validity: createValidityArray(bytes, field, fieldNode, offset, buffers.next().value),
            offsets: createTypedArray(bytes, field, fieldNode, offset, buffers.next().value, Int32Array)
        };
    }
}

function createValidityArray(bytes: Uint8Array, field: Field, fieldNode: FieldNode, offset: number, buffer: Buffer) {
    return field.nullable && fieldNode.nullCount.low > 0 &&
        createTypedArray(bytes, field, fieldNode, offset, buffer, Uint8Array) || null;
}

function createTypedArray<T extends TypedArray>(bytes: Uint8Array, _field: Field, _fieldNode: FieldNode, offset: number, buffer: Buffer, ArrayConstructor: TypedArrayConstructor<T>): T {
    return new ArrayConstructor(
        bytes.buffer,
        bytes.byteOffset + offset + buffer.offset.low,
        buffer.length.low / ArrayConstructor.BYTES_PER_ELEMENT
    );
}
