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

import { Vector } from '../vector/vector';
import { flatbuffers } from 'flatbuffers';
import { readVector, readValueVector } from './vector';
import {
    readFileFooter, readFileMessages,
    readStreamSchema, readStreamMessages
} from './format';

import * as File_ from '../format/File_generated';
import * as Schema_ from '../format/Schema_generated';
import * as Message_ from '../format/Message_generated';

import ByteBuffer = flatbuffers.ByteBuffer;
import Footer = File_.org.apache.arrow.flatbuf.Footer;
import Field = Schema_.org.apache.arrow.flatbuf.Field;
import Schema = Schema_.org.apache.arrow.flatbuf.Schema;
import Message = Message_.org.apache.arrow.flatbuf.Message;
import RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;
import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;
import DictionaryBatch = Message_.org.apache.arrow.flatbuf.DictionaryBatch;
import DictionaryEncoding = Schema_.org.apache.arrow.flatbuf.DictionaryEncoding;

export type ArrowReaderContext = {
    schema?: Schema;
    footer?: Footer | null;
    dictionaries: Map<string, Vector>;
    dictionaryEncodedFields: Map<string, Field>;
    readMessages: (bb: ByteBuffer, footer: Footer) => Iterable<Message>;
};

export type VectorReaderContext = {
    node: number;
    buffer: number;
    offset: number;
    bytes: Uint8Array;
    batch: RecordBatch;
    dictionaries: Map<string, Vector>;
};

export function* readVectors(buffers: Iterable<Uint8Array | Buffer | string>, context?: ArrowReaderContext) {
    const context_ = context || {} as ArrowReaderContext;
    for (const buffer of buffers) {
        yield* readBuffer(toByteBuffer(buffer), context_);
    }
}

export async function* readVectorsAsync(buffers: AsyncIterable<Uint8Array | Buffer | string>, context?: ArrowReaderContext) {
    const context_ = context || {} as ArrowReaderContext;
    for await (const buffer of buffers) {
        yield* readBuffer(toByteBuffer(buffer), context_);
    }
}

function* readBuffer(bb: ByteBuffer, readerContext: ArrowReaderContext) {

    let { schema, footer, readMessages, dictionaryEncodedFields, dictionaries } = readerContext;

    if (!schema) {
        ({ schema, footer, readMessages, dictionaryEncodedFields } = readSchema(bb));
        readerContext.schema = schema;
        readerContext.readMessages = readMessages;
        readerContext.dictionaryEncodedFields = dictionaryEncodedFields;
        readerContext.dictionaries = dictionaries = new Map<string, Vector>();
    }

    const bytes = bb.bytes(), fieldsLength = schema.fieldsLength();
    const context = { node: 0, buffer: 0, offset: 0, bytes, dictionaries } as VectorReaderContext;
    let id: string, field: Field, vector: Vector, batch: DictionaryBatch, vectors: Array<Vector>;

    for (const message of readMessages(bb, footer!)) {

        context.node = 0;
        context.buffer = 0;
        context.offset = bb.position();

        if (message.headerType() === MessageHeader.DictionaryBatch) {
            if (batch = message.header(new DictionaryBatch())!) {
                context.batch = batch.data()!;
                id = batch.id().toFloat64().toString();
                field = dictionaryEncodedFields.get(id)!;
                vector = readValueVector(field, context);
                if (batch.isDelta() && dictionaries.has(id)) {
                    vector = dictionaries.get(id)!.concat(vector);
                }
                dictionaries.set(id, vector);
            }
            continue;
        }

        vectors = new Array<Vector>(fieldsLength);
        context.batch = message.header(new RecordBatch())!;

        for (let i = -1; ++i < fieldsLength;) {
            if ((field = schema.fields(i)!) || (vectors[i] = null as any)) {
                vectors[i] = readVector(field, context);
            }
        }

        yield vectors;
    }
}

function readSchema(bb: ByteBuffer) {
    let schema: Schema, readMessages, footer = readFileFooter(bb);
    if (footer) {
        schema = footer.schema()!;
        readMessages = readFileMessages;
    } else if (schema = readStreamSchema(bb)!) {
        readMessages = readStreamMessages;
    } else {
        throw new Error('Invalid Arrow buffer');
    }
    return { schema, footer, readMessages, dictionaryEncodedFields: readDictionaryEncodedFields(schema, new Map<string, Field>()) };
}

function readDictionaryEncodedFields(parent: Schema | Field, fields: Map<string, Field>) {
    let field: Field, encoding: DictionaryEncoding, id: string;
    let getField = parent instanceof Field ? parent.children : parent.fields;
    let getFieldCount = parent instanceof Field ? parent.childrenLength : parent.fieldsLength;
    for (let i = -1, n = getFieldCount.call(parent); ++i < n;) {
        if (field = getField.call(parent, i)!) {
            if ((encoding = field.dictionary()!) &&
                (id = encoding.id().toFloat64().toString())) {
                !fields.has(id) && fields.set(id, field);
            }
            readDictionaryEncodedFields(field, fields);
        }
    }
    return fields;
}

function toByteBuffer(bytes?: Uint8Array | Buffer | string) {
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
