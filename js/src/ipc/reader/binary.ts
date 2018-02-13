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

import { Vector } from '../../vector';
import { flatbuffers } from 'flatbuffers';
import { TypeDataLoader } from './vector';
import { Message, Footer, FileBlock, RecordBatchMetadata, DictionaryBatch, BufferMetadata, FieldMetadata, } from '../metadata';
import {
    Schema, Field,
    DataType, Dictionary,
    Null, TimeBitWidth,
    Binary, Bool, Utf8, Decimal,
    Date_, Time, Timestamp, Interval,
    List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
} from '../../type';

import {
    Int8,  Uint8,
    Int16, Uint16,
    Int32, Uint32,
    Int64, Uint64,
    Float16, Float64, Float32,
} from '../../type';

import ByteBuffer = flatbuffers.ByteBuffer;

type MessageReader = (bb: ByteBuffer) => IterableIterator<RecordBatchMetadata | DictionaryBatch>;

export function* readBuffers<T extends Uint8Array | Buffer | string>(sources: Iterable<T> | Uint8Array | Buffer | string) {
    let schema: Schema | null = null;
    let dictionaries = new Map<number, Vector>();
    let readMessages: MessageReader | null = null;
    if (ArrayBuffer.isView(sources) || typeof sources === 'string') {
        sources = [sources as T];
    }
    for (const source of sources) {
        const bb = toByteBuffer(source);
        if ((!schema && ({ schema, readMessages } = readSchema(bb))) && schema && readMessages) {
            for (const message of readMessages(bb)) {
                yield {
                    schema, message,
                    loader: new BinaryDataLoader(
                        bb,
                        arrayIterator(message.nodes),
                        arrayIterator(message.buffers),
                        dictionaries
                    )
                };
            }
        }
    }
}

export async function* readBuffersAsync<T extends Uint8Array | Buffer | string>(sources: AsyncIterable<T>) {
    let schema: Schema | null = null;
    let dictionaries = new Map<number, Vector>();
    let readMessages: MessageReader | null = null;
    for await (const source of sources) {
        const bb = toByteBuffer(source);
        if ((!schema && ({ schema, readMessages } = readSchema(bb))) && schema && readMessages) {
            for (const message of readMessages(bb)) {
                yield {
                    schema, message,
                    loader: new BinaryDataLoader(
                        bb,
                        arrayIterator(message.nodes),
                        arrayIterator(message.buffers),
                        dictionaries
                    )
                };
            }
        }
    }
}

export class BinaryDataLoader extends TypeDataLoader {
    private bytes: Uint8Array;
    private messageOffset: number;
    constructor(bb: ByteBuffer, nodes: Iterator<FieldMetadata>, buffers: Iterator<BufferMetadata>, dictionaries: Map<number, Vector>) {
        super(nodes, buffers, dictionaries);
        this.bytes = bb.bytes();
        this.messageOffset = bb.position();
    }
    protected readOffsets<T extends DataType>(type: T, buffer?: BufferMetadata) { return this.readData(type, buffer); }
    protected readTypeIds<T extends DataType>(type: T, buffer?: BufferMetadata) { return this.readData(type, buffer); }
    protected readData<T extends DataType>(_type: T, { length, offset }: BufferMetadata = this.getBufferMetadata()) {
        return new Uint8Array(this.bytes.buffer, this.bytes.byteOffset + this.messageOffset + offset, length);
    }
}

function* arrayIterator(arr: Array<any>) { yield* arr; }

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

function readSchema(bb: ByteBuffer) {
    let schema: Schema, readMessages, footer: Footer | null;
    if (footer = readFileSchema(bb)) {
        schema = footer.schema;
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
            if (Message.isSchema(message)) {
                return message as Schema;
            }
        }
    }
    return null;
}

function* readStreamMessages(bb: ByteBuffer) {
    for (const message of readMessages(bb)) {
        if (Message.isRecordBatch(message)) {
            yield message;
        } else if (Message.isDictionaryBatch(message)) {
            yield message;
        } else {
            continue;
        }
        // position the buffer after the body to read the next message
        bb.setPosition(bb.position() + message.bodyLength);
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
            yield readMessage(bb, bb.readInt32(bb.position())) as RecordBatchMetadata;
        }
    };
}

function* readMessages(bb: ByteBuffer) {
    let length: number, message: Schema | RecordBatchMetadata | DictionaryBatch;
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

import * as File_ from '../../fb/File';
import * as Schema_ from '../../fb/Schema';
import * as Message_ from '../../fb/Message';

import Type = Schema_.org.apache.arrow.flatbuf.Type;
import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;
import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;
import _Footer = File_.org.apache.arrow.flatbuf.Footer;
import _Block = File_.org.apache.arrow.flatbuf.Block;
import _Message = Message_.org.apache.arrow.flatbuf.Message;
import _Schema = Schema_.org.apache.arrow.flatbuf.Schema;
import _Field = Schema_.org.apache.arrow.flatbuf.Field;
import _RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;
import _DictionaryBatch = Message_.org.apache.arrow.flatbuf.DictionaryBatch;
import _FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;
import _Buffer = Schema_.org.apache.arrow.flatbuf.Buffer;
import _DictionaryEncoding = Schema_.org.apache.arrow.flatbuf.DictionaryEncoding;
import _Null = Schema_.org.apache.arrow.flatbuf.Null;
import _Int = Schema_.org.apache.arrow.flatbuf.Int;
import _FloatingPoint = Schema_.org.apache.arrow.flatbuf.FloatingPoint;
import _Binary = Schema_.org.apache.arrow.flatbuf.Binary;
import _Bool = Schema_.org.apache.arrow.flatbuf.Bool;
import _Utf8 = Schema_.org.apache.arrow.flatbuf.Utf8;
import _Decimal = Schema_.org.apache.arrow.flatbuf.Decimal;
import _Date = Schema_.org.apache.arrow.flatbuf.Date;
import _Time = Schema_.org.apache.arrow.flatbuf.Time;
import _Timestamp = Schema_.org.apache.arrow.flatbuf.Timestamp;
import _Interval = Schema_.org.apache.arrow.flatbuf.Interval;
import _List = Schema_.org.apache.arrow.flatbuf.List;
import _Struct = Schema_.org.apache.arrow.flatbuf.Struct_;
import _Union = Schema_.org.apache.arrow.flatbuf.Union;
import _FixedSizeBinary = Schema_.org.apache.arrow.flatbuf.FixedSizeBinary;
import _FixedSizeList = Schema_.org.apache.arrow.flatbuf.FixedSizeList;
import _Map = Schema_.org.apache.arrow.flatbuf.Map;

function footerFromByteBuffer(bb: ByteBuffer) {
    const dictionaryFields = new Map<number, Field<Dictionary>>();
    const f = _Footer.getRootAsFooter(bb), s = f.schema()!;
    return new Footer(
        dictionaryBatchesFromFooter(f), recordBatchesFromFooter(f),
        new Schema(fieldsFromSchema(s, dictionaryFields), customMetadata(s), f.version(), dictionaryFields)
    );
}

function messageFromByteBuffer(bb: ByteBuffer) {
    const m = _Message.getRootAsMessage(bb)!, type = m.headerType(), version = m.version();
    switch (type) {
        case MessageHeader.Schema: return schemaFromMessage(version, m.header(new _Schema())!, new Map());
        case MessageHeader.RecordBatch: return recordBatchFromMessage(version, m.header(new _RecordBatch())!);
        case MessageHeader.DictionaryBatch: return dictionaryBatchFromMessage(version, m.header(new _DictionaryBatch())!);
    }
    return null;
    // throw new Error(`Unrecognized Message type '${type}'`);
}

function schemaFromMessage(version: MetadataVersion, s: _Schema, dictionaryFields: Map<number, Field<Dictionary>>) {
    return new Schema(fieldsFromSchema(s, dictionaryFields), customMetadata(s), version, dictionaryFields);
}

function recordBatchFromMessage(version: MetadataVersion, b: _RecordBatch) {
    return new RecordBatchMetadata(version, b.length(), fieldNodesFromRecordBatch(b), buffersFromRecordBatch(b, version));
}

function dictionaryBatchFromMessage(version: MetadataVersion, d: _DictionaryBatch) {
    return new DictionaryBatch(version, recordBatchFromMessage(version, d.data()!), d.id(), d.isDelta());
}

function dictionaryBatchesFromFooter(f: _Footer) {
    const blocks = [] as FileBlock[];
    for (let b: _Block, i = -1, n = f && f.dictionariesLength(); ++i < n;) {
        if (b = f.dictionaries(i)!) {
            blocks.push(new FileBlock(b.metaDataLength(), b.bodyLength(), b.offset()));
        }
    }
    return blocks;
}

function recordBatchesFromFooter(f: _Footer) {
    const blocks = [] as FileBlock[];
    for (let b: _Block, i = -1, n = f && f.recordBatchesLength(); ++i < n;) {
        if (b = f.recordBatches(i)!) {
            blocks.push(new FileBlock(b.metaDataLength(), b.bodyLength(), b.offset()));
        }
    }
    return blocks;
}

function fieldsFromSchema(s: _Schema, dictionaryFields: Map<number, Field<Dictionary>> | null) {
    const fields = [] as Field[];
    for (let i = -1, c: Field | null, n = s && s.fieldsLength(); ++i < n;) {
        if (c = field(s.fields(i)!, dictionaryFields)) {
            fields.push(c);
        }
    }
    return fields;
}

function fieldsFromField(f: _Field, dictionaryFields: Map<number, Field<Dictionary>> | null) {
    const fields = [] as Field[];
    for (let i = -1, c: Field | null, n = f && f.childrenLength(); ++i < n;) {
        if (c = field(f.children(i)!, dictionaryFields)) {
            fields.push(c);
        }
    }
    return fields;
}

function fieldNodesFromRecordBatch(b: _RecordBatch) {
    const fieldNodes = [] as FieldMetadata[];
    for (let i = -1, n = b.nodesLength(); ++i < n;) {
        fieldNodes.push(fieldNodeFromRecordBatch(b.nodes(i)!));
    }
    return fieldNodes;
}

function buffersFromRecordBatch(b: _RecordBatch, version: MetadataVersion) {
    const buffers = [] as BufferMetadata[];
    for (let i = -1, n = b.buffersLength(); ++i < n;) {
        let buffer = b.buffers(i)!;
        // If this Arrow buffer was written before version 4,
        // advance the buffer's bb_pos 8 bytes to skip past
        // the now-removed page id field.
        if (version < MetadataVersion.V4) {
            buffer.bb_pos += (8 * (i + 1));
        }
        buffers.push(bufferFromRecordBatch(buffer));
    }
    return buffers;
}

function field(f: _Field, dictionaryFields: Map<number, Field<Dictionary>> | null) {
    let name = f.name()!;
    let field: Field | void;
    let nullable = f.nullable();
    let metadata = customMetadata(f);
    let dataType: DataType<any> | null;
    let keysMeta: _Int | null, id: number;
    let dictMeta: _DictionaryEncoding | null;
    if (!dictionaryFields || !(dictMeta = f.dictionary())) {
        if (dataType = typeFromField(f, fieldsFromField(f, dictionaryFields))) {
            field = new Field(name, dataType, nullable, metadata);
        }
    } else if (dataType = dictionaryFields.has(id = dictMeta.id().low)
                        ? dictionaryFields.get(id)!.type.dictionary
                        : typeFromField(f, fieldsFromField(f, null))) {
        dataType = new Dictionary(dataType,
            // a dictionary index defaults to signed 32 bit int if unspecified
            (keysMeta = dictMeta.indexType()) ? intFromField(keysMeta)! : new Int32(),
            id, dictMeta.isOrdered()
        );
        field = new Field(name, dataType, nullable, metadata);
        dictionaryFields.has(id) || dictionaryFields.set(id, field as Field<Dictionary>);
    }
    return field || null;
}

function customMetadata(parent?: _Schema | _Field | null) {
    const data = new Map<string, string>();
    if (parent) {
        for (let entry, key, i = -1, n = parent.customMetadataLength() | 0; ++i < n;) {
            if ((entry = parent.customMetadata(i)) && (key = entry.key()) != null) {
                data.set(key, entry.value()!);
            }
        }
    }
    return data;
}

function fieldNodeFromRecordBatch(f: _FieldNode) {
    return new FieldMetadata(f.length(), f.nullCount());
}

function bufferFromRecordBatch(b: _Buffer) {
    return new BufferMetadata(b.offset(), b.length());
}

function typeFromField(f: _Field, children?: Field[]): DataType<any> | null {
    switch (f.typeType()) {
        case Type.NONE: return null;
        case Type.Null: return nullFromField(f.type(new _Null())!);
        case Type.Int: return intFromField(f.type(new _Int())!);
        case Type.FloatingPoint: return floatFromField(f.type(new _FloatingPoint())!);
        case Type.Binary: return binaryFromField(f.type(new _Binary())!);
        case Type.Utf8: return utf8FromField(f.type(new _Utf8())!);
        case Type.Bool: return boolFromField(f.type(new _Bool())!);
        case Type.Decimal: return decimalFromField(f.type(new _Decimal())!);
        case Type.Date: return dateFromField(f.type(new _Date())!);
        case Type.Time: return timeFromField(f.type(new _Time())!);
        case Type.Timestamp: return timestampFromField(f.type(new _Timestamp())!);
        case Type.Interval: return intervalFromField(f.type(new _Interval())!);
        case Type.List: return listFromField(f.type(new _List())!, children || []);
        case Type.Struct_: return structFromField(f.type(new _Struct())!, children || []);
        case Type.Union: return unionFromField(f.type(new _Union())!, children || []);
        case Type.FixedSizeBinary: return fixedSizeBinaryFromField(f.type(new _FixedSizeBinary())!);
        case Type.FixedSizeList: return fixedSizeListFromField(f.type(new _FixedSizeList())!, children || []);
        case Type.Map: return mapFromField(f.type(new _Map())!, children || []);
    }
    throw new Error(`Unrecognized type ${f.typeType()}`);
}

function nullFromField           (_type: _Null)                             { return new Null();                                                                }
function intFromField            (_type: _Int)                              { switch (_type.bitWidth()) {
                                                                                  case  8: return _type.isSigned() ? new  Int8() : new  Uint8();
                                                                                  case 16: return _type.isSigned() ? new Int16() : new Uint16();
                                                                                  case 32: return _type.isSigned() ? new Int32() : new Uint32();
                                                                                  case 64: return _type.isSigned() ? new Int64() : new Uint64();
                                                                              }
                                                                              return null;                                                                      }
function floatFromField          (_type: _FloatingPoint)                    { switch (_type.precision()) {
                                                                                  case Precision.HALF: return new Float16();
                                                                                  case Precision.SINGLE: return new Float32();
                                                                                  case Precision.DOUBLE: return new Float64();
                                                                              }
                                                                              return null;                                                                      }
function binaryFromField         (_type: _Binary)                           { return new Binary();                                                              }
function utf8FromField           (_type: _Utf8)                             { return new Utf8();                                                                }
function boolFromField           (_type: _Bool)                             { return new Bool();                                                                }
function decimalFromField        (_type: _Decimal)                          { return new Decimal(_type.scale(), _type.precision());                             }
function dateFromField           (_type: _Date)                             { return new Date_(_type.unit());                                                   }
function timeFromField           (_type: _Time)                             { return new Time(_type.unit(), _type.bitWidth() as TimeBitWidth);                  }
function timestampFromField      (_type: _Timestamp)                        { return new Timestamp(_type.unit(), _type.timezone());                             }
function intervalFromField       (_type: _Interval)                         { return new Interval(_type.unit());                                                }
function listFromField           (_type: _List, children: Field[])          { return new List(children);                                                        }
function structFromField         (_type: _Struct, children: Field[])        { return new Struct(children);                                                      }
function unionFromField          (_type: _Union, children: Field[])         { return new Union(_type.mode(), (_type.typeIdsArray() || []) as Type[], children); }
function fixedSizeBinaryFromField(_type: _FixedSizeBinary)                  { return new FixedSizeBinary(_type.byteWidth());                                    }
function fixedSizeListFromField  (_type: _FixedSizeList, children: Field[]) { return new FixedSizeList(_type.listSize(), children);                             }
function mapFromField            (_type: _Map, children: Field[])           { return new Map_(_type.keysSorted(), children);                                    }
