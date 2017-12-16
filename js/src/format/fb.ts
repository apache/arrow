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

import * as File_ from './fb/File';
import * as Schema_ from './fb/Schema';
import * as Message_ from './fb/Message';
import { flatbuffers } from 'flatbuffers';
import ByteBuffer = flatbuffers.ByteBuffer;
import Type = Schema_.org.apache.arrow.flatbuf.Type;
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

import {
    IntBitWidth, TimeBitWidth,
    Footer, Block, Schema, RecordBatch, DictionaryBatch, Field, DictionaryEncoding, Buffer, FieldNode,
    Null, Int, FloatingPoint, Binary, Bool, Utf8, Decimal, Date, Time, Timestamp, Interval, List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
} from './types';

export function footerFromByteBuffer(bb: ByteBuffer) {
    const f = _Footer.getRootAsFooter(bb), s = f.schema()!;
    return new Footer(
        dictionaryBatchesFromFooter(f), recordBatchesFromFooter(f),
        new Schema(f.version(), fieldsFromSchema(s), customMetadata(s), s.endianness())
    );
}

export function messageFromByteBuffer(bb: ByteBuffer) {
    const m = _Message.getRootAsMessage(bb)!, type = m.headerType(), version = m.version();
    switch (type) {
        case MessageHeader.Schema: return schemaFromMessage(version, m.header(new _Schema())!);
        case MessageHeader.RecordBatch: return recordBatchFromMessage(version, m.header(new _RecordBatch())!);
        case MessageHeader.DictionaryBatch: return dictionaryBatchFromMessage(version, m.header(new _DictionaryBatch())!);
    }
    return null;
    // throw new Error(`Unrecognized Message type '${type}'`);
}

function schemaFromMessage(version: MetadataVersion, s: _Schema) {
    return new Schema(version, fieldsFromSchema(s), customMetadata(s), s.endianness());
}

function recordBatchFromMessage(version: MetadataVersion, b: _RecordBatch) {
    return new RecordBatch(version, b.length(), fieldNodesFromRecordBatch(b), buffersFromRecordBatch(b, version));
}

function dictionaryBatchFromMessage(version: MetadataVersion, d: _DictionaryBatch) {
    return new DictionaryBatch(version, recordBatchFromMessage(version, d.data()!), d.id(), d.isDelta());
}

function dictionaryBatchesFromFooter(f: _Footer) {
    const blocks = [] as Block[];
    for (let b: _Block, i = -1, n = f && f.dictionariesLength(); ++i < n;) {
        if (b = f.dictionaries(i)!) {
            blocks.push(new Block(b.metaDataLength(), b.bodyLength(), b.offset()));
        }
    }
    return blocks;
}

function recordBatchesFromFooter(f: _Footer) {
    const blocks = [] as Block[];
    for (let b: _Block, i = -1, n = f && f.recordBatchesLength(); ++i < n;) {
        if (b = f.recordBatches(i)!) {
            blocks.push(new Block(b.metaDataLength(), b.bodyLength(), b.offset()));
        }
    }
    return blocks;
}

function fieldsFromSchema(s: _Schema) {
    const fields = [] as Field[];
    for (let i = -1, n = s && s.fieldsLength(); ++i < n;) {
        fields.push(field(s.fields(i)!));
    }
    return fields;
}

function fieldsFromField(f: _Field) {
    const fields = [] as Field[];
    for (let i = -1, n = f && f.childrenLength(); ++i < n;) {
        fields.push(field(f.children(i)!));
    }
    return fields;
}

function fieldNodesFromRecordBatch(b: _RecordBatch) {
    const fieldNodes = [] as FieldNode[];
    for (let i = -1, n = b.nodesLength(); ++i < n;) {
        fieldNodes.push(fieldNodeFromRecordBatch(b.nodes(i)!));
    }
    return fieldNodes;
}

function buffersFromRecordBatch(b: _RecordBatch, version: MetadataVersion) {
    const buffers = [] as Buffer[];
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

function field(f: _Field) {
    return new Field(
        f.name()!,
        typeFromField(f),
        f.typeType(),
        f.nullable(),
        fieldsFromField(f),
        customMetadata(f),
        dictionaryEncodingFromField(f)
    );
}

function dictionaryEncodingFromField(f: _Field) {
    let t: _Int | null;
    let e: _DictionaryEncoding | null;
    if (e = f.dictionary()) {
        if (t = e.indexType()) {
            return new DictionaryEncoding(new Int(t.isSigned(), t.bitWidth() as IntBitWidth), e.id(), e.isOrdered());
        }
        return new DictionaryEncoding(null, e.id(), e.isOrdered());
    }
    return undefined;
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
    return new FieldNode(f.length(), f.nullCount());
}

function bufferFromRecordBatch(b: _Buffer) {
    return new Buffer(b.offset(), b.length());
}

function typeFromField(f: _Field) {
    switch (f.typeType()) {
        case Type.NONE: return nullFromField(f.type(new _Null())!);
        case Type.Null: return nullFromField(f.type(new _Null())!);
        case Type.Int: return intFromField(f.type(new _Int())!);
        case Type.FloatingPoint: return floatingPointFromField(f.type(new _FloatingPoint())!);
        case Type.Binary: return binaryFromField(f.type(new _Binary())!);
        case Type.Utf8: return utf8FromField(f.type(new _Utf8())!);
        case Type.Bool: return boolFromField(f.type(new _Bool())!);
        case Type.Decimal: return decimalFromField(f.type(new _Decimal())!);
        case Type.Date: return dateFromField(f.type(new _Date())!);
        case Type.Time: return timeFromField(f.type(new _Time())!);
        case Type.Timestamp: return timestampFromField(f.type(new _Timestamp())!);
        case Type.Interval: return intervalFromField(f.type(new _Interval())!);
        case Type.List: return listFromField(f.type(new _List())!);
        case Type.Struct_: return structFromField(f.type(new _Struct())!);
        case Type.Union: return unionFromField(f.type(new _Union())!);
        case Type.FixedSizeBinary: return fixedSizeBinaryFromField(f.type(new _FixedSizeBinary())!);
        case Type.FixedSizeList: return fixedSizeListFromField(f.type(new _FixedSizeList())!);
        case Type.Map: return mapFromField(f.type(new _Map())!);
    }
    throw new Error(`Unrecognized type ${f.typeType()}`);
}

function nullFromField(_type: _Null) { return new Null(); }
function intFromField(_type: _Int) { return new Int(_type.isSigned(), _type.bitWidth() as IntBitWidth); }
function floatingPointFromField(_type: _FloatingPoint) { return new FloatingPoint(_type.precision()); }
function binaryFromField(_type: _Binary) { return new Binary(); }
function utf8FromField(_type: _Utf8) { return new Utf8(); }
function boolFromField(_type: _Bool) { return new Bool(); }
function decimalFromField(_type: _Decimal) { return new Decimal(_type.scale(), _type.precision()); }
function dateFromField(_type: _Date) { return new Date(_type.unit()); }
function timeFromField(_type: _Time) { return new Time(_type.unit(), _type.bitWidth() as TimeBitWidth); }
function timestampFromField(_type: _Timestamp) { return new Timestamp(_type.unit(), _type.timezone()); }
function intervalFromField(_type: _Interval) { return new Interval(_type.unit()); }
function listFromField(_type: _List) { return new List(); }
function structFromField(_type: _Struct) { return new Struct(); }
function unionFromField(_type: _Union) { return new Union(_type.mode(), (_type.typeIdsArray() || []) as Type[]); }
function fixedSizeBinaryFromField(_type: _FixedSizeBinary) { return new FixedSizeBinary(_type.byteWidth()); }
function fixedSizeListFromField(_type: _FixedSizeList) { return new FixedSizeList(_type.listSize()); }
function mapFromField(_type: _Map) { return new Map_(_type.keysSorted()); }
