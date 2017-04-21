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
import { org } from './Arrow_generated';
var arrow = org.apache.arrow;
import { vectorFromField, Vector } from './types';

export function loadVectors(buf) {
    var fileLength = buf.length, bb, footerLengthOffset, footerLength,
        footerOffset, footer, schema, field, type, type_str, i,
        len, rb_metas, rb_meta, rtrn, recordBatchBlock, recordBatchBlocks = [];
    var vectors : Vector[] = [];

    bb = new flatbuffers.ByteBuffer(buf);

    footer = _loadFooter(bb);

    schema = footer.schema();

    for (i = 0, len = schema.fieldsLength(); i < len; i += 1|0) {
        field = schema.fields(i);
        vectors.push(vectorFromField(field));
    }

    for (i = 0; i < footer.recordBatchesLength(); i += 1|0) {
        recordBatchBlock = footer.recordBatches(i);
        recordBatchBlocks.push({
            offset: recordBatchBlock.offset(),
            metaDataLength: recordBatchBlock.metaDataLength(),
            bodyLength: recordBatchBlock.bodyLength(),
        })
    }

    loadBuffersIntoVectors(recordBatchBlocks, bb, vectors);
    var rtrn : any = {};
    for (var i : any = 0; i < vectors.length; i += 1|0) {
      rtrn[vectors[i].name] = vectors[i]
    }
    return rtrn;
}

export function loadSchema(buf) {
    var footer = _loadFooter(new flatbuffers.ByteBuffer(buf));
    var schema = footer.schema();

    return parseSchema(schema);
}

function _loadFooter(bb) {
    var fileLength: number = bb.bytes_.length;

    if (fileLength < MAGIC.length*2 + 4) {
      console.error("file too small " + fileLength);
      return;
    }

    if (!_checkMagic(bb.bytes_, 0)) {
      console.error("missing magic bytes at beginning of file")
      return;
    }

    if (!_checkMagic(bb.bytes_, fileLength - MAGIC.length)) {
      console.error("missing magic bytes at end of file")
      return;
    }

    var footerLengthOffset: number = fileLength - MAGIC.length - 4;
    bb.setPosition(footerLengthOffset);
    var footerLength: number = Int64FromByteBuffer(bb, footerLengthOffset)

    if (footerLength <= 0 || footerLength + MAGIC.length*2 + 4 > fileLength)  {
      console.log("Invalid footer length: " + footerLength)
    }

    var footerOffset: number = footerLengthOffset - footerLength;
    bb.setPosition(footerOffset);
    var footer = arrow.flatbuf.Footer.getRootAsFooter(bb);

    return footer;
}

function Int64FromByteBuffer(bb, offset) {
    return ((bb.bytes_[offset + 3] & 255) << 24) |
           ((bb.bytes_[offset + 2] & 255) << 16) |
           ((bb.bytes_[offset + 1] & 255) << 8) |
           ((bb.bytes_[offset] & 255));
}


var MAGIC_STR = "ARROW1";
var MAGIC = new Uint8Array(MAGIC_STR.length);
for (var i = 0; i < MAGIC_STR.length; i += 1|0) {
    MAGIC[i] = MAGIC_STR.charCodeAt(i);
}

function _checkMagic(buf, index) {
    for (var i = 0; i < MAGIC.length; i += 1|0) {
        if (MAGIC[i] != buf[index + i]) {
            return false;
        }
    }
    return true;
}

var TYPEMAP = {}
TYPEMAP[arrow.flatbuf.Type.NONE]          = "NONE";
TYPEMAP[arrow.flatbuf.Type.Null]          = "Null";
TYPEMAP[arrow.flatbuf.Type.Int]           = "Int";
TYPEMAP[arrow.flatbuf.Type.FloatingPoint] = "FloatingPoint";
TYPEMAP[arrow.flatbuf.Type.Binary]        = "Binary";
TYPEMAP[arrow.flatbuf.Type.Utf8]          = "Utf8";
TYPEMAP[arrow.flatbuf.Type.Bool]          = "Bool";
TYPEMAP[arrow.flatbuf.Type.Decimal]       = "Decimal";
TYPEMAP[arrow.flatbuf.Type.Date]          = "Date";
TYPEMAP[arrow.flatbuf.Type.Time]          = "Time";
TYPEMAP[arrow.flatbuf.Type.Timestamp]     = "Timestamp";
TYPEMAP[arrow.flatbuf.Type.Interval]      = "Interval";
TYPEMAP[arrow.flatbuf.Type.List]          = "List";
TYPEMAP[arrow.flatbuf.Type.Struct_]       = "Struct";
TYPEMAP[arrow.flatbuf.Type.Union]         = "Union";

var VECTORTYPEMAP = {};
VECTORTYPEMAP[arrow.flatbuf.VectorType.OFFSET]   = 'OFFSET';
VECTORTYPEMAP[arrow.flatbuf.VectorType.DATA]     = 'DATA';
VECTORTYPEMAP[arrow.flatbuf.VectorType.VALIDITY] = 'VALIDITY';
VECTORTYPEMAP[arrow.flatbuf.VectorType.TYPE]     = 'TYPE';

function parseField(field) {
    var children = [];
    for (var i = 0; i < field.childrenLength(); i += 1|0) {
        children.push(parseField(field.children(i)));
    }

    var layouts = [];
    for (var i = 0; i < field.layoutLength(); i += 1|0) {
        layouts.push(VECTORTYPEMAP[field.layout(i).type()]);

    }

    return {
      name: field.name(),
      nullable: field.nullable(),
      type: TYPEMAP[field.typeType()],
      children: children,
      layout: layouts
    };
}

function parseSchema(schema) {
    var result = [];
    var this_result, type;
    for (var i = 0, len = schema.fieldsLength(); i < len; i += 1|0) {
        result.push(parseField(schema.fields(i)));
    }
    return result;
}

function parseBuffer(buffer) {
    return {
        offset: buffer.offset(),
        length: buffer.length()
    };
}

function loadBuffersIntoVectors(recordBatchBlocks, bb, vectors : Vector[]) {
    var fieldNode, recordBatchBlock, recordBatch, numBuffers, bufReader = {index: 0, node_index: 1}, field_ctr = 0;
    var buffer = bb.bytes_.buffer;
    var baseOffset = bb.bytes_.byteOffset;
    for (var i = recordBatchBlocks.length - 1; i >= 0; i -= 1|0) {
        recordBatchBlock = recordBatchBlocks[i];
        bb.setPosition(recordBatchBlock.offset.low);
        recordBatch = arrow.flatbuf.RecordBatch.getRootAsRecordBatch(bb);
        bufReader.index = 0;
        bufReader.node_index = 0;
        numBuffers = recordBatch.buffersLength();

        //console.log('num buffers: ' + recordBatch.buffersLength());
        //console.log('num nodes: ' + recordBatch.nodesLength());

        while (bufReader.index < numBuffers) {
            //console.log('Allocating buffers starting at ' + bufReader.index + '/' + numBuffers + ' to field ' + field_ctr);
            vectors[field_ctr].loadData(recordBatch, buffer, bufReader, baseOffset + recordBatchBlock.offset.low + recordBatchBlock.metaDataLength)
            field_ctr += 1;
        }
    }
}
