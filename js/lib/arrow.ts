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
import { vectorFromField, Vector } from './types';

import ByteBuffer = flatbuffers.ByteBuffer;
var Footer = org.apache.arrow.flatbuf.Footer;
var Message = org.apache.arrow.flatbuf.Message;
var MessageHeader = org.apache.arrow.flatbuf.MessageHeader;
var RecordBatch = org.apache.arrow.flatbuf.RecordBatch;
var Schema = org.apache.arrow.flatbuf.Schema;
var Type = org.apache.arrow.flatbuf.Type;
var VectorType = org.apache.arrow.flatbuf.VectorType;

export class ArrowReader {

    private bb;
    private schema: any = [];
    private vectors: Vector[];
    private vectorMap: any = {};
    private batches: any = [];
    private batchIndex: number = 0;

    constructor(bb, schema, vectors: Vector[], batches) {
        this.bb = bb;
        this.schema = schema;
        this.vectors = vectors;
        for (var i: any = 0; i < vectors.length; i += 1|0) {
            this.vectorMap[vectors[i].name] = vectors[i]
        }
        this.batches = batches;
    }

    loadNextBatch() {
        if (this.batchIndex < this.batches.length) {
            var batch = this.batches[this.batchIndex];
            this.batchIndex += 1;
            loadVectors(this.bb, this.vectors, batch);
            return batch.length;
        } else {
            return 0;
        }
    }

    getSchema() {
        return this.schema;
    }

    getVectors() {
        return this.vectors;
    }

    getVector(name) {
        return this.vectorMap[name];
    }

    getBatchCount() {
        return this.batches.length;
    }

    // the index of the next batch to be loaded
    getBatchIndex() {
        return this.batchIndex;
    }

    // set the index of the next batch to be loaded
    setBatchIndex(i: number) {
        this.batchIndex = i;
    }
}

export function getReader(buf) {
    var bb = new ByteBuffer(buf);

    // if this is the file format (vs streaming), skip past the magic bytes
    if (_checkMagic(bb.bytes_, 0)) {
      // TODO stop before logging error for reading past record batches
      bb.setPosition(MAGIC.length);
    }

    var schema = _loadSchema(bb),
        field,
        vectors: Vector[] = [],
        i,
        len,
        recordBatch,
        recordBatches = [];

    for (i = 0, len = schema.fieldsLength(); i < len; i += 1|0) {
        field = schema.fields(i);
        vectors.push(vectorFromField(field));
    }

    while (bb.position() < bb.capacity()) {
      // TODO read dictionaries before record batches
      recordBatch = _loadRecordBatch(bb);
      if (recordBatch == null) {
          break;
      }
      recordBatches.push(recordBatch)
    }

    return new ArrowReader(bb, parseSchema(schema), vectors, recordBatches);
}

function _loadSchema(bb) {
    return _loadMessage(bb, MessageHeader.Schema, new Schema()).header;
}

function _loadRecordBatch(bb) {
    var i, loaded = _loadMessage(bb, MessageHeader.RecordBatch, new RecordBatch());
    if (loaded == null) {
        return;
    }
    var nodes_ = [], nodesLength = loaded.header.nodesLength();
    var buffer, buffers_ = [], buffersLength = loaded.header.buffersLength();

    for (i = 0; i < nodesLength; i += 1) {
        nodes_.push(loaded.header.nodes(i));
    }
    for (i = 0; i < buffersLength; i += 1) {
        buffer = loaded.header.buffers(i);
        buffers_.push({ offset: bb.position() + buffer.offset().low, length: buffer.length().low });
    }
    // position the buffer after the body to read the next message
    bb.setPosition(bb.position() + loaded.length);

    return { nodes: nodes_, buffers: buffers_, length: loaded.header.length().low };
}

function _loadMessage(bb, type, container) {
    var messageLength: number = Int32FromByteBuffer(bb, bb.position());
    if (messageLength == 0) {
      return;
    }
    bb.setPosition(bb.position() + 4);
    var message = Message.getRootAsMessage(bb);
    // position the buffer at the end of the message so it's ready to read further
    bb.setPosition(bb.position() + messageLength);

    if (message == null) {
      console.error("Unexpected end of input");
      return;
    } else if (message.headerType() != type) {
      console.error("Expected header type " + type + " but got " + message.headerType());
      return;
    }
    return { header: message.header(container), length: message.bodyLength().low };
}

function Int32FromByteBuffer(bb, offset) {
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
TYPEMAP[Type.NONE]          = "NONE";
TYPEMAP[Type.Null]          = "Null";
TYPEMAP[Type.Int]           = "Int";
TYPEMAP[Type.FloatingPoint] = "FloatingPoint";
TYPEMAP[Type.Binary]        = "Binary";
TYPEMAP[Type.Utf8]          = "Utf8";
TYPEMAP[Type.Bool]          = "Bool";
TYPEMAP[Type.Decimal]       = "Decimal";
TYPEMAP[Type.Date]          = "Date";
TYPEMAP[Type.Time]          = "Time";
TYPEMAP[Type.Timestamp]     = "Timestamp";
TYPEMAP[Type.Interval]      = "Interval";
TYPEMAP[Type.List]          = "List";
TYPEMAP[Type.FixedSizeList] = "FixedSizeList";
TYPEMAP[Type.Struct_]       = "Struct";
TYPEMAP[Type.Union]         = "Union";

var VECTORTYPEMAP = {};
VECTORTYPEMAP[VectorType.OFFSET]   = 'OFFSET';
VECTORTYPEMAP[VectorType.DATA]     = 'DATA';
VECTORTYPEMAP[VectorType.VALIDITY] = 'VALIDITY';
VECTORTYPEMAP[VectorType.TYPE]     = 'TYPE';

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

function loadVectors(bb, vectors: Vector[], recordBatch) {
    var indices = { bufferIndex: 0, nodeIndex: 0 }, i;
    for (i = 0; i < vectors.length; i += 1) {
        loadVector(bb, vectors[i], recordBatch, indices);
    }
}

/**
 * Loads a vector with data from a batch
 *   recordBatch: { nodes: org.apache.arrow.flatbuf.FieldNode[], buffers: { offset: number, length: number }[] }
 */
function loadVector(bb, vector: Vector, recordBatch, indices) {
    var node = recordBatch.nodes[indices.nodeIndex], ownBuffersLength, ownBuffers = [], i;
    indices.nodeIndex += 1;
    ownBuffersLength = vector.field.layoutLength();
    for (i = 0; i < ownBuffersLength; i += 1) {
        ownBuffers.push(recordBatch.buffers[indices.bufferIndex + i]);
    }
    indices.bufferIndex += ownBuffersLength;

    vector.loadData(bb, node, ownBuffers);

    var children = vector.getChildVectors();
    for (i = 0; i < children.length; i++) {
        loadVector(bb, children[i], recordBatch, indices);
    }
}
