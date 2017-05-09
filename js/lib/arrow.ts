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
var DictionaryBatch = org.apache.arrow.flatbuf.DictionaryBatch;
var Schema = org.apache.arrow.flatbuf.Schema;
var Type = org.apache.arrow.flatbuf.Type;
var VectorType = org.apache.arrow.flatbuf.VectorType;

export class ArrowReader {

    private bb;
    private schema: any = [];
    private vectors: Vector[];
    private vectorMap: any = {};
    private dictionaries: any = {};
    private batches: any = [];
    private batchIndex: number = 0;

    constructor(bb, schema, vectors: Vector[], batches, dictionaries) {
        this.bb = bb;
        this.schema = schema;
        this.vectors = vectors;
        for (var i = 0; i < vectors.length; i += 1|0) {
            this.vectorMap[vectors[i].name] = vectors[i]
        }
        this.batches = batches;
        this.dictionaries = dictionaries;
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

export function getSchema(buf) { return getReader(buf).getSchema(); }

export function getReader(buf) : ArrowReader {
    if (_checkMagic(buf, 0)) {
        return getFileReader(buf);
    } else {
        return getStreamReader(buf);
    }
}

export function getStreamReader(buf) : ArrowReader {
    var bb = new ByteBuffer(buf);

    var schema = _loadSchema(bb),
        field,
        vectors: Vector[] = [],
        i,j,
        iLen,jLen,
        batch,
        recordBatches = [],
        dictionaryBatches = [],
        dictionaries = {};

    for (i = 0, iLen = schema.fieldsLength(); i < iLen; i += 1|0) {
        field = schema.fields(i);
        _createDictionaryVectors(field, dictionaries);
        vectors.push(vectorFromField(field, dictionaries));
    }

    while (bb.position() < bb.capacity()) {
      batch = _loadBatch(bb);
      if (batch == null) {
          break;
      } else if (batch.type == MessageHeader.DictionaryBatch) {
          dictionaryBatches.push(batch);
      } else if (batch.type == MessageHeader.RecordBatch) {
          recordBatches.push(batch)
      } else {
          console.error("Expected batch type" + MessageHeader.RecordBatch + " or " +
              MessageHeader.DictionaryBatch + " but got " + batch.type);
      }
    }

    // load dictionary vectors
    for (i = 0; i < dictionaryBatches.length; i += 1|0) {
      batch = dictionaryBatches[i];
      loadVectors(bb, [dictionaries[batch.id]], batch);
    }

    return new ArrowReader(bb, parseSchema(schema), vectors, recordBatches, dictionaries);
}

export function getFileReader (buf) : ArrowReader {
    var bb = new ByteBuffer(buf);

    var footer = _loadFooter(bb);

    var schema = footer.schema();
    var i, len, field,
        vectors: Vector[] = [],
        block,
        batch,
        recordBatchBlocks = [],
        dictionaryBatchBlocks = [],
        dictionaries = {};

    for (i = 0, len = schema.fieldsLength(); i < len; i += 1|0) {
        field = schema.fields(i);
        _createDictionaryVectors(field, dictionaries);
        vectors.push(vectorFromField(field, dictionaries));
    }

    for (i = 0; i < footer.dictionariesLength(); i += 1|0) {
        block = footer.dictionaries(i);
        dictionaryBatchBlocks.push({
            offset: block.offset().low,
            metaDataLength: block.metaDataLength(),
            bodyLength: block.bodyLength().low,
        })
    }

    for (i = 0; i < footer.recordBatchesLength(); i += 1|0) {
        block = footer.recordBatches(i);
        recordBatchBlocks.push({
            offset: block.offset().low,
            metaDataLength: block.metaDataLength(),
            bodyLength: block.bodyLength().low,
        })
    }

    var dictionaryBatches = dictionaryBatchBlocks.map(function (block) {
        bb.setPosition(block.offset);
        // TODO: Make sure this is a dictionary batch
        return _loadBatch(bb);
    });

    var recordBatches = recordBatchBlocks.map(function (block) {
        bb.setPosition(block.offset);
        // TODO: Make sure this is a record batch
        return _loadBatch(bb);
    });

    // load dictionary vectors
    for (i = 0; i < dictionaryBatches.length; i += 1|0) {
        batch = dictionaryBatches[i];
        loadVectors(bb, [dictionaries[batch.id]], batch);
    }

    return new ArrowReader(bb, parseSchema(schema), vectors, recordBatches, dictionaries);
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
    var footerLength: number = Int32FromByteBuffer(bb, footerLengthOffset)

    if (footerLength <= 0 || footerLength + MAGIC.length*2 + 4 > fileLength)  {
      console.log("Invalid footer length: " + footerLength)
    }

    var footerOffset: number = footerLengthOffset - footerLength;
    bb.setPosition(footerOffset);
    var footer = Footer.getRootAsFooter(bb);

    return footer;
}

function _loadSchema(bb) {
    var message =_loadMessage(bb);
    if (message.headerType() != MessageHeader.Schema) {
        console.error("Expected header type " + MessageHeader.Schema + " but got " + message.headerType());
        return;
    }
    return message.header(new Schema());
}

function _loadBatch(bb) {
    var message = _loadMessage(bb);
    if (message == null) {
        return;
    } else if (message.headerType() == MessageHeader.RecordBatch) {
        var batch = { header: message.header(new RecordBatch()), length: message.bodyLength().low }
        return _loadRecordBatch(bb, batch);
    } else if (message.headerType() == MessageHeader.DictionaryBatch) {
        var batch = { header: message.header(new DictionaryBatch()), length: message.bodyLength().low }
        return _loadDictionaryBatch(bb, batch);
    } else {
        console.error("Expected header type " + MessageHeader.RecordBatch + " or " + MessageHeader.DictionaryBatch +
            " but got " + message.headerType());
        return;
    }
}

function _loadRecordBatch(bb, batch) {
    var data = batch.header;
    var i, nodes_ = [], nodesLength = data.nodesLength();
    var buffer, buffers_ = [], buffersLength = data.buffersLength();

    for (i = 0; i < nodesLength; i += 1) {
        nodes_.push(data.nodes(i));
    }
    for (i = 0; i < buffersLength; i += 1) {
        buffer = data.buffers(i);
        buffers_.push({ offset: bb.position() + buffer.offset().low, length: buffer.length().low });
    }
    // position the buffer after the body to read the next message
    bb.setPosition(bb.position() + batch.length);

    return { nodes: nodes_, buffers: buffers_, length: data.length().low, type: MessageHeader.RecordBatch };
}

function _loadDictionaryBatch(bb, batch) {
    var id_ = batch.header.id().toFloat64().toString(), data = batch.header.data();
    var i, nodes_ = [], nodesLength = data.nodesLength();
    var buffer, buffers_ = [], buffersLength = data.buffersLength();

    for (i = 0; i < nodesLength; i += 1) {
        nodes_.push(data.nodes(i));
    }
    for (i = 0; i < buffersLength; i += 1) {
        buffer = data.buffers(i);
        buffers_.push({ offset: bb.position() + buffer.offset().low, length: buffer.length().low });
    }
    // position the buffer after the body to read the next message
    bb.setPosition(bb.position() + batch.length);

    return { id: id_, nodes: nodes_, buffers: buffers_, length: data.length().low, type: MessageHeader.DictionaryBatch };
}

function _loadMessage(bb) {
    var messageLength: number = Int32FromByteBuffer(bb, bb.position());
    if (messageLength == 0) {
      return;
    }
    bb.setPosition(bb.position() + 4);
    var message = Message.getRootAsMessage(bb);
    // position the buffer at the end of the message so it's ready to read further
    bb.setPosition(bb.position() + messageLength);

    return message;
}

function _createDictionaryVectors(field, dictionaries) {
    var encoding = field.dictionary();
    if (encoding != null) {
        var id = encoding.id().toFloat64().toString();
        if (dictionaries[id] == null) {
            // create a field for the dictionary
            var dictionaryField = _createDictionaryField(id, field);
            dictionaries[id] = vectorFromField(dictionaryField, null);
        }
    }

    // recursively examine child fields
    for (var i = 0, len = field.childrenLength(); i < len; i += 1|0) {
        _createDictionaryVectors(field.children(i), dictionaries);
    }
}

function _createDictionaryField(id, field) {
    var builder = new flatbuffers.Builder();
    var nameOffset = builder.createString("dict-" + id);

    var typeType = field.typeType();
    var typeOffset;
    if (typeType === Type.Int) {
        var type = field.type(new org.apache.arrow.flatbuf.Int());
        org.apache.arrow.flatbuf.Int.startInt(builder);
        org.apache.arrow.flatbuf.Int.addBitWidth(builder, type.bitWidth());
        org.apache.arrow.flatbuf.Int.addIsSigned(builder, type.isSigned());
        typeOffset = org.apache.arrow.flatbuf.Int.endInt(builder);
    } else if (typeType === Type.FloatingPoint) {
        var type = field.type(new org.apache.arrow.flatbuf.FloatingPoint());
        org.apache.arrow.flatbuf.FloatingPoint.startFloatingPoint(builder);
        org.apache.arrow.flatbuf.FloatingPoint.addPrecision(builder, type.precision());
        typeOffset = org.apache.arrow.flatbuf.FloatingPoint.endFloatingPoint(builder);
    } else if (typeType === Type.Utf8) {
        org.apache.arrow.flatbuf.Utf8.startUtf8(builder);
        typeOffset = org.apache.arrow.flatbuf.Utf8.endUtf8(builder);
    } else if (typeType === Type.Date) {
        var type = field.type(new org.apache.arrow.flatbuf.Date());
        org.apache.arrow.flatbuf.Date.startDate(builder);
        org.apache.arrow.flatbuf.Date.addUnit(builder, type.unit());
        typeOffset = org.apache.arrow.flatbuf.Date.endDate(builder);
    } else {
        throw "Unimplemented dictionary type " + typeType;
    }
    if (field.childrenLength() > 0) {
      throw "Dictionary encoded fields can't have children"
    }
    var childrenOffset = org.apache.arrow.flatbuf.Field.createChildrenVector(builder, []);

    var layout, layoutOffsets = [];
    for (var i = 0, len = field.layoutLength(); i < len; i += 1|0) {
        layout = field.layout(i);
        org.apache.arrow.flatbuf.VectorLayout.startVectorLayout(builder);
        org.apache.arrow.flatbuf.VectorLayout.addBitWidth(builder, layout.bitWidth());
        org.apache.arrow.flatbuf.VectorLayout.addType(builder, layout.type());
        layoutOffsets.push(org.apache.arrow.flatbuf.VectorLayout.endVectorLayout(builder));
    }
    var layoutOffset = org.apache.arrow.flatbuf.Field.createLayoutVector(builder, layoutOffsets);

    org.apache.arrow.flatbuf.Field.startField(builder);
    org.apache.arrow.flatbuf.Field.addName(builder, nameOffset);
    org.apache.arrow.flatbuf.Field.addNullable(builder, field.nullable());
    org.apache.arrow.flatbuf.Field.addTypeType(builder, typeType);
    org.apache.arrow.flatbuf.Field.addType(builder, typeOffset);
    org.apache.arrow.flatbuf.Field.addChildren(builder, childrenOffset);
    org.apache.arrow.flatbuf.Field.addLayout(builder, layoutOffset);
    var offset = org.apache.arrow.flatbuf.Field.endField(builder);
    builder.finish(offset);

    return org.apache.arrow.flatbuf.Field.getRootAsField(builder.bb);
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

    // dictionary vectors are always ints, so will have a data vector plus optional null vector
    if (vector.field.dictionary() == null) {
        ownBuffersLength = vector.field.layoutLength();
    } else if (vector.field.nullable()) {
        ownBuffersLength = 2;
    } else {
        ownBuffersLength = 1;
    }

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
