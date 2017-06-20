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

import { flatbuffers } from "flatbuffers";
import { org } from "./Arrow_generated";
import { Vector, vectorFromField } from "./types";

import ByteBuffer = flatbuffers.ByteBuffer;
const Footer = org.apache.arrow.flatbuf.Footer;
const Message = org.apache.arrow.flatbuf.Message;
const MessageHeader = org.apache.arrow.flatbuf.MessageHeader;
const RecordBatch = org.apache.arrow.flatbuf.RecordBatch;
const DictionaryBatch = org.apache.arrow.flatbuf.DictionaryBatch;
const Schema = org.apache.arrow.flatbuf.Schema;
const Type = org.apache.arrow.flatbuf.Type;
const VectorType = org.apache.arrow.flatbuf.VectorType;

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
        for (let i = 0; i < vectors.length; i++) {
            this.vectorMap[vectors[i].name] = vectors[i];
        }
        this.batches = batches;
        this.dictionaries = dictionaries;
    }

    public loadNextBatch() {
        if (this.batchIndex < this.batches.length) {
            const batch = this.batches[this.batchIndex];
            this.batchIndex += 1;
            loadVectors(this.bb, this.vectors, batch);
            return batch.length;
        } else {
            return 0;
        }
    }

    public getSchema() {
        return this.schema;
    }

    public getVectors() {
        return this.vectors;
    }

    public getVector(name) {
        return this.vectorMap[name];
    }

    public getBatchCount() {
        return this.batches.length;
    }

    // the index of the next batch to be loaded
    public getBatchIndex() {
        return this.batchIndex;
    }

    // set the index of the next batch to be loaded
    public setBatchIndex(i: number) {
        this.batchIndex = i;
    }
}

export function getSchema(buf) { return getReader(buf).getSchema(); }

export function getReader(buf): ArrowReader {
    if (_checkMagic(buf, 0)) {
        return getFileReader(buf);
    } else {
        return getStreamReader(buf);
    }
}

export function getStreamReader(buf): ArrowReader {
    const bb = new ByteBuffer(buf);

    const schema = _loadSchema(bb);
    let field;
    const vectors: Vector[] = [];
    let i;
    let iLen;
    let batch;
    const recordBatches = [];
    const dictionaryBatches = [];
    const dictionaries = {};

    for (i = 0, iLen = schema.fieldsLength(); i < iLen; i++) {
        field = schema.fields(i);
        _createDictionaryVectors(field, dictionaries);
        vectors.push(vectorFromField(field, dictionaries));
    }

    while (bb.position() < bb.capacity()) {
      batch = _loadBatch(bb);
      if (batch == null) {
          break;
      } else if (batch.type === MessageHeader.DictionaryBatch) {
          dictionaryBatches.push(batch);
      } else if (batch.type === MessageHeader.RecordBatch) {
          recordBatches.push(batch);
      } else {
          throw new Error("Expected batch type" + MessageHeader.RecordBatch + " or " +
              MessageHeader.DictionaryBatch + " but got " + batch.type);
      }
    }

    // load dictionary vectors
    for (i = 0; i < dictionaryBatches.length; i++) {
      batch = dictionaryBatches[i];
      loadVectors(bb, [dictionaries[batch.id]], batch);
    }

    return new ArrowReader(bb, parseSchema(schema), vectors, recordBatches, dictionaries);
}

export function getFileReader(buf): ArrowReader {
    const bb = new ByteBuffer(buf);

    const footer = _loadFooter(bb);

    const schema = footer.schema();
    let i;
    let len;
    let field;
    const vectors: Vector[] = [];
    let block;
    let batch;
    const recordBatchBlocks = [];
    const dictionaryBatchBlocks = [];
    const dictionaries = {};

    for (i = 0, len = schema.fieldsLength(); i < len; i++) {
        field = schema.fields(i);
        _createDictionaryVectors(field, dictionaries);
        vectors.push(vectorFromField(field, dictionaries));
    }

    for (i = 0; i < footer.dictionariesLength(); i++) {
        block = footer.dictionaries(i);
        dictionaryBatchBlocks.push({
            bodyLength: block.bodyLength().low,
            metaDataLength: block.metaDataLength(),
            offset: block.offset().low,
        });
    }

    for (i = 0; i < footer.recordBatchesLength(); i++) {
        block = footer.recordBatches(i);
        recordBatchBlocks.push({
            bodyLength: block.bodyLength().low,
            metaDataLength: block.metaDataLength(),
            offset: block.offset().low,
        });
    }

    const dictionaryBatches = dictionaryBatchBlocks.map((batchBlock) => {
        bb.setPosition(batchBlock.offset);
        // TODO: Make sure this is a dictionary batch
        return _loadBatch(bb);
    });

    const recordBatches = recordBatchBlocks.map((batchBlock) => {
        bb.setPosition(batchBlock.offset);
        // TODO: Make sure this is a record batch
        return _loadBatch(bb);
    });

    // load dictionary vectors
    for (i = 0; i < dictionaryBatches.length; i++) {
        batch = dictionaryBatches[i];
        loadVectors(bb, [dictionaries[batch.id]], batch);
    }

    return new ArrowReader(bb, parseSchema(schema), vectors, recordBatches, dictionaries);
}

function _loadFooter(bb) {
    const fileLength: number = bb.bytes_.length;

    if (fileLength < MAGIC.length * 2 + 4) {
      throw new Error("file too small " + fileLength);
    }

    if (!_checkMagic(bb.bytes_, 0)) {
      throw new Error("missing magic bytes at beginning of file");
    }

    if (!_checkMagic(bb.bytes_, fileLength - MAGIC.length)) {
      throw new Error("missing magic bytes at end of file");
    }

    const footerLengthOffset: number = fileLength - MAGIC.length - 4;
    bb.setPosition(footerLengthOffset);
    const footerLength: number = Int32FromByteBuffer(bb, footerLengthOffset);

    if (footerLength <= 0 || footerLength + MAGIC.length * 2 + 4 > fileLength)  {
      throw new Error("Invalid footer length: " + footerLength);
    }

    const footerOffset: number = footerLengthOffset - footerLength;
    bb.setPosition(footerOffset);
    const footer = Footer.getRootAsFooter(bb);

    return footer;
}

function _loadSchema(bb) {
    const message = _loadMessage(bb);
    if (message.headerType() !== MessageHeader.Schema) {
        throw new Error("Expected header type " + MessageHeader.Schema + " but got " + message.headerType());
    }
    return message.header(new Schema());
}

function _loadBatch(bb) {
    const message = _loadMessage(bb);
    if (message == null) {
        return;
    } else if (message.headerType() === MessageHeader.RecordBatch) {
        const batch = { header: message.header(new RecordBatch()), length: message.bodyLength().low };
        return _loadRecordBatch(bb, batch);
    } else if (message.headerType() === MessageHeader.DictionaryBatch) {
        const batch = { header: message.header(new DictionaryBatch()), length: message.bodyLength().low };
        return _loadDictionaryBatch(bb, batch);
    } else {
        throw new Error("Expected header type " + MessageHeader.RecordBatch + " or " + MessageHeader.DictionaryBatch +
            " but got " + message.headerType());
    }
}

function _loadRecordBatch(bb, batch) {
    const data = batch.header;
    let i;
    const nodesLength = data.nodesLength();
    const nodes = new Array(nodesLength);
    let buffer;
    const buffersLength = data.buffersLength();
    const buffers = new Array(buffersLength);

    for (i = 0; i < nodesLength; i += 1) {
        nodes[i] = data.nodes(i);
    }

    for (i = 0; i < buffersLength; i += 1) {
        buffer = data.buffers(i);
        buffers[i] = {
            length: buffer.length().low,
            offset: bb.position() + buffer.offset().low,
        };
    }
    // position the buffer after the body to read the next message
    bb.setPosition(bb.position() + batch.length);

    return { nodes, buffers, length: data.length().low, type: MessageHeader.RecordBatch };
}

function _loadDictionaryBatch(bb, batch) {
    const id = batch.header.id().toFloat64().toString();
    const data = batch.header.data();
    let i;
    const nodesLength = data.nodesLength();
    const nodes = new Array(nodesLength);
    let buffer;
    const buffersLength = data.buffersLength();
    const buffers = new Array(buffersLength);

    for (i = 0; i < nodesLength; i += 1) {
        nodes[i] = data.nodes(i);
    }
    for (i = 0; i < buffersLength; i += 1) {
        buffer = data.buffers(i);
        buffers[i] = {
            length: buffer.length().low,
            offset: bb.position() + buffer.offset().low,
        };
    }
    // position the buffer after the body to read the next message
    bb.setPosition(bb.position() + batch.length);

    return {
        buffers,
        id,
        length: data.length().low,
        nodes,
        type: MessageHeader.DictionaryBatch,
    };
}

function _loadMessage(bb) {
    const messageLength: number = Int32FromByteBuffer(bb, bb.position());
    if (messageLength === 0) {
      return;
    }
    bb.setPosition(bb.position() + 4);
    const message = Message.getRootAsMessage(bb);
    // position the buffer at the end of the message so it's ready to read further
    bb.setPosition(bb.position() + messageLength);

    return message;
}

function _createDictionaryVectors(field, dictionaries) {
    const encoding = field.dictionary();
    if (encoding != null) {
        const id = encoding.id().toFloat64().toString();
        if (dictionaries[id] == null) {
            // create a field for the dictionary
            const dictionaryField = _createDictionaryField(id, field);
            dictionaries[id] = vectorFromField(dictionaryField, null);
        }
    }

    // recursively examine child fields
    for (let i = 0, len = field.childrenLength(); i < len; i++) {
        _createDictionaryVectors(field.children(i), dictionaries);
    }
}

function _createDictionaryField(id, field) {
    const builder = new flatbuffers.Builder();
    const nameOffset = builder.createString("dict-" + id);

    const typeType = field.typeType();
    let typeOffset;
    if (typeType === Type.Int) {
        const type = field.type(new org.apache.arrow.flatbuf.Int());
        org.apache.arrow.flatbuf.Int.startInt(builder);
        org.apache.arrow.flatbuf.Int.addBitWidth(builder, type.bitWidth());
        org.apache.arrow.flatbuf.Int.addIsSigned(builder, type.isSigned());
        typeOffset = org.apache.arrow.flatbuf.Int.endInt(builder);
    } else if (typeType === Type.FloatingPoint) {
        const type = field.type(new org.apache.arrow.flatbuf.FloatingPoint());
        org.apache.arrow.flatbuf.FloatingPoint.startFloatingPoint(builder);
        org.apache.arrow.flatbuf.FloatingPoint.addPrecision(builder, type.precision());
        typeOffset = org.apache.arrow.flatbuf.FloatingPoint.endFloatingPoint(builder);
    } else if (typeType === Type.Utf8) {
        org.apache.arrow.flatbuf.Utf8.startUtf8(builder);
        typeOffset = org.apache.arrow.flatbuf.Utf8.endUtf8(builder);
    } else if (typeType === Type.Date) {
        const type = field.type(new org.apache.arrow.flatbuf.Date());
        org.apache.arrow.flatbuf.Date.startDate(builder);
        org.apache.arrow.flatbuf.Date.addUnit(builder, type.unit());
        typeOffset = org.apache.arrow.flatbuf.Date.endDate(builder);
    } else {
        throw new Error("Unimplemented dictionary type " + typeType);
    }
    if (field.childrenLength() > 0) {
      throw new Error("Dictionary encoded fields can't have children");
    }
    const childrenOffset = org.apache.arrow.flatbuf.Field.createChildrenVector(builder, []);

    let layout;
    const layoutOffsets = [];
    for (let i = 0, len = field.layoutLength(); i < len; i++) {
        layout = field.layout(i);
        org.apache.arrow.flatbuf.VectorLayout.startVectorLayout(builder);
        org.apache.arrow.flatbuf.VectorLayout.addBitWidth(builder, layout.bitWidth());
        org.apache.arrow.flatbuf.VectorLayout.addType(builder, layout.type());
        layoutOffsets.push(org.apache.arrow.flatbuf.VectorLayout.endVectorLayout(builder));
    }
    const layoutOffset = org.apache.arrow.flatbuf.Field.createLayoutVector(builder, layoutOffsets);

    org.apache.arrow.flatbuf.Field.startField(builder);
    org.apache.arrow.flatbuf.Field.addName(builder, nameOffset);
    org.apache.arrow.flatbuf.Field.addNullable(builder, field.nullable());
    org.apache.arrow.flatbuf.Field.addTypeType(builder, typeType);
    org.apache.arrow.flatbuf.Field.addType(builder, typeOffset);
    org.apache.arrow.flatbuf.Field.addChildren(builder, childrenOffset);
    org.apache.arrow.flatbuf.Field.addLayout(builder, layoutOffset);
    const offset = org.apache.arrow.flatbuf.Field.endField(builder);
    builder.finish(offset);

    return org.apache.arrow.flatbuf.Field.getRootAsField(builder.bb);
}

function Int32FromByteBuffer(bb, offset) {
    return ((bb.bytes_[offset + 3] & 255) << 24) |
           ((bb.bytes_[offset + 2] & 255) << 16) |
           ((bb.bytes_[offset + 1] & 255) << 8) |
           ((bb.bytes_[offset] & 255));
}

const MAGIC_STR = "ARROW1";
const MAGIC = new Uint8Array(MAGIC_STR.length);
for (let i = 0; i < MAGIC_STR.length; i++) {
    MAGIC[i] = MAGIC_STR.charCodeAt(i);
}

function _checkMagic(buf, index) {
    for (let i = 0; i < MAGIC.length; i++) {
        if (MAGIC[i] !== buf[index + i]) {
            return false;
        }
    }
    return true;
}

const TYPEMAP = {};
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

const VECTORTYPEMAP = {};
VECTORTYPEMAP[VectorType.OFFSET]   = "OFFSET";
VECTORTYPEMAP[VectorType.DATA]     = "DATA";
VECTORTYPEMAP[VectorType.VALIDITY] = "VALIDITY";
VECTORTYPEMAP[VectorType.TYPE]     = "TYPE";

function parseField(field) {
    const children = [];
    for (let i = 0; i < field.childrenLength(); i++) {
        children.push(parseField(field.children(i)));
    }

    const layouts = [];
    for (let i = 0; i < field.layoutLength(); i++) {
        layouts.push(VECTORTYPEMAP[field.layout(i).type()]);
    }

    return {
      children,
      layout: layouts,
      name: field.name(),
      nullable: field.nullable(),
      type: TYPEMAP[field.typeType()],
    };
}

function parseSchema(schema) {
    const result = [];
    for (let i = 0, len = schema.fieldsLength(); i < len; i++) {
        result.push(parseField(schema.fields(i)));
    }
    return result;
}

function loadVectors(bb, vectors: Vector[], recordBatch) {
    const indices = { bufferIndex: 0, nodeIndex: 0 };
    for (const vector of vectors) {
        loadVector(bb, vector, recordBatch, indices);
    }
}

/**
 * Loads a vector with data from a batch
 *   recordBatch: { nodes: org.apache.arrow.flatbuf.FieldNode[], buffers: { offset: number, length: number }[] }
 */
function loadVector(bb, vector: Vector, recordBatch, indices) {
    const node = recordBatch.nodes[indices.nodeIndex];
    let ownBuffersLength;
    const ownBuffers = [];
    let i;
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

    const children = vector.getChildVectors();
    for (i = 0; i < children.length; i++) {
        loadVector(bb, children[i], recordBatch, indices);
    }
}
