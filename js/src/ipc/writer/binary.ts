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

import { DenseUnionData } from '../../data';
import { RecordBatch } from '../../recordbatch';
import { VectorVisitor, TypeVisitor } from '../../visitor';
import { align, getBool, packBools, iterateBits } from '../../util/bit';
import { Vector, UnionVector, DictionaryVector, NestedVector } from '../../vector';
import { BufferMetadata, FieldMetadata, Footer, FileBlock, Message, RecordBatchMetadata, DictionaryBatch } from '../metadata';
import {
    Schema, Field, TypedArray, MetadataVersion,
    Dictionary,
    Null, Int, Float,
    Binary, Bool, Utf8, Decimal,
    Date_, Time, Timestamp, Interval,
    List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
    UnionMode, SparseUnion, DenseUnion, FlatListType, DataType, FlatType, NestedType,
} from '../../type';

export class RecordBatchSerializer extends VectorVisitor {
    protected byteLength = 0;
    // @ts-ignore
    protected buffers: TypedArray[];
    // @ts-ignore
    protected fieldNodes: FieldMetadata[];
    // @ts-ignore
    protected buffersMeta: BufferMetadata[];
    public writeRecordBatch(recordBatch: RecordBatch) {
        this.byteLength = 0;
        this.buffers = [];
        this.fieldNodes = [];
        this.buffersMeta = [];
        for (let vector: Vector, index = -1, numCols = recordBatch.numCols; ++index < numCols;) {
            if (vector = recordBatch.getChildAt(index)!) {
                this.visit(vector);
            }
        }
        const b = new Builder();
        _Message.finishMessageBuffer(
            b, writeMessage(b, new RecordBatchMetadata(
                MetadataVersion.V4, this.byteLength, this.fieldNodes, this.buffersMeta
            ))
        );
        const metadataBytes = b.asUint8Array();
        // 4 bytes for the metadata length + 4 bytes of padding + the length of the metadata buffer
        const metadataBytesOffset = 8 + metadataBytes.byteLength;
        // + the length of all the vector buffers
        const recordBatchBytes = new Uint8Array(metadataBytesOffset + this.byteLength);
        // Write the metadata length as the first 4 bytes
        new DataView(recordBatchBytes.buffer).setInt32(0, metadataBytes.byteLength);
        // Now write the buffers
        const { buffers, buffersMeta } = this;
        for (let bufferIndex = -1, buffersLen = buffers.length; ++bufferIndex < buffersLen;) {
            const { buffer, byteLength } = buffers[bufferIndex];
            const { offset: byteOffset } = buffersMeta[bufferIndex];
            recordBatchBytes.set(
                new Uint8Array(buffer, 0, byteLength),
                metadataBytesOffset + byteOffset
            );
        }
        return recordBatchBytes;
    }
    public visit<T extends DataType>(vector: Vector<T>) {
        const { data, length, nullCount } = vector;
        if (length > 2147483647) {
            throw new RangeError('Cannot write arrays larger than 2^31 - 1 in length');
        }
        this.fieldNodes.push(new FieldMetadata(length, nullCount));
        this.addBuffer(nullCount <= 0
            ? new Uint8Array(0) // placeholder validity buffer
            : this.getTruncatedBitmap(data.offset, length, data.nullBitmap!), 64);
        return super.visit(vector);
    }
    public visitNull           (_vector: Vector<Null>)           { return this;                              }
    public visitBool           (vector: Vector<Bool>)            { return this.visitFlatVector(vector);      }
    public visitInt            (vector: Vector<Int>)             { return this.visitFlatVector(vector);      }
    public visitFloat          (vector: Vector<Float>)           { return this.visitFlatVector(vector);      }
    public visitUtf8           (vector: Vector<Utf8>)            { return this.visitFlatListVector(vector);  }
    public visitBinary         (vector: Vector<Binary>)          { return this.visitFlatListVector(vector);  }
    public visitFixedSizeBinary(vector: Vector<FixedSizeBinary>) { return this.visitFlatVector(vector);      }
    public visitDate           (vector: Vector<Date_>)           { return this.visitFlatVector(vector);      }
    public visitTimestamp      (vector: Vector<Timestamp>)       { return this.visitFlatVector(vector);      }
    public visitTime           (vector: Vector<Time>)            { return this.visitFlatVector(vector);      }
    public visitDecimal        (vector: Vector<Decimal>)         { return this.visitFlatVector(vector);      }
    public visitInterval       (vector: Vector<Interval>)        { return this.visitFlatVector(vector);      }
    public visitStruct         (vector: Vector<Struct>)          { return this.visitNestedVector(vector);    }
    public visitMap            (vector: Vector<Map_>)            { return this.visitNestedVector(vector);    }
    public visitFixedSizeList  (vector: Vector<FixedSizeList>)   { return this.visitNestedVector(vector, 1); }
    public visitList           (vector: Vector<List>)            { return this.visitNestedVector(vector, 1); }
    public visitDictionary     (vector: DictionaryVector)        {
        // Dictionary written out separately. Slice offset contained in the indices
        return this.visit(vector.indices);
    }
    public visitUnion(vector: Vector<DenseUnion | SparseUnion>) {
        const { data, type, length } = vector;
        const { offset: sliceOffset, typeIds } = data;
        // All Union Vectors have a typeIds buffer
        this.addBuffer(typeIds);
        // If this is a Sparse Union, treat it like all other Nested types
        if (type.mode === UnionMode.Sparse) {
            return this.visitNestedVector(vector);
        } else if (type.mode === UnionMode.Dense) {
            // If this is a Dense Union, add the valueOffsets buffer and potentially slice the children
            const valueOffsets = (data as DenseUnionData).valueOffsets;
            if (sliceOffset <= 0) {
                // If the Vector hasn't been sliced, write the existing valueOffsets
                this.addBuffer(valueOffsets);
                // We can treat this like all other Nested types
                return this.visitNestedVector(vector);
            } else {
                // A sliced Dense Union is an unpleasant case. Because the offsets are different for
                // each child vector, we need to "rebase" the valueOffsets for each child
                // Union typeIds are not necessary 0-indexed
                const maxChildTypeId = Math.max(...type.typeIds);
                const childLengths = new Int32Array(maxChildTypeId + 1);
                // Set all to -1 to indicate that we haven't observed a first occurrence of a particular child yet
                const childOffsets = new Int32Array(maxChildTypeId + 1).fill(-1);
                const shiftedOffsets = new Int32Array(length);
                const unshiftedOffsets = this.getZeroBasedValueOffsets(sliceOffset, length, valueOffsets);
                for (let typeId, shift, index = -1; ++index < length;) {
                    typeId = typeIds[index];
                    // ~(-1) used to be faster than x === -1, so maybe worth benchmarking the difference of these two impls for large dense unions:
                    // ~(shift = childOffsets[typeId]) || (shift = childOffsets[typeId] = unshiftedOffsets[index]);
                    // Going with this form for now, as it's more readable
                    if ((shift = childOffsets[typeId]) === -1) {
                        shift = childOffsets[typeId] = unshiftedOffsets[typeId];
                    }
                    shiftedOffsets[index] = unshiftedOffsets[index] - shift;
                    ++childLengths[typeId];
                }
                this.addBuffer(shiftedOffsets);
                // Slice and visit children accordingly
                for (let childIndex = -1, numChildren = type.children.length; ++childIndex < numChildren;) {
                    const typeId = type.typeIds[childIndex];
                    const child = (vector as UnionVector).getChildAt(childIndex)!;
                    this.visit(child.slice(childOffsets[typeId], Math.min(length, childLengths[typeId])));
                }
            }
        }
        return this;
    }
    protected visitFlatVector<T extends FlatType>(vector: Vector<T>) {
        // Use the TypedArray constructor defined by the Vector's DataType
        const { ArrayType } = vector.type;
        // Use `vector.toArray()` here, since the View currently applied to the Vector might iterate different values
        // than are in the original Data's values buffer.
        // An example is a TimestampVector[ns] that's been converted to an Int32Vector via `asEpochMilliseconds()`.
        // If the Vector's View is a ValidityView, the null slots will be automatically coerced by the TypedArray
        // constructor, e.g. `0` for an Int8Array, `NaN` for Float64Array, etc.
        this.addBuffer(new ArrayType(vector.toArray()));
        return this;
    }
    protected visitFlatListVector<T extends FlatListType>(vector: Vector<T>) {
        const { data, length } = vector;
        const sliceOffset = data.offset;
        const { values, valueOffsets } = data;
        const firstOffset = valueOffsets[0];
        const lastOffset = valueOffsets[valueOffsets.length - 1];
        const byteLength = Math.min(align(lastOffset - firstOffset, 8), values.byteLength - firstOffset);
        // Push in the order of FlatList types
        // valueOffsets buffer first
        this.addBuffer(this.getZeroBasedValueOffsets(sliceOffset, length, valueOffsets));
        // sliced values buffer second
        this.addBuffer(values.subarray(firstOffset + sliceOffset, firstOffset + sliceOffset + byteLength));
        return this;
    }
    protected visitNestedVector<T extends NestedType>(vector: Vector<T>, numChildren = vector.type.children.length) {
        // Visit the children accordingly
        for (let childIndex = -1; ++childIndex < numChildren;) {
            this.visit((vector as NestedVector<T>).getChildAt(childIndex)!);
        }
        return this;
    }
    protected addBuffer(array?: TypedArray | null, alignment: number = 8) {
        const values = array || new Uint8Array(0);
        const alignedLength = align(values.byteLength, alignment);
        this.buffers.push(values);
        this.buffersMeta.push(new BufferMetadata(this.byteLength, alignedLength));
        this.byteLength += alignedLength;
    }
    protected getTruncatedBitmap(sliceOffset: number, length: number, nullBitmap: Uint8Array) {
        const alignedLength = align(length, 64);
        if (sliceOffset > 0 || length < alignedLength) {
            // With a sliced array / non-zero offset, we have to copy the bitmap
            const bytes = new Uint8Array(alignedLength).fill(255);
            bytes.set(
                (sliceOffset % 8 === 0)
                // If the sliceOffset is aligned to 1 byte, it's safe to slice the nullBitmap directly
                ? nullBitmap.subarray(sliceOffset >> 3)
                // iterate each bit starting from the sliceOffset, and repack into an aligned nullBitmap
                : packBools(iterateBits(nullBitmap, sliceOffset, length, null, getBool))
            );
            return bytes;
        }
        return nullBitmap;
    }
    protected getZeroBasedValueOffsets(sliceOffset: number, length: number, valueOffsets: Int32Array) {
        // If we have a non-zero offset, then the value offsets do not start at
        // zero. We must a) create a new offsets array with shifted offsets and
        // b) slice the values array accordingly
        if (sliceOffset > 0 || valueOffsets[0] !== 0) {
            const startOffset = valueOffsets[0];
            const destOffsets = new Int32Array(length + 1);
            for (let index = -1; ++index < length;) {
                destOffsets[index] = valueOffsets[index] - startOffset;
            }
            // Final offset
            destOffsets[length] = valueOffsets[length] - startOffset;
            return destOffsets;
        }
        return valueOffsets;
    }
}

import { flatbuffers } from 'flatbuffers';
import Long = flatbuffers.Long;
import Builder = flatbuffers.Builder;
import * as File_ from '../../fb/File';
import * as Schema_ from '../../fb/Schema';
import * as Message_ from '../../fb/Message';

import _Block = File_.org.apache.arrow.flatbuf.Block;
import _Footer = File_.org.apache.arrow.flatbuf.Footer;
import _Field = Schema_.org.apache.arrow.flatbuf.Field;
import _Schema = Schema_.org.apache.arrow.flatbuf.Schema;
import _Buffer = Schema_.org.apache.arrow.flatbuf.Buffer;
import _Message = Message_.org.apache.arrow.flatbuf.Message;
import _KeyValue = Schema_.org.apache.arrow.flatbuf.KeyValue;
import _FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;
import _RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;
import _DictionaryBatch = Message_.org.apache.arrow.flatbuf.DictionaryBatch;
import _DictionaryEncoding = Schema_.org.apache.arrow.flatbuf.DictionaryEncoding;
import _Endianness = Schema_.org.apache.arrow.flatbuf.Endianness;

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

export class TypeSerializer extends TypeVisitor {
    constructor(protected builder: Builder) {
        super();
    }
    public visitNull(_node: Null) {
        const b = this.builder;
        return (
            _Null.startNull(b) ||
            _Null.endNull(b)
        );
    }
    public visitInt(node: Int) {
        const b = this.builder;
        return (
            _Int.startInt(b) ||
            _Int.addBitWidth(b, node.bitWidth) ||
            _Int.addIsSigned(b, node.isSigned) ||
            _Int.endInt(b)
        );
    }
    public visitFloat(node: Float) {
        const b = this.builder;
        return (
            _FloatingPoint.startFloatingPoint(b) ||
            _FloatingPoint.addPrecision(b, node.precision) ||
            _FloatingPoint.endFloatingPoint(b)
        );
    }
    public visitBinary(_node: Binary) {
        const b = this.builder;
        return (
            _Binary.startBinary(b) ||
            _Binary.endBinary(b)
        );
    }
    public visitBool(_node: Bool) {
        const b = this.builder;
        return (
            _Bool.startBool(b) ||
            _Bool.endBool(b)
        );
    }
    public visitUtf8(_node: Utf8) {
        const b = this.builder;
        return (
            _Utf8.startUtf8(b) ||
            _Utf8.endUtf8(b)
        );
    }
    public visitDecimal(node: Decimal) {
        const b = this.builder;
        return (
            _Decimal.startDecimal(b) ||
            _Decimal.addScale(b, node.scale) ||
            _Decimal.addPrecision(b, node.precision) ||
            _Decimal.endDecimal(b)
        );
    }
    public visitDate(node: Date_) {
        const b = this.builder;
        return _Date.startDate(b) || _Date.addUnit(b, node.unit) || _Date.endDate(b);
    }
    public visitTime(node: Time) {
        const b = this.builder;
        return (
            _Time.startTime(b) ||
            _Time.addUnit(b, node.unit) ||
            _Time.addBitWidth(b, node.bitWidth) ||
            _Time.endTime(b)
        );
    }
    public visitTimestamp(node: Timestamp) {
        const b = this.builder;
        const timezone = (node.timezone && b.createString(node.timezone)) || undefined;
        return (
            _Timestamp.startTimestamp(b) ||
            _Timestamp.addUnit(b, node.unit) ||
            (timezone !== undefined && _Timestamp.addTimezone(b, timezone)) ||
            _Timestamp.endTimestamp(b)
        );
    }
    public visitInterval(node: Interval) {
        const b = this.builder;
        return (
            _Interval.startInterval(b) || _Interval.addUnit(b, node.unit) || _Interval.endInterval(b)
        );
    }
    public visitList(_node: List) {
        const b = this.builder;
        return (
            _List.startList(b) ||
            _List.endList(b)
        );
    }
    public visitStruct(_node: Struct) {
        const b = this.builder;
        return (
            _Struct.startStruct_(b) ||
            _Struct.endStruct_(b)
        );
    }
    public visitUnion(node: Union) {
        const b = this.builder;
        const typeIds =
            _Union.startTypeIdsVector(b, node.typeIds.length) ||
            _Union.createTypeIdsVector(b, node.typeIds);
        return (
            _Union.startUnion(b) ||
            _Union.addMode(b, node.mode) ||
            _Union.addTypeIds(b, typeIds) ||
            _Union.endUnion(b)
        );
    }
    public visitDictionary(node: Dictionary) {
        const b = this.builder;
        const indexType = this.visit(node.indicies);
        return (
            _DictionaryEncoding.startDictionaryEncoding(b) ||
            _DictionaryEncoding.addId(b, new Long(node.id, 0)) ||
            _DictionaryEncoding.addIsOrdered(b, node.isOrdered) ||
            (indexType !== undefined && _DictionaryEncoding.addIndexType(b, indexType)) ||
            _DictionaryEncoding.endDictionaryEncoding(b)
        );
    }
    public visitFixedSizeBinary(node: FixedSizeBinary) {
        const b = this.builder;
        return (
            _FixedSizeBinary.startFixedSizeBinary(b) ||
            _FixedSizeBinary.addByteWidth(b, node.byteWidth) ||
            _FixedSizeBinary.endFixedSizeBinary(b)
        );
    }
    public visitFixedSizeList(node: FixedSizeList) {
        const b = this.builder;
        return (
            _FixedSizeList.startFixedSizeList(b) ||
            _FixedSizeList.addListSize(b, node.listSize) ||
            _FixedSizeList.endFixedSizeList(b)
        );
    }
    public visitMap(node: Map_) {
        const b = this.builder;
        return (
            _Map.startMap(b) ||
            _Map.addKeysSorted(b, node.keysSorted) ||
            _Map.endMap(b)
        );
    }
}

// @ts-ignore
function writeFooter(b: Builder, node: Footer) {
    let schemaOffset = writeSchema(b, node.schema);
    let recordBatchesOffset: number | undefined = undefined;
    let dictionaryBatchesOffset: number | undefined = undefined;
    if (node.recordBatches && node.recordBatches.length) {
        recordBatchesOffset =
            _Footer.startRecordBatchesVector(b, node.recordBatches.length) ||
            node.recordBatches.map((rb) => writeBlock(b, rb)) &&
            b.endVector();
    }
    if (node.dictionaryBatches && node.dictionaryBatches.length) {
        dictionaryBatchesOffset =
            _Footer.startDictionariesVector(b, node.dictionaryBatches.length) ||
            node.dictionaryBatches.map((db) => writeBlock(b, db)) &&
            b.endVector();
    }
    return (
        _Footer.startFooter(b) ||
        _Footer.addSchema(b, schemaOffset) ||
        _Footer.addVersion(b, node.schema.version) ||
        (recordBatchesOffset !== undefined && _Footer.addRecordBatches(b, recordBatchesOffset)) ||
        (dictionaryBatchesOffset !== undefined && _Footer.addDictionaries(b, dictionaryBatchesOffset)) ||
        _Footer.endFooter(b)
    );
}

function writeBlock(b: Builder, node: FileBlock) {
    return _Block.createBlock(b, node.offset, node.metaDataLength, node.bodyLength);
}

function writeMessage(b: Builder, node: Message) {
    let messageHeaderOffset = 0;
    if (Message.isSchema(node)) {
        messageHeaderOffset = writeSchema(b, node as Schema);
    } else if (Message.isRecordBatch(node)) {
        messageHeaderOffset = writeRecordBatch(b, node as RecordBatchMetadata);
    } else if (Message.isDictionaryBatch(node)) {
        messageHeaderOffset = writeDictionaryBatch(b, node as DictionaryBatch);
    }
    return (
        _Message.startMessage(b) ||
        _Message.addVersion(b, node.version) ||
        _Message.addHeader(b, messageHeaderOffset) ||
        _Message.addHeaderType(b, node.headerType) ||
        _Message.addBodyLength(b, new Long(node.bodyLength, 0)) ||
        _Message.endMessage(b)
    );
}

function writeSchema(b: Builder, node: Schema) {
    const fieldOffsets = node.fields.map((f) => writeField(b, f));
    const fieldsOffset =
        _Schema.startFieldsVector(b, fieldOffsets.length) ||
        _Schema.createFieldsVector(b, fieldOffsets);
    return (
        _Schema.startSchema(b) ||
        _Schema.addFields(b, fieldsOffset) ||
        _Schema.addEndianness(b, _Endianness.Little) ||
        _Schema.endSchema(b)
    );
}

function writeRecordBatch(b: Builder, node: RecordBatchMetadata) {
    let nodesOffset: number | undefined = undefined;
    let buffersOffset: number | undefined = undefined;
    if (node.nodes && node.nodes.length) {
        nodesOffset =
            _RecordBatch.startNodesVector(b, node.nodes.length) ||
            node.nodes.map((n) => writeFieldNode(b, n)) &&
            b.endVector();
    }
    if (node.buffers && node.buffers.length) {
        buffersOffset =
            _RecordBatch.startBuffersVector(b, node.buffers.length) ||
            node.buffers.map((bm) => writeBuffer(b, bm)) &&
            b.endVector();
    }
    return (
        _RecordBatch.startRecordBatch(b) ||
        _RecordBatch.addLength(b, new Long(node.length, 0)) ||
        (nodesOffset !== undefined && _RecordBatch.addNodes(b, nodesOffset)) ||
        (buffersOffset !== undefined && _RecordBatch.addBuffers(b, buffersOffset)) ||
        _RecordBatch.endRecordBatch(b)
    );
}

function writeDictionaryBatch(b: Builder, node: DictionaryBatch) {
    const dataOffset = writeRecordBatch(b, node.data);
    return (
        _DictionaryBatch.startDictionaryBatch(b) ||
        _DictionaryBatch.addId(b, new Long(node.id, 0)) ||
        _DictionaryBatch.addIsDelta(b, node.isDelta) ||
        _DictionaryBatch.addData(b, dataOffset) ||
        _DictionaryBatch.endDictionaryBatch(b)
    );
}

function writeBuffer(b: Builder, node: BufferMetadata) {
    return _Buffer.createBuffer(b, new Long(node.offset, 0), new Long(node.length, 0));
}

function writeFieldNode(b: Builder, node: FieldMetadata) {
    return _FieldNode.createFieldNode(b, new Long(node.length, 0), new Long(node.nullCount, 0));
}

function writeField(b: Builder, node: Field) {
    let type = node.type;
    let typeOffset = new TypeSerializer(b).visit(type);
    let name: number | undefined = undefined;
    let children: number | undefined = undefined;
    let metadata: number | undefined = undefined;
    let dictionary: number | undefined = undefined;
    if (DataType.isDictionary(node.type)) {
        dictionary = new TypeSerializer(b).visit(node.type.dictionary);
    }
    if (type.children && type.children.length) {
        children = _Field.createChildrenVector(b, type.children.map((f) => writeField(b, f)));
    }
    if (node.metadata) {
        metadata = _Field.createCustomMetadataVector(
            b,
            [...node.metadata].map(([k, v]) => {
                const key = b.createString(k);
                const val = b.createString(v);
                return (
                    _KeyValue.startKeyValue(b) ||
                    _KeyValue.addKey(b, key) ||
                    _KeyValue.addValue(b, val) ||
                    _KeyValue.endKeyValue(b)
                );
            })
        );
    }
    if (node.name) {
        name = b.createString(node.name);
    }
    return (
        _Field.startField(b) ||
        _Field.addType(b, typeOffset) ||
        _Field.addTypeType(b, node.typeId) ||
        _Field.addNullable(b, !!node.nullable) ||
        (name !== undefined && _Field.addName(b, name)) ||
        (children !== undefined && _Field.addChildren(b, children)) ||
        (dictionary !== undefined && _Field.addDictionary(b, dictionary)) ||
        (metadata !== undefined && _Field.addCustomMetadata(b, metadata)) ||
        _Field.endField(b)
    );
}
