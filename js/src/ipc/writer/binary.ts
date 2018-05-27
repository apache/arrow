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

import { Table } from '../../table';
import { DenseUnionData } from '../../data';
import { RecordBatch } from '../../recordbatch';
import { VectorVisitor, TypeVisitor } from '../../visitor';
import { MAGIC, magicLength, magicAndPadding, PADDING } from '../magic';
import { align, getBool, packBools, iterateBits } from '../../util/bit';
import { Vector, UnionVector, DictionaryVector, NestedVector, ListVector } from '../../vector';
import { BufferMetadata, FieldMetadata, Footer, FileBlock, Message, RecordBatchMetadata, DictionaryBatch } from '../metadata';
import {
    Schema, Field, TypedArray, MetadataVersion,
    DataType,
    Dictionary,
    Null, Int, Float,
    Binary, Bool, Utf8, Decimal,
    Date_, Time, Timestamp, Interval,
    List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
    FlatType, FlatListType, NestedType, UnionMode, SparseUnion, DenseUnion, SingleNestedType,
} from '../../type';

export function* serializeStream(table: Table) {
    yield serializeMessage(table.schema).buffer;
    for (const [id, field] of table.schema.dictionaries) {
        const vec = table.getColumn(field.name) as DictionaryVector;
        if (vec && vec.dictionary) {
            yield serializeDictionaryBatch(vec.dictionary, id).buffer;
        }
    }
    for (const recordBatch of table.batches) {
        yield serializeRecordBatch(recordBatch).buffer;
    }
}

export function* serializeFile(table: Table) {

    const recordBatches = [];
    const dictionaryBatches = [];

    // First yield the magic string (aligned)
    let buffer = new Uint8Array(align(magicLength, 8));
    let metadataLength, byteLength = buffer.byteLength;
    buffer.set(MAGIC, 0);
    yield buffer;

    // Then yield the schema
    ({ metadataLength, buffer } = serializeMessage(table.schema));
    byteLength += buffer.byteLength;
    yield buffer;

    for (const [id, field] of table.schema.dictionaries) {
        const vec = table.getColumn(field.name) as DictionaryVector;
        if (vec && vec.dictionary) {
            ({ metadataLength, buffer } = serializeDictionaryBatch(vec.dictionary, id));
            dictionaryBatches.push(new FileBlock(metadataLength, buffer.byteLength, byteLength));
            byteLength += buffer.byteLength;
            yield buffer;
        }
    }
    for (const recordBatch of table.batches) {
        ({ metadataLength, buffer } = serializeRecordBatch(recordBatch));
        recordBatches.push(new FileBlock(metadataLength, buffer.byteLength, byteLength));
        byteLength += buffer.byteLength;
        yield buffer;
    }

    // Then yield the footer metadata (not aligned)
    ({ metadataLength, buffer } = serializeFooter(new Footer(dictionaryBatches, recordBatches, table.schema)));
    yield buffer;
    
    // Last, yield the footer length + terminating magic arrow string (aligned)
    buffer = new Uint8Array(magicAndPadding);
    new DataView(buffer.buffer).setInt32(0, metadataLength, platformIsLittleEndian);
    buffer.set(MAGIC, buffer.byteLength - magicLength);
    yield buffer;
}

export function serializeRecordBatch(recordBatch: RecordBatch) {
    const { byteLength, fieldNodes, buffers, buffersMeta } = new RecordBatchSerializer().visitRecordBatch(recordBatch);
    const rbMeta = new RecordBatchMetadata(MetadataVersion.V4, recordBatch.length, fieldNodes, buffersMeta);
    const rbData = concatBuffersWithMetadata(byteLength, buffers, buffersMeta);
    return serializeMessage(rbMeta, rbData);
}

export function serializeDictionaryBatch(dictionary: Vector, id: Long | number, isDelta: boolean = false) {
    const { byteLength, fieldNodes, buffers, buffersMeta } = new RecordBatchSerializer().visitRecordBatch(RecordBatch.from([dictionary]));
    const rbMeta = new RecordBatchMetadata(MetadataVersion.V4, dictionary.length, fieldNodes, buffersMeta);
    const dbMeta = new DictionaryBatch(MetadataVersion.V4, rbMeta, id, isDelta);
    const rbData = concatBuffersWithMetadata(byteLength, buffers, buffersMeta);
    return serializeMessage(dbMeta, rbData);
}

export function serializeMessage(message: Message, data?: Uint8Array) {
    const b = new Builder();
    _Message.finishMessageBuffer(b, writeMessage(b, message));
    // Slice out the buffer that contains the message metadata
    const metadataBytes = b.asUint8Array();
    // Reserve 4 bytes for writing the message size at the front.
    // Metadata length includes the metadata byteLength + the 4
    // bytes for the length, and rounded up to the nearest 8 bytes.
    const metadataLength = align(PADDING + metadataBytes.byteLength, 8);
    // + the length of the optional data buffer at the end, padded
    const dataByteLength = data ? data.byteLength : 0;
    // ensure the entire message is aligned to an 8-byte boundary
    const messageBytes = new Uint8Array(align(metadataLength + dataByteLength, 8));
    // Write the metadata length into the first 4 bytes, but subtract the
    // bytes we use to hold the length itself.
    new DataView(messageBytes.buffer).setInt32(0, metadataLength - PADDING, platformIsLittleEndian);
    // Copy the metadata bytes into the message buffer
    messageBytes.set(metadataBytes, PADDING);
    // Copy the optional data buffer after the metadata bytes
    (data && dataByteLength > 0) && messageBytes.set(data, metadataLength);
    // if (messageBytes.byteLength % 8 !== 0) { debugger; }
    // Return the metadata length because we need to write it into each FileBlock also
    return { metadataLength, buffer: messageBytes };
}

export function serializeFooter(footer: Footer) {
    const b = new Builder();
    _Footer.finishFooterBuffer(b, writeFooter(b, footer));
    // Slice out the buffer that contains the footer metadata
    const footerBytes = b.asUint8Array();
    const metadataLength = footerBytes.byteLength;
    return { metadataLength, buffer: footerBytes };
}

export class RecordBatchSerializer extends VectorVisitor {
    public byteLength = 0;
    public buffers: TypedArray[] = [];
    public fieldNodes: FieldMetadata[] = [];
    public buffersMeta: BufferMetadata[] = [];
    public visitRecordBatch(recordBatch: RecordBatch) {
        this.buffers = [];
        this.byteLength = 0;
        this.fieldNodes = [];
        this.buffersMeta = [];
        for (let vector: Vector, index = -1, numCols = recordBatch.numCols; ++index < numCols;) {
            if (vector = recordBatch.getChildAt(index)!) {
                this.visit(vector);
            }
        }
        return this;
    }
    public visit<T extends DataType>(vector: Vector<T>) {
        if (!DataType.isDictionary(vector.type)) {
            const { data, length, nullCount } = vector;
            if (length > 2147483647) {
                throw new RangeError('Cannot write arrays larger than 2^31 - 1 in length');
            }
            this.fieldNodes.push(new FieldMetadata(length, nullCount));
            this.addBuffer(nullCount <= 0
                ? new Uint8Array(0) // placeholder validity buffer
                : this.getTruncatedBitmap(data.offset, length, data.nullBitmap!)
            );
        }
        return super.visit(vector);
    }
    public visitNull           (_nullz: Vector<Null>)            { return this;                              }
    public visitBool           (vector: Vector<Bool>)            { return this.visitBoolVector(vector);      }
    public visitInt            (vector: Vector<Int>)             { return this.visitFlatVector(vector);      }
    public visitFloat          (vector: Vector<Float>)           { return this.visitFlatVector(vector);      }
    public visitUtf8           (vector: Vector<Utf8>)            { return this.visitFlatListVector(vector);  }
    public visitBinary         (vector: Vector<Binary>)          { return this.visitFlatListVector(vector);  }
    public visitDate           (vector: Vector<Date_>)           { return this.visitFlatVector(vector);      }
    public visitTimestamp      (vector: Vector<Timestamp>)       { return this.visitFlatVector(vector);      }
    public visitTime           (vector: Vector<Time>)            { return this.visitFlatVector(vector);      }
    public visitDecimal        (vector: Vector<Decimal>)         { return this.visitFlatVector(vector);      }
    public visitInterval       (vector: Vector<Interval>)        { return this.visitFlatVector(vector);      }
    public visitList           (vector: Vector<List>)            { return this.visitListVector(vector);      }
    public visitStruct         (vector: Vector<Struct>)          { return this.visitNestedVector(vector);    }
    public visitFixedSizeBinary(vector: Vector<FixedSizeBinary>) { return this.visitFlatVector(vector);      }
    public visitFixedSizeList  (vector: Vector<FixedSizeList>)   { return this.visitListVector(vector);      }
    public visitMap            (vector: Vector<Map_>)            { return this.visitNestedVector(vector);    }
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
    protected visitBoolVector(vector: Vector<Bool>) {
        // Bool vector is a special case of FlatVector, as its data buffer needs to stay packed
        let bitmap: Uint8Array;
        let values, { data, length } = vector;
        if (vector.nullCount >= length) {
            // If all values are null, just insert a placeholder empty data buffer (fastest path)
            bitmap = new Uint8Array(0);
        } else if (!((values = data.values) instanceof Uint8Array)) {
            // Otherwise if the underlying data *isn't* a Uint8Array, enumerate
            // the values as bools and re-pack them into a Uint8Array (slow path)
            bitmap = packBools(vector);
        } else {
            // otherwise just slice the bitmap (fast path)
            bitmap = this.getTruncatedBitmap(data.offset, length, values);
        }
        return this.addBuffer(bitmap);
    }
    protected visitFlatVector<T extends FlatType>(vector: Vector<T>) {
        const { view, data } = vector;
        const { offset, length, values } = data;
        const scaledLength = length * ((view as any).size || 1);
        return this.addBuffer(values.subarray(offset, scaledLength));
    }
    protected visitFlatListVector<T extends FlatListType>(vector: Vector<T>) {
        const { data, length } = vector;
        const { offset, values, valueOffsets } = data;
        const firstOffset = valueOffsets[0];
        const lastOffset = valueOffsets[length];
        const byteLength = Math.min(lastOffset - firstOffset, values.byteLength - firstOffset);
        // Push in the order FlatList types read their buffers
        // valueOffsets buffer first
        this.addBuffer(this.getZeroBasedValueOffsets(offset, length, valueOffsets));
        // sliced values buffer second
        this.addBuffer(values.subarray(firstOffset + offset, firstOffset + offset + byteLength));
        return this;
    }
    protected visitListVector<T extends SingleNestedType>(vector: Vector<T>) {
        const { data, length } = vector;
        const { offset, valueOffsets } = <any> data;
        // If we have valueOffsets (ListVector), push that buffer first
        if (valueOffsets) {
            this.addBuffer(this.getZeroBasedValueOffsets(offset, length, valueOffsets));
        }
        // Then insert the List's values child
        return this.visit((vector as any as ListVector<T>).getChildAt(0)!);
    }
    protected visitNestedVector<T extends NestedType>(vector: Vector<T>) {
        // Visit the children accordingly
        const numChildren = (vector.type.children || []).length;
        for (let child: Vector | null, childIndex = -1; ++childIndex < numChildren;) {
            if (child = (vector as NestedVector<T>).getChildAt(childIndex)) {
                this.visit(child);
            }
        }
        return this;
    }
    protected addBuffer(values: TypedArray) {
        const byteLength = align(values.byteLength, 8);
        this.buffers.push(values);
        this.buffersMeta.push(new BufferMetadata(this.byteLength, byteLength));
        this.byteLength += byteLength;
        return this;
    }
    protected getTruncatedBitmap(offset: number, length: number, bitmap: Uint8Array) {
        const alignedLength = align(bitmap.byteLength, 8);
        if (offset > 0 || bitmap.byteLength < alignedLength) {
            // With a sliced array / non-zero offset, we have to copy the bitmap
            const bytes = new Uint8Array(alignedLength);
            bytes.set(
                (offset % 8 === 0)
                // If the slice offset is aligned to 1 byte, it's safe to slice the nullBitmap directly
                ? bitmap.subarray(offset >> 3)
                // iterate each bit starting from the slice offset, and repack into an aligned nullBitmap
                : packBools(iterateBits(bitmap, offset, length, null, getBool))
            );
            return bytes;
        }
        return bitmap;
    }
    protected getZeroBasedValueOffsets(offset: number, length: number, valueOffsets: Int32Array) {
        // If we have a non-zero offset, then the value offsets do not start at
        // zero. We must a) create a new offsets array with shifted offsets and
        // b) slice the values array accordingly
        if (offset > 0 || valueOffsets[0] !== 0) {
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
        const indexType = this.visit(node.indices);
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

function concatBuffersWithMetadata(totalByteLength: number, buffers: Uint8Array[], buffersMeta: BufferMetadata[]) {
    const data = new Uint8Array(totalByteLength);
    for (let i = -1, n = buffers.length; ++i < n;) {
        const { offset, length } = buffersMeta[i];
        const { buffer, byteOffset, byteLength } = buffers[i];
        const realBufferLength = Math.min(length, byteLength);
        if (realBufferLength > 0) {
            data.set(new Uint8Array(buffer, byteOffset, realBufferLength), offset);
        }
    }
    return data;
}

function writeFooter(b: Builder, node: Footer) {
    let schemaOffset = writeSchema(b, node.schema);
    let recordBatches = (node.recordBatches || []);
    let dictionaryBatches = (node.dictionaryBatches || []);
    let recordBatchesOffset =
        _Footer.startRecordBatchesVector(b, recordBatches.length) ||
            mapReverse(recordBatches, (rb) => writeBlock(b, rb)) &&
        b.endVector();

    let dictionaryBatchesOffset =
        _Footer.startDictionariesVector(b, dictionaryBatches.length) ||
            mapReverse(dictionaryBatches, (db) => writeBlock(b, db)) &&
        b.endVector();

    return (
        _Footer.startFooter(b) ||
        _Footer.addSchema(b, schemaOffset) ||
        _Footer.addVersion(b, node.schema.version) ||
        _Footer.addRecordBatches(b, recordBatchesOffset) ||
        _Footer.addDictionaries(b, dictionaryBatchesOffset) ||
        _Footer.endFooter(b)
    );
}

function writeBlock(b: Builder, node: FileBlock) {
    return _Block.createBlock(b,
        new Long(node.offset, 0),
        node.metaDataLength,
        new Long(node.bodyLength, 0)
    );
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

    let metadata: number | undefined = undefined;
    if (node.metadata && node.metadata.size > 0) {
        metadata = _Schema.createCustomMetadataVector(
            b,
            [...node.metadata].map(([k, v]) => {
                const key = b.createString(`${k}`);
                const val = b.createString(`${v}`);
                return (
                    _KeyValue.startKeyValue(b) ||
                    _KeyValue.addKey(b, key) ||
                    _KeyValue.addValue(b, val) ||
                    _KeyValue.endKeyValue(b)
                );
            })
        );
    }

    return (
        _Schema.startSchema(b) ||
        _Schema.addFields(b, fieldsOffset) ||
        _Schema.addEndianness(b, platformIsLittleEndian ? _Endianness.Little : _Endianness.Big) ||
        (metadata !== undefined && _Schema.addCustomMetadata(b, metadata)) ||
        _Schema.endSchema(b)
    );
}

function writeRecordBatch(b: Builder, node: RecordBatchMetadata) {
    let nodes = (node.nodes || []);
    let buffers = (node.buffers || []);
    let nodesOffset =
        _RecordBatch.startNodesVector(b, nodes.length) ||
        mapReverse(nodes, (n) => writeFieldNode(b, n)) &&
        b.endVector();

    let buffersOffset =
        _RecordBatch.startBuffersVector(b, buffers.length) ||
        mapReverse(buffers, (b_) => writeBuffer(b, b_)) &&
        b.endVector();

    return (
        _RecordBatch.startRecordBatch(b) ||
        _RecordBatch.addLength(b, new Long(node.length, 0)) ||
        _RecordBatch.addNodes(b, nodesOffset) ||
        _RecordBatch.addBuffers(b, buffersOffset) ||
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
    let typeOffset = -1;
    let type = node.type;
    let typeId = node.typeId;
    let name: number | undefined = undefined;
    let metadata: number | undefined = undefined;
    let dictionary: number | undefined = undefined;

    if (!DataType.isDictionary(type)) {
        typeOffset = new TypeSerializer(b).visit(type);
    } else {
        typeId = type.dictionary.TType;
        dictionary = new TypeSerializer(b).visit(type);
        typeOffset = new TypeSerializer(b).visit(type.dictionary);
    }

    let children = _Field.createChildrenVector(b, (type.children || []).map((f) => writeField(b, f)));
    if (node.metadata && node.metadata.size > 0) {
        metadata = _Field.createCustomMetadataVector(
            b,
            [...node.metadata].map(([k, v]) => {
                const key = b.createString(`${k}`);
                const val = b.createString(`${v}`);
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
        _Field.addTypeType(b, typeId) ||
        _Field.addChildren(b, children) ||
        _Field.addNullable(b, !!node.nullable) ||
        (name !== undefined && _Field.addName(b, name)) ||
        (dictionary !== undefined && _Field.addDictionary(b, dictionary)) ||
        (metadata !== undefined && _Field.addCustomMetadata(b, metadata)) ||
        _Field.endField(b)
    );
}

function mapReverse<T, U>(source: T[], callbackfn: (value: T, index: number, array: T[]) => U): U[] {
    const result = new Array(source.length);
    for (let i = -1, j = source.length; --j > -1;) {
        result[i] = callbackfn(source[j], i, source);
    }
    return result;
}

const platformIsLittleEndian = (function() {
    const buffer = new ArrayBuffer(2);
    new DataView(buffer).setInt16(0, 256, true /* littleEndian */);
    // Int16Array uses the platform's endianness.
    return new Int16Array(buffer)[0] === 256;
})();
