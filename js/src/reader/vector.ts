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
import { MessageBatch } from './message';
import { Vector } from '../vector/vector';
import * as Schema_ from '../format/Schema_generated';
import { StructVector } from '../vector/struct';
import { IteratorState, Dictionaries } from './arrow';
import { DictionaryVector } from '../vector/dictionary';
import { Utf8Vector, ListVector, FixedSizeListVector } from '../vector/list';
import {
    TypedArray, TypedArrayCtor, IntArray, FloatArray,
    Int8Vector, Int16Vector, Int32Vector, Int64Vector,
    Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector,
    Float32Vector, Float64Vector, IndexVector, DateVector,
} from '../vector/typed';

import Int = Schema_.org.apache.arrow.flatbuf.Int;
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import Field = Schema_.org.apache.arrow.flatbuf.Field;
import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
import VectorType = Schema_.org.apache.arrow.flatbuf.VectorType;
import VectorLayout = Schema_.org.apache.arrow.flatbuf.VectorLayout;
import FixedSizeList = Schema_.org.apache.arrow.flatbuf.FixedSizeList;
import FloatingPoint = Schema_.org.apache.arrow.flatbuf.FloatingPoint;
import DictionaryEncoding = Schema_.org.apache.arrow.flatbuf.DictionaryEncoding;

export function readVector(field: Field, batch: MessageBatch, state: IteratorState, dictionaries: Dictionaries) {
    return readDictionaryVector(field, batch, state, dictionaries) ||
                readTypedVector(field, batch, state, dictionaries);
}

function readTypedVector(field: Field, batch: MessageBatch, iterator: IteratorState, dictionaries: Dictionaries) {
    let typeType = field.typeType(), readTyped = typedVectorReaders[typeType];
    if (!readTyped) {
        throw new Error('Unrecognized vector name "' + Type[typeType] + '" type "' + typeType + '"');
    }
    return readTyped(field, batch, iterator, dictionaries);
}

function readDictionaryVector(field: Field, batch: MessageBatch, iterator: IteratorState, dictionaries: Dictionaries) {
    let encoding: DictionaryEncoding;
    if (dictionaries && (encoding = field.dictionary())) {
        let id = encoding.id().toFloat64().toString();
        let fieldType =  encoding.indexType() ||
            /* a dictionary index defaults to signed 32 bit int if unspecified */
            { bitWidth: () => 32, isSigned: () => true };
        let indexField = createSyntheticDictionaryIndexField(field, fieldType);
        let index = readIntVector(indexField, batch, iterator, null, fieldType);
        return DictionaryVector.create(field, index.length, index, dictionaries[id]);
    }
}

const IntViews    = [Int8Array,    Int16Array,   Int32Array,   Int32Array  ];
const Int32Views  = [Int32Array,   Int32Array,   Int32Array,   Int32Array  ];
const UintViews   = [Uint8Array,   Uint16Array,  Uint32Array,  Uint32Array ];
const Uint8Views  = [Uint8Array,   Uint8Array,   Uint8Array,   Uint8Array  ];
const Uint32Views = [Uint32Array,  Uint32Array,  Uint32Array,  Uint32Array ];
const FloatViews  = [Int8Array,    Int16Array,   Float32Array, Float64Array];

const createIntDataViews = createDataView.bind(null, IntViews, null);
const createUintDataViews = createDataView.bind(null, UintViews, null);
const createDateDataViews = createDataView.bind(null, Uint32Views, null);
const createFloatDataViews = createDataView.bind(null, FloatViews, null);
const createNestedDataViews = createDataView.bind(null, Uint32Views, null);
const createValidityDataViews = createDataView.bind(null, Uint8Views, null);
const createUtf8DataViews = createDataView.bind(null, Uint8Views, Int32Views);

const floatVectors = {
    [Precision.SINGLE]: Float32Vector,
    [Precision.DOUBLE]: Float64Vector
};
const intVectors = [
    [/* unsigned */ Uint8Vector,   /* signed */ Int8Vector ],
    [/* unsigned */ Uint16Vector,  /* signed */ Int16Vector],
    [/* unsigned */ Uint32Vector,  /* signed */ Int32Vector],
    [/* unsigned */ Uint64Vector,  /* signed */ Int64Vector]
];

function readIntVector(field: Field, batch: MessageBatch, iterator: IteratorState, dictionaries: Dictionaries, fieldType?: FieldType) {
    let type = (fieldType || field.type(new Int()));
    return type.isSigned() ?
        read_IntVector(field, batch, iterator, dictionaries, type) :
        readUintVector(field, batch, iterator, dictionaries, type);
}

const read_IntVector = readVectorLayout<number, IntArray>(createIntDataViews, createIntVector);
const readUintVector = readVectorLayout<number, IntArray>(createUintDataViews, createIntVector);
function createIntVector(field, length, data, validity, offsets, fieldType, batch, iterator, dictionaries) {
    let type = fieldType || field.type(new Int()), bitWidth = type.bitWidth();
    let Vector = valueForBitWidth(bitWidth, intVectors)[+type.isSigned()];
    return Vector.create(field, length, validity, data || offsets);
    // ---------------------- so this is kinda strange 👆:
    // The dictionary encoded vectors I generated from sample mapd-core queries have the indicies' data buffers
    // tagged as VectorType.OFFSET (0) in the field metadata. The current TS impl ignores buffers' layout type,
    // and assumes the second buffer is the data for a NullableIntVector. Since we've been stricter about enforcing
    // the Arrow spec while parsing, the IntVector's data buffer reads empty in this case. If so, fallback to using
    // the offsets buffer as the data, since IntVectors don't have offsets.
}

const readFloatVector = readVectorLayout<number, FloatArray>(
    createFloatDataViews,
    (field, length, data, validity, offsets, fieldType, batch, iterator, dictionaries) => {
        let type = field.type(new FloatingPoint());
        let Vector = floatVectors[type.precision()];
        return Vector.create(field, length, validity, data);
    }
);

const readDateVector = readVectorLayout<Date, Uint32Array>(
    createDateDataViews,
    (field, length, data, validity, offsets, fieldType, batch, iterator, dictionaries) => {
        return DateVector.create(field, length, validity, data);
    }
);

const readUtf8Vector = readVectorLayout<string, Uint8Array>(
    createUtf8DataViews,
    (field, length, data, validity, offsets, fieldType, batch, iterator, dictionaries) => {
        let offsetsAdjusted = new Int32Array(offsets.buffer, offsets.byteOffset, length + 1);
        return Utf8Vector.create(
            field, length, validity,
            Uint8Vector.create(field, data.length, null, data),
            IndexVector.create(field, length + 1, null, offsetsAdjusted)
        );
    }
);

const readListVector = readVectorLayout<any[], Uint32Array>(
    createNestedDataViews,
    (field, length, data, validity, offsets, fieldType, batch, iterator, dictionaries) => {
        let offsetsAdjusted = new Int32Array(offsets.buffer, offsets.byteOffset, length + 1);
        return ListVector.create(
            field, length, validity,
            readVector(field.children(0), batch, iterator, dictionaries),
            IndexVector.create(field, length + 1, null, offsetsAdjusted)
        );
    }
);

const readFixedSizeListVector = readVectorLayout<any[], Uint32Array>(
    createNestedDataViews,
    (field, length, data, validity, offsets, fieldType, batch, iterator, dictionaries) => {
        let size = field.type(new FixedSizeList()).listSize();
        return FixedSizeListVector.create(
            field, length, size, validity,
            readVector(field.children(0), batch, iterator, dictionaries)
        );
    }
);

const readStructVector = readVectorLayout<any[], ArrayLike<any>>(
    createNestedDataViews,
    (field, length, data, validity, offsets, fieldType, batch, iterator, dictionaries) => {
        let vectors: Vector<any>[] = [];
        for (let i = -1, n = field.childrenLength(); ++i < n;) {
            vectors[i] = readVector(field.children(i), batch, iterator, dictionaries);
        }
        return StructVector.create(field, length, validity, ...vectors);
    }
);

const typedVectorReaders = {
    [Type.Int]: readIntVector,
    [Type.Date]: readDateVector,
    [Type.List]: readListVector,
    [Type.Utf8]: readUtf8Vector,
    [Type.Struct_]: readStructVector,
    [Type.FloatingPoint]: readFloatVector,
    [Type.FixedSizeList]: readFixedSizeListVector,
};

type FieldType = { bitWidth(): number; isSigned(): boolean };
type dataViewFactory<V = TypedArray> = (batch: MessageBatch, type: VectorType, bitWidth: number, offset: number, length: number) => V;
type vectorFactory<TList, V = Vector<any>> = (field: Field,
                                              length: number,
                                              data: TList,
                                              nulls: Uint8Array,
                                              offsets: TypedArray,
                                              fieldType: FieldType,
                                              chunk: MessageBatch,
                                              iterable: IteratorState,
                                              dictionaries: Dictionaries) => V;

function readVectorLayout<T, TList>(createDataView: dataViewFactory<TList>, createVector: vectorFactory<TList, Vector<T>>) {
    return function readLayout(
            field: Field,
            chunk: MessageBatch,
            iterator: IteratorState,
            dictionaries: Dictionaries,
            integerFieldType?: FieldType
    ) {
        let batch = chunk.data;
        let layoutLength = field.layoutLength();
        let node = batch.nodes(iterator.nodeIndex++);
        let data: TList, offsets: any, validity: Uint8Array;
        let type, bitWidth, bufferLength, nodeLength = node.length().low;
        for (let i = -1; ++i < layoutLength;) {
            let layout = field.layout(i);
            let buffer = batch.buffers(iterator.bufferIndex++);
            if ((type = layout.type()) === VectorType.TYPE ||
                (bufferLength = buffer.length().low) <= 0  ||
                (bitWidth = layout.bitWidth()) <= 0) {
                continue;
            } else if (type === VectorType.DATA) {
                data = createDataView(chunk, type, bitWidth, buffer.offset().low, bufferLength);
            } else if (type === VectorType.OFFSET) {
                offsets = createDataView(chunk, type, bitWidth, buffer.offset().low, bufferLength);
            } else if (node.nullCount().low > 0) {
                validity = createValidityDataViews(chunk, type, bitWidth, buffer.offset().low, nodeLength);
            }
        }
        return createVector(field, nodeLength, data, validity, offsets, integerFieldType, chunk, iterator, dictionaries);
    };
}

function createDataView(
    dataViews: TypedArrayCtor<any>[], offsetViews: TypedArrayCtor<any>[] | null,
    batch: MessageBatch, type: VectorType, bitWidth: number, offset: number, length: number
) {
    const buffer = batch.bytes.buffer;
    const byteLength = buffer.byteLength;
    const byteOffset = batch.offset + offset;
    const DataViewType = valueForBitWidth(bitWidth, type === VectorType.OFFSET && offsetViews || dataViews);
    const dataViewLength = ((byteOffset + length) <= byteLength
        ? length
        : byteLength - byteOffset
    ) / DataViewType['BYTES_PER_ELEMENT'];
    return new DataViewType(buffer, byteOffset, dataViewLength);
}

function valueForBitWidth(bitWidth: number, values: any[]) {
    return values[bitWidth >> 4] || values[3];
}

function createSyntheticDictionaryIndexField(field: Field, type: FieldType) {
    let layouts = [];
    let builder = new flatbuffers.Builder();
    if (field.nullable()) {
        VectorLayout.startVectorLayout(builder);
        VectorLayout.addBitWidth(builder, 8);
        VectorLayout.addType(builder, VectorType.VALIDITY);
        builder.finish(VectorLayout.endVectorLayout(builder));
        layouts.push(VectorLayout.getRootAsVectorLayout(builder.dataBuffer()));
        builder = new flatbuffers.Builder();
    }
    VectorLayout.startVectorLayout(builder);
    VectorLayout.addBitWidth(builder, type.bitWidth());
    VectorLayout.addType(builder, VectorType.DATA);
    builder.finish(VectorLayout.endVectorLayout(builder));
    layouts.push(VectorLayout.getRootAsVectorLayout(builder.dataBuffer()));
    return Object.create(field, {
        layout: { value(i) { return layouts[i]; } },
        layoutLength: { value() { return layouts.length; } }
    });
}