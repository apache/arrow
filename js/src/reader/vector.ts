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
import * as Schema_ from '../format/Schema_generated';
import * as Message_ from '../format/Message_generated';
import { IteratorState, Dictionaries } from './arrow';
import {
    Vector, Column,
    IntArray, FloatArray,
    TypedArray, TypedArrayConstructor,
} from '../types/types';

import {
    DictionaryVector,
    BoolVector, LongVector,
    Utf8Vector, StructVector,
    ListVector, FixedSizeListVector,
    Int8Vector, Int16Vector, Int32Vector, Int64Vector,
    Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector,
    Date32Vector, Date64Vector, Float32Vector, Float64Vector,
} from '../types/arrow';

import Int = Schema_.org.apache.arrow.flatbuf.Int;
import Date = Schema_.org.apache.arrow.flatbuf.Date;
import Time = Schema_.org.apache.arrow.flatbuf.Time;
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import Field = Schema_.org.apache.arrow.flatbuf.Field;
import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
import FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;
import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
import VectorType = Schema_.org.apache.arrow.flatbuf.VectorType;
import VectorLayout = Schema_.org.apache.arrow.flatbuf.VectorLayout;
import FixedSizeList = Schema_.org.apache.arrow.flatbuf.FixedSizeList;
import FloatingPoint = Schema_.org.apache.arrow.flatbuf.FloatingPoint;
import DictionaryEncoding = Schema_.org.apache.arrow.flatbuf.DictionaryEncoding;

export function readVector<T>(field: Field, batch: MessageBatch, state: IteratorState, dictionaries: Dictionaries): Column<T> | DictionaryVector<T> | null {
    return readDictionaryVector<T>(field, batch, state, dictionaries) ||
                readTypedVector<T>(field, batch, state, dictionaries);
}

function readTypedVector<T>(field: Field, batch: MessageBatch, iterator: IteratorState, dictionaries: Dictionaries): Column<T> | DictionaryVector<T> | null {
    let typeType = field.typeType(), readTyped = typedVectorReaders[typeType];
    if (!readTyped) {
        throw new Error('Unrecognized vector name "' + Type[typeType] + '" type "' + typeType + '"');
    }
    return readTyped(field, batch, iterator, dictionaries) as Column<T>;
}

function readDictionaryVector<T>(field: Field, batch: MessageBatch, iterator: IteratorState, dictionaries: Dictionaries): DictionaryVector<T> | null {
    let data: Vector<any>, encoding: DictionaryEncoding;
    if (dictionaries &&
        (encoding = field.dictionary()!) &&
        (data = dictionaries[encoding.id().toFloat64().toString()])) {
        let indexType =  encoding.indexType() ||
            /* a dictionary index defaults to signed 32 bit int if unspecified */
            { bitWidth: () => 32, isSigned: () => true };
        // workaround for https://issues.apache.org/jira/browse/ARROW-1363
        let indexField = createSyntheticDictionaryIndexField(field, indexType);
        let keys = readIntVector(indexField, batch, iterator, null, indexType)!;
        return new DictionaryVector<T>({ data, keys: keys! });
    }
    return null;
}

function bindVectorReader<T extends TypedArray, V>(createBufferView: BufferViewFactory<T>, createVector: VectorFactory<T, V>) {
    return function readVector(field: Field, batch: MessageBatch, iterator: IteratorState, dictionaries: Dictionaries, primitiveType?: PrimitiveType) {
        return readVectorLayout(createBufferView, createVector, field, batch, iterator, dictionaries, primitiveType);
    };
}

const IntViews    = [Int8Array,    Int16Array,   Int32Array,   Int32Array  ];
const Int32Views  = [Int32Array,   Int32Array,   Int32Array,   Int32Array  ];
const UintViews   = [Uint8Array,   Uint16Array,  Uint32Array,  Uint32Array ];
const Uint8Views  = [Uint8Array,   Uint8Array,   Uint8Array,   Uint8Array  ];
const Uint32Views = [Uint32Array,  Uint32Array,  Uint32Array,  Uint32Array ];
const FloatViews  = [Int8Array,    Int16Array,   Float32Array, Float64Array];

const createIntDataViews = createTypedArray.bind(null, IntViews, null);
const createUintDataViews = createTypedArray.bind(null, UintViews, null);
const createDateDataViews = createTypedArray.bind(null, Int32Views, null);
const createFloatDataViews = createTypedArray.bind(null, FloatViews, null);
const createNestedDataViews = createTypedArray.bind(null, Uint32Views, null);
const createValidityDataViews = createTypedArray.bind(null, Uint8Views, null);
const createTimestampDataViews = createTypedArray.bind(null, Int32Views, null);
const createBinaryDataViews = createTypedArray.bind(null, Uint8Views, Int32Views);

const intVectors = [
    [/* unsigned */ [ Uint8Vector,  Uint8Array],  /* signed */ [ Int8Vector,  Int8Array]],
    [/* unsigned */ [Uint16Vector, Uint16Array],  /* signed */ [Int16Vector, Int16Array]],
    [/* unsigned */ [Uint32Vector, Uint32Array],  /* signed */ [Int32Vector, Int32Array]],
    [/* unsigned */ [Uint64Vector, Uint32Array],  /* signed */ [Int64Vector, Int32Array]]
] as any[][];

// Define as computed properties for closure-compiler, and as string-indexed keys for Uglify...
const floatVectors = {
    [Precision.HALF]: [Float32Vector, Float32Array],
    [Precision.SINGLE]: [Float32Vector, Float32Array],
    [Precision.DOUBLE]: [Float64Vector, Float64Array],
} as { [k: number]: [any, Float32ArrayConstructor | Float64ArrayConstructor] };

floatVectors[Precision['HALF']] = [Float32Vector, Float32Array];
floatVectors[Precision['SINGLE']] = [Float32Vector, Float32Array];
floatVectors[Precision['DOUBLE']] = [Float64Vector, Float64Array];

const dateVectors = {
    [DateUnit.DAY]: [Date32Vector, Int32Array],
    [DateUnit.MILLISECOND]: [Date64Vector, Uint32Array],
} as { [k: number]: [any, Int32ArrayConstructor | Uint32ArrayConstructor] };

dateVectors[DateUnit['DAY']] = [Date32Vector, Int32Array];
dateVectors[DateUnit['MILLISECOND']] = [Date64Vector, Uint32Array];

const readIntVector = (() => {
    return function readIntVector(field: Field, batch: MessageBatch, iterator: IteratorState, dictionaries: Dictionaries, primitiveType?: PrimitiveType) {
        let type = (primitiveType || field.type(new Int())!);
        return type.isSigned() ?
            readVectorLayout(createIntDataViews, createIntVector, field, batch, iterator, dictionaries, type) :
            readVectorLayout(createUintDataViews, createIntVector, field, batch, iterator, dictionaries, type);
    };
    function createIntVector(argv: VectorFactoryArgv<IntArray>) {
        let { field, fieldNode, data, validity, offsets, primitiveType } = argv;
        let type = primitiveType || field.type(new Int())!, bitWidth = type.bitWidth();
        let [IntVector, IntArray] = valueForBitWidth(bitWidth, intVectors)[+type.isSigned()];
        return new IntVector({ fieldNode, field, validity, data: data || offsets || new IntArray(0) });
        // ---------------------------------------------------------- 👆:
        // Workaround for https://issues.apache.org/jira/browse/ARROW-1363
        // This bug causes dictionary encoded vector indicies' IntVector data
        // buffers to be tagged as VectorType.OFFSET (0) in the field metadata
        // instead of VectorType.DATA. The `readVectorLayout` routine strictly
        // obeys the types in the field metadata, so if we're parsing an Arrow
        // file written by a version of the library published before ARROW-1363
        // was fixed, the IntVector's data buffer will be null, and the offset
        // buffer will be the actual data. If data is null, it's safe to assume
        // the offset buffer is the data, because IntVectors don't have offsets.
    }
})();

const readFloatVector = bindVectorReader(createFloatDataViews, (argv: VectorFactoryArgv<FloatArray>) => {
    let { field, fieldNode, validity, data } = argv;
    let type = field.type(new FloatingPoint())!;
    let [FloatVector, FloatArray] = floatVectors[type.precision()];
    return new FloatVector({ field, fieldNode, validity, data: data || new FloatArray(0) });
});

const readBoolVector = bindVectorReader(createValidityDataViews, (argv: VectorFactoryArgv<Uint8Array>) => {
    let { field, fieldNode, validity, data } = argv;
    return new BoolVector({ field, fieldNode, validity, data: data || new Uint8Array(0) });
});

const readDateVector = bindVectorReader(createDateDataViews, (argv: VectorFactoryArgv<Int32Array>) => {
    let { field, fieldNode, validity, data } = argv;
    let type = field.type(new Date())!;
    let [DateVector, DateArray] = dateVectors[type.unit()];
    return new DateVector({ field, fieldNode, validity, data: data || new DateArray(0) });
});

const readTimeVector = bindVectorReader(createIntDataViews, (argv: VectorFactoryArgv<IntArray>) => {
    let { field, fieldNode, data, validity } = argv;
    let type = field.type(new Time())!, bitWidth = type.bitWidth();
    let [IntVector, IntArray] = valueForBitWidth(bitWidth, intVectors)[1];
    return new IntVector({ fieldNode, field, validity, data: data || new IntArray(0) });
});

const readTimestampVector = bindVectorReader(createTimestampDataViews, (argv: VectorFactoryArgv<IntArray>) => {
    let { field, fieldNode, validity, data } = argv;
    return new LongVector({ fieldNode, field, validity, data: data || new Int32Array(0) });
});

const readBinaryVector = bindVectorReader(createBinaryDataViews, (argv: VectorFactoryArgv<Uint8Array>) => {
    let { field, fieldNode, data, offsets, validity } = argv;
    return new ListVector({
        field, fieldNode, validity,
        offsets: offsets as Int32Array,
        values: new Uint8Vector({ data: data || new Uint8Array(0) })
    });
});

const readUtf8Vector = bindVectorReader(createBinaryDataViews, (argv: VectorFactoryArgv<Uint8Array>) => {
    let { field, fieldNode, offsets, validity, data, messageBatch, iterator } = argv;
    // workaround for https://issues.apache.org/jira/browse/ARROW-1693
    if (!offsets && data) {
        let buffer = messageBatch.data.buffers(iterator.bufferIndex++)!;
        offsets = new Int32Array(data.buffer, data.byteOffset, data.byteLength / Int32Array.BYTES_PER_ELEMENT);
        data = createBinaryDataViews(messageBatch, VectorType.DATA, 8, buffer.offset().low, buffer.length().low);
    }
    return new Utf8Vector({
        field, fieldNode,
        values: new ListVector({
            validity,
            offsets: offsets as Int32Array,
            values: new Uint8Vector({ data: data || new Uint8Array(0) })
        }) as any as Vector<Uint8Array | null>
    });
});

const readListVector = bindVectorReader(createNestedDataViews, (argv: VectorFactoryArgv<TypedArray>) => {
    let { field, fieldNode, offsets, validity, iterator, messageBatch, dictionaries } = argv;
    return new ListVector({
        field, fieldNode, validity,
        offsets: offsets! as Int32Array,
        values: readVector(field.children(0)!, messageBatch, iterator, dictionaries)!
    });
});

const readFixedSizeListVector = bindVectorReader(createNestedDataViews, (argv: VectorFactoryArgv<Uint32Array>) => {
    let { field, fieldNode, validity, iterator, messageBatch, dictionaries } = argv;
    return new FixedSizeListVector({
        field, fieldNode, validity,
        listSize: field.type(new FixedSizeList())!.listSize(),
        values: readVector(field.children(0)!, messageBatch, iterator, dictionaries)!
    });
});

const readStructVector = bindVectorReader(createNestedDataViews, (argv: VectorFactoryArgv<ArrayLike<any>>) => {
    let { field, fieldNode, validity, iterator, messageBatch, dictionaries } = argv;
    let columns: Column<any>[] = [];
    for (let i = -1, n = field.childrenLength(); ++i < n;) {
        columns[i] = readVector<any>(field.children(i)!, messageBatch, iterator, dictionaries) as Column<any>;
    }
    return new StructVector({ field, fieldNode, validity, columns });
});

// Define as computed properties for closure-compiler, and again as string-indexed keys for Uglify...
const typedVectorReaders = {
    [Type.Int]: readIntVector,
    [Type.Bool]: readBoolVector,
    [Type.Date]: readDateVector,
    [Type.Time]: readTimeVector,
    [Type.Timestamp]: readTimestampVector,
    [Type.Interval]: readIntVector,
    [Type.List]: readListVector,
    [Type.Utf8]: readUtf8Vector,
    [Type.Binary]: readBinaryVector,
    [Type.Struct_]: readStructVector,
    [Type.FloatingPoint]: readFloatVector,
    [Type.FixedSizeList]: readFixedSizeListVector,
} as { [k: number]: (...args: any[]) => Vector | null };

typedVectorReaders[Type['Int']] = readIntVector;
typedVectorReaders[Type['Bool']] = readBoolVector;
typedVectorReaders[Type['Date']] = readDateVector;
typedVectorReaders[Type['Time']] = readTimeVector;
typedVectorReaders[Type['Timestamp']] = readTimestampVector;
typedVectorReaders[Type['Interval']] = readIntVector;
typedVectorReaders[Type['List']] = readListVector;
typedVectorReaders[Type['Utf8']] = readUtf8Vector;
typedVectorReaders[Type['Binary']] = readBinaryVector;
typedVectorReaders[Type['Struct_']] = readStructVector;
typedVectorReaders[Type['FloatingPoint']] = readFloatVector;
typedVectorReaders[Type['FixedSizeList']] = readFixedSizeListVector;

type VectorFactory<T, V> = (argv: VectorFactoryArgv<T>) => V;
type PrimitiveType = { bitWidth(): number; isSigned(): boolean };
type BufferViewFactory<T extends TypedArray> = (batch: MessageBatch, type: VectorType, bitWidth: number, offset: number, length: number) => T;

interface VectorFactoryArgv<T> {
    field: Field;
    fieldNode: FieldNode;
    iterator: IteratorState;
    dictionaries: Dictionaries;
    messageBatch: MessageBatch;
    data?: T;
    offsets?: TypedArray;
    validity?: Uint8Array;
    primitiveType?: PrimitiveType;
}

function readVectorLayout<T extends TypedArray, V>(
    createBufferView: BufferViewFactory<T>, createVector: VectorFactory<T, V>,
    field: Field, messageBatch: MessageBatch, iterator: IteratorState, dictionaries: Dictionaries, primitiveType?: PrimitiveType
) {
    let fieldNode: FieldNode, recordBatch = messageBatch.data;
    if (!(fieldNode = recordBatch.nodes(iterator.nodeIndex)!)) {
        return null;
    }
    iterator.nodeIndex += 1;
    let type, bitWidth, layout, buffer, bufferLength;
    let data: T | undefined, offsets: TypedArray | undefined, validity: Uint8Array | undefined;
    for (let i = -1, n = field.layoutLength(); ++i < n;) {
        if (!(layout = field.layout(i)!) ||
            !(buffer = recordBatch.buffers(iterator.bufferIndex)!)) {
            continue;
        }
        iterator.bufferIndex += 1;
        if ((type = layout.type()) === VectorType.TYPE ||
            (bufferLength = buffer.length().low) <= 0  ||
            (bitWidth = layout.bitWidth()) <= 0) {
            continue;
        } else if (type === VectorType.DATA) {
            data = createBufferView(messageBatch, type, bitWidth, buffer.offset().low, bufferLength);
        } else if (type === VectorType.OFFSET) {
            offsets = createBufferView(messageBatch, type, bitWidth, buffer.offset().low, bufferLength);
        } else if (fieldNode.nullCount().low > 0) {
            validity = createValidityDataViews(messageBatch, type, bitWidth, buffer.offset().low, fieldNode.length().low);
        }
    }
    return createVector({ data, offsets, validity, field, fieldNode, iterator, messageBatch, dictionaries, primitiveType });
}

function createTypedArray(
    bufferViews: TypedArrayConstructor[], offsetViews: TypedArrayConstructor[] | null,
    batch: MessageBatch, type: VectorType, bitWidth: number, offset: number, length: number
) {
    const buffer = batch.bytes.buffer;
    const byteLength = buffer.byteLength;
    const byteOffset = batch.offset + offset;
    const DataViewType = valueForBitWidth(bitWidth, type === VectorType.OFFSET && offsetViews || bufferViews);
    const dataViewLength = ((byteOffset + length) <= byteLength
        ? length
        : byteLength - byteOffset
    ) / DataViewType['BYTES_PER_ELEMENT'];
    return new DataViewType(buffer, byteOffset, dataViewLength);
}

function valueForBitWidth<T>(bitWidth: number, values: T[]) {
    return values[bitWidth >> 4] || values[3];
}

function createSyntheticDictionaryIndexField(field: Field, type: PrimitiveType) {
    let layouts = [] as VectorLayout[];
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
        layout: { value(i: number) { return layouts[i]; } },
        layoutLength: { value() { return layouts.length; } }
    });
}
