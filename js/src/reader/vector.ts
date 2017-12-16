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

import * as Schema_ from '../format/fb/Schema';
import { TypedArray, TypedArrayConstructor } from '../vector/types';
import { Schema, RecordBatch, DictionaryBatch, Field, FieldNode } from '../format/arrow';
import { Int, Date, Time, Timestamp, Decimal, FixedSizeList, FixedSizeBinary, FloatingPoint } from '../format/arrow';
import {
    Vector, BoolVector, BinaryVector, DictionaryVector,
    Int8Vector, Int16Vector, Int32Vector, Int64Vector,
    Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector,
    Utf8Vector, ListVector, FixedSizeListVector, StructVector,
    Float16Vector, Float32Vector, Float64Vector, DecimalVector,
    Date32Vector, Date64Vector, Time32Vector, Time64Vector, TimestampVector,
} from '../vector/arrow';

import Type = Schema_.org.apache.arrow.flatbuf.Type;
import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
import TimeUnit = Schema_.org.apache.arrow.flatbuf.TimeUnit;
import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
// import IntervalUnit = Schema_.org.apache.arrow.flatbuf.IntervalUnit;

export interface ContainerLayout {
    fieldNode: FieldNode;
    validity: Uint8Array | null | void;
}

export interface VariableWidthLayout {
    fieldNode: FieldNode;
    offsets: Int32Array;
    validity: Uint8Array | null | void;
}

export interface BinaryLayout extends FixedWidthLayout<Uint8Array> {
    offsets: Int32Array;
}

export interface FixedWidthLayout<T extends TypedArray> {
    fieldNode: FieldNode;
    data: T;
    validity: Uint8Array | null | void;
}

export function* readVectors(messages: Iterable<{ schema: Schema, message: RecordBatch | DictionaryBatch, reader: VectorLayoutReader }>) {
    const dictionaries = new Map<string, Vector>();
    for (const { schema, message, reader } of messages) {
        yield* readMessageVectors(schema, message, new VectorReader(dictionaries, reader));
    }
}

export async function* readVectorsAsync(messages: AsyncIterable<{ schema: Schema, message: RecordBatch | DictionaryBatch, reader: VectorLayoutReader }>) {
    const dictionaries = new Map<string, Vector>();
    for await (const { schema, message, reader } of messages) {
        yield* readMessageVectors(schema, message, new VectorReader(dictionaries, reader));
    }
}

function* readMessageVectors(schema: Schema, message: RecordBatch | DictionaryBatch, reader: VectorReader) {
    if (message.isRecordBatch() === true) {
        yield schema.fields.map((field) => reader.readVector(field));
    } else if (message.isDictionaryBatch()) {
        let id = message.dictionaryId.toFloat64().toString();
        let vector = reader.readValueVector(schema.dictionaries.get(id)!);
        if (message.isDelta) {
            vector = reader.dictionaries.get(id)!.concat(vector);
        }
        reader.dictionaries.set(id, vector);
    }
}

export interface VectorLayoutReader {
    readBinaryLayout(field: Field): BinaryLayout;
    readContainerLayout(field: Field): ContainerLayout;
    readVariableWidthLayout(field: Field): VariableWidthLayout;
    readFixedWidthLayout<T extends TypedArray>(field: Field, TypedArrayConstructor: TypedArrayConstructor<T>): FixedWidthLayout<T>;
}

export class VectorReader implements VectorLayoutReader {
    constructor(public dictionaries: Map<string, Vector>, protected layout: VectorLayoutReader) {}
    readVector(field: Field): Vector {
        return this.readDictionaryVector(field) || this.readValueVector(field);
    }
    readDictionaryVector(field: Field) {
        const encoding = field.dictionary;
        if (encoding) {
            const keys = this.readIntVector(field.indexField());
            const data = this.dictionaries.get(encoding.dictionaryId.toFloat64().toString())!;
            return new DictionaryVector({
                field, data, keys,
                validity: (keys as any).validity,
                fieldNode: (keys as any).fieldNode,
            });
        }
        return null;
    }
    readValueVector(field: Field) {
        switch (field.typeType) {
            case Type.NONE: return this.readNullVector();
            case Type.Null: return this.readNullVector();
            // case Type.Map: return this.readMapVector(field);
            case Type.Int: return this.readIntVector(field);
            case Type.Bool: return this.readBoolVector(field);
            case Type.Date: return this.readDateVector(field);
            case Type.List: return this.readListVector(field);
            case Type.Utf8: return this.readUtf8Vector(field);
            case Type.Time: return this.readTimeVector(field);
            // case Type.Union: return this.readUnionVector(field);
            case Type.Binary: return this.readBinaryVector(field);
            case Type.Decimal: return this.readDecimalVector(field);
            case Type.Struct_: return this.readStructVector(field);
            case Type.FloatingPoint: return this.readFloatVector(field);
            case Type.Timestamp: return this.readTimestampVector(field);
            case Type.FixedSizeList: return this.readFixedSizeListVector(field);
            case Type.FixedSizeBinary: return this.readFixedSizeBinaryVector(field);
        }
        throw new Error(`Unrecognized ${field.toString()}`);
    }
    readNullVector() {
        return new Vector();
    }
    readBoolVector(field: Field) {
        return new BoolVector(this.readFixedWidthLayout(field, Uint8Array));
    }
    readDateVector(field: Field) {
        const type = field.type as Date;
        switch (type.unit) {
            case DateUnit.DAY: return new Date32Vector({ ...this.readFixedWidthLayout(field, Int32Array), unit: DateUnit[type.unit] });
            case DateUnit.MILLISECOND: return new Date64Vector({ ...this.readFixedWidthLayout(field, Int32Array), unit: DateUnit[type.unit] });
        }
        throw new Error(`Unrecognized ${type.toString()}`);
    }
    readTimeVector(field: Field) {
        const type = field.type as Time;
        switch (type.bitWidth) {
            case 32: return new Time32Vector({ ...this.readFixedWidthLayout(field, Int32Array), unit: TimeUnit[type.unit] });
            case 64: return new Time64Vector({ ...this.readFixedWidthLayout(field, Uint32Array), unit: TimeUnit[type.unit] });
        }
        throw new Error(`Unrecognized ${type.toString()}`);
    }
    readTimestampVector(field: Field) {
        const type = field.type as Timestamp;
        const { fieldNode, validity, data } = this.readFixedWidthLayout(field, Uint32Array);
        return new TimestampVector({
            field, fieldNode, validity, data,
            timezone: type.timezone!,
            unit: TimeUnit[type.unit],
        });
    }
    readListVector(field: Field) {
        const { fieldNode, validity, offsets } = this.readVariableWidthLayout(field);
        return new ListVector({
            field, fieldNode, validity, offsets,
            values: this.readVector(field.children[0])
        });
    }
    readStructVector(field: Field) {
        const { fieldNode, validity } = this.readContainerLayout(field);
        return new StructVector({
            field, fieldNode, validity,
            columns: field.children.map((field) => this.readVector(field))
        });
    }
    readBinaryVector(field: Field) {
        return new BinaryVector(this.readBinaryLayout(field));
    }
    readDecimalVector(field: Field) {
        const type = field.type as Decimal;
        const { fieldNode, validity, data } = this.readFixedWidthLayout(field, Uint32Array);
        return new DecimalVector({
            scale: type.scale,
            precision: type.precision,
            field, fieldNode, validity, data
        });
    }
    readUtf8Vector(field: Field) {
        const { fieldNode, validity, offsets, data } = this.readBinaryLayout(field);
        return new Utf8Vector({
            field, fieldNode,
            values: new BinaryVector({
                validity, offsets, data
            })
        });
    }
    readFixedSizeListVector(field: Field) {
        const type = field.type as FixedSizeList;
        const { fieldNode, validity } = this.readContainerLayout(field);
        return new FixedSizeListVector({
            field, fieldNode, validity,
            size: type.listSize,
            values: this.readVector(field.children[0])
        });
    }
    readFixedSizeBinaryVector(field: Field) {
        const type = field.type as FixedSizeBinary;
        const { fieldNode, validity, data } = this.readFixedWidthLayout(field, Uint8Array);
        return new FixedSizeListVector({
            size: type.byteWidth,
            field, fieldNode, validity,
            values: new Uint8Vector({ data })
        });
    }
    readFloatVector(field: Field) {
        const type = field.type as FloatingPoint;
        switch (type.precision) {
            case Precision.HALF:   return new Float16Vector(this.readFixedWidthLayout(field, Uint16Array));
            case Precision.SINGLE: return new Float32Vector(this.readFixedWidthLayout(field, Float32Array));
            case Precision.DOUBLE: return new Float64Vector(this.readFixedWidthLayout(field, Float64Array));
        }
        throw new Error(`Unrecognized FloatingPoint { precision: ${type.precision} }`);
    }
    readIntVector(field: Field) {
        const type = field.type as Int;
        if (type.isSigned) {
            switch (type.bitWidth) {
                case  8: return new  Int8Vector(this.readFixedWidthLayout(field, Int8Array));
                case 16: return new Int16Vector(this.readFixedWidthLayout(field, Int16Array));
                case 32: return new Int32Vector(this.readFixedWidthLayout(field, Int32Array));
                case 64: return new Int64Vector(this.readFixedWidthLayout(field, Int32Array));
            }
        }
        switch (type.bitWidth) {
            case  8: return new  Uint8Vector(this.readFixedWidthLayout(field, Uint8Array));
            case 16: return new Uint16Vector(this.readFixedWidthLayout(field, Uint16Array));
            case 32: return new Uint32Vector(this.readFixedWidthLayout(field, Uint32Array));
            case 64: return new Uint64Vector(this.readFixedWidthLayout(field, Uint32Array));
        }
        throw new Error(`Unrecognized Int { isSigned: ${type.isSigned}, bitWidth: ${type.bitWidth} }`);
    }
    readContainerLayout(field: Field) {
        return this.layout.readContainerLayout(field);
    }
    readBinaryLayout(field: Field) {
        return this.layout.readBinaryLayout(field);
    }
    readVariableWidthLayout(field: Field) {
        return this.layout.readVariableWidthLayout(field);
    }
    readFixedWidthLayout<T extends TypedArray>(field: Field, TypedArrayConstructor: TypedArrayConstructor<T>) {
        return this.layout.readFixedWidthLayout(field, TypedArrayConstructor);
    }
}
