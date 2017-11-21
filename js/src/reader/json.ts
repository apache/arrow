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
import { Vector } from '../vector/vector';
import { TypedArray, TypedArrayConstructor } from '../vector/types';
import { BinaryVector, BoolVector, Utf8Vector, Int8Vector,
         Int16Vector, Int32Vector, Int64Vector, Uint8Vector,
         Uint16Vector, Uint32Vector, Uint64Vector,
         Float32Vector, Float64Vector, ListVector, StructVector } from '../vector/arrow';

import { fb, FieldBuilder, FieldNodeBuilder } from '../format/arrow';

import { TextEncoder } from 'text-encoding-utf-8';
const encoder = new TextEncoder('utf-8');

export function* readJSON(jsonString: string): IterableIterator<Vector<any>[]> {
    let obj: any = JSON.parse(jsonString);
    let schema: any = {};
    for (const field of obj.schema.fields) {
        schema[field.name] = field;
    }

    for (const batch of obj.batches) {
        yield batch.columns.map((column: any): Vector => readVector(schema[column.name], column));
    }
}

function readVector(field: any, column: any): Vector {
    return readDictionaryVector(field, column) || readValueVector(field, column);
}

function readDictionaryVector(field: any, column: any) {
    if (field.name == column.name) { return null; } else { return null; }
}

function readValueVector(field: any, column: any): Vector {
    switch (field.type.name) {
        //case "NONE": return readNullVector(field, column);
        //case "null": return readNullVector(field, column);
        //case "map": return readMapVector(field, column);
        case 'int': return readIntVector(field, column);
        case 'bool': return readBoolVector(field, column);
        //case "date": return readDateVector(field, column);
        case 'list': return readListVector(field, column);
        case 'utf8': return readUtf8Vector(field, column);
        //case "time": return readTimeVector(field, column);
        //case "union": return readUnionVector(field, column);
        case 'binary': return readBinaryVector(field, column);
        //case "decimal": return readDecimalVector(field, column);
        case 'struct': return readStructVector(field, column);
        case 'floatingpoint': return readFloatVector(field, column);
        //case "timestamp": return readTimestampVector(field, column);
        //case "fixedsizelist": return readFixedSizeListVector(field, column);
        //case "fixedsizebinary": return readFixedSizeBinaryVector(field, column);
    }
    throw new Error(`Unrecognized Vector { name: ${field.name}, type: ${field.type.name} }`);
}

function readIntVector(field: any, column: any) {
    if (field.type.isSigned) {
        switch (field.type.bitWidth) {
            case  8: return new  Int8Vector(readNumeric(field, column, Int8Array));
            case 16: return new Int16Vector(readNumeric(field, column, Int16Array));
            case 32: return new Int32Vector(readNumeric(field, column, Int32Array));
            case 64: return new Int64Vector(readInt64(field, column, Int32Array));
        }
    }
    switch (field.type.bitWidth) {
        case  8: return new  Uint8Vector(readNumeric(field, column, Uint8Array));
        case 16: return new Uint16Vector(readNumeric(field, column, Uint16Array));
        case 32: return new Uint32Vector(readNumeric(field, column, Uint32Array));
        case 64: return new Uint64Vector(readInt64(field, column, Uint32Array));
    }
    throw new Error(`Unrecognized Int { isSigned: ${field.type.isSigned}, bitWidth: ${field.type.bitWidth} }`);
}

function readBoolVector(fieldObj: any, column: any) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    const data = readBoolean(column.DATA);
    return new BoolVector({field, fieldNode, validity, data});
}

function readListVector(fieldObj: any, column: any): Vector {
    const { field, fieldNode, validity, offsets } = readList(fieldObj, column);
    return new ListVector({
        field, fieldNode, validity, offsets,
        values: readVector(fieldObj.children[0], column.children[0])
    });
}

function readUtf8Vector(fieldObj: any, column: any): Vector {
    const { field, fieldNode, validity, offsets } = readList(fieldObj, column);
    const data = encoder.encode(column.DATA.join(''));
    return new Utf8Vector({
        field, fieldNode,
        values: new BinaryVector({
            validity, offsets, data
        })
    });
}

function readBinaryVector(field: any, column: any) {
    return new BinaryVector(readBinary(field, column));
}

function readStructVector(fieldObj: any, column: any) {
    const n = fieldObj.children.length;
    const columns = new Array<Vector>(n);
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    for (let i = -1; ++i < n;) {
            columns[i] = readVector(fieldObj.children[i], column.children[i]);
    }
    return new StructVector({ field, fieldNode, validity, columns });
}

function readFloatVector(field: any, column: any) {
    switch (field.type.precision) {
        // TODO: case "HALF":   return new Float16Vector(readNumeric(field, column, Uint16Array));
        case 'SINGLE': return new Float32Vector(readNumeric(field, column, Float32Array));
        case 'DOUBLE': return new Float64Vector(readNumeric(field, column, Float64Array));
    }
    throw new Error(`Unrecognized FloatingPoint { precision: ${field.type.precision} }`);
}

function readList(fieldObj: any, column: any) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    const offsets = readData(Int32Array, column.OFFSET);
    return { field, fieldNode, validity, offsets };
}

// "VALIDITY": [1,1],
// "OFFSET": [0,7,14],
// "DATA": ["49BC7D5B6C47D2","3F5FB6D9322026"]
function readBinary(fieldObj: any, column: any) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    const offsets = readData(Int32Array, column.OFFSET);
    // There are definitely more efficient ways to do this... but it gets the
    // job done.
    const joined = column.DATA.join('');
    let data = new Uint8Array(joined.length / 2);
    for (let i = 0; i < joined.length; i += 2) {
        data[i >> 1] = parseInt(joined.substr(i, 2), 16);
    }
    return { field, fieldNode, validity, offsets, data };
}

function readNumeric<T extends TypedArray>(fieldObj: any, column: any, ArrayConstructor: TypedArrayConstructor<T>) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    const data = readData(ArrayConstructor, column.DATA);
    return { field, fieldNode, validity, data };
}

function readInt64<T extends (Uint32Array|Int32Array)>(fieldObj: any, column: any, ArrayConstructor: TypedArrayConstructor<T>) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    let data = new ArrayConstructor(column.DATA.length * 2);
    for (let i = 0; i < column.DATA.length; ++i) {
        data[2 * i  ] = column.DATA[i] >>> 0;
        data[2 * i + 1] = Math.floor((column.DATA[i] / 0xFFFFFFFF));
    }
    return { field, fieldNode, validity, data };
}

function readData<T extends TypedArray>(ArrayConstructor: TypedArrayConstructor<T>, column: [number]) {
    return new ArrayConstructor(column);
}

function readValidity(column: any) {
    return readBoolean(column.VALIDITY);
}

function readBoolean(arr: Array<number>) {
    let rtrn: Uint8Array = new Uint8Array(Math.ceil(arr.length / 8));
    for (const {item, index} of arr.map((item: any, index: number) => ({item, index}))) {
        rtrn[index / 8 | 0] |= item << (index % 8);
    }
    return rtrn;
}

const TYPE_LOOKUP: {[index: string]: fb.Schema.Type} = {
    'NONE':            fb.Schema.Type.NONE,
    'null':            fb.Schema.Type.Null,
    'map':             fb.Schema.Type.Map,
    'int':             fb.Schema.Type.Int,
    'bool':            fb.Schema.Type.Bool,
    'date':            fb.Schema.Type.Date,
    'list':            fb.Schema.Type.List,
    'utf8':            fb.Schema.Type.Utf8,
    'time':            fb.Schema.Type.Time,
    'union':           fb.Schema.Type.Union,
    'binary':          fb.Schema.Type.Binary,
    'decimal':         fb.Schema.Type.Decimal,
    'struct_':         fb.Schema.Type.Struct_,
    'floatingpoint':   fb.Schema.Type.FloatingPoint,
    'timestamp':       fb.Schema.Type.Timestamp,
    'fixedsizelist':   fb.Schema.Type.FixedSizeList,
    'fixedsizebinary': fb.Schema.Type.FixedSizeBinary
};

function fieldFromJSON(obj: any): FieldBuilder {
    // TODO: metadata
    return new FieldBuilder(obj.name, TYPE_LOOKUP[obj.type.name], obj.nullable, []);
}

function fieldNodeFromJSON(obj: any): FieldNodeBuilder {
    let nullCount = obj.VALIDITY && obj.VALIDITY.length ?
        obj.VALIDITY.reduce((sum: number, current: number) => sum + current) : 0;
    return new FieldNodeBuilder(
        flatbuffers.Long.create(obj.count, 0),
        flatbuffers.Long.create(nullCount, 0)
    );
}
