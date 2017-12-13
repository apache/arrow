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
import { Int64, Int128 } from '../util/int';
import { VectorLayoutReader } from './vector';
import { TextEncoder } from 'text-encoding-utf-8';
import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
import { TypedArray, TypedArrayConstructor } from '../vector/types';
import { schemaFromJSON, recordBatchFromJSON, dictionaryBatchFromJSON } from '../format/json';
import { Schema, RecordBatch, DictionaryBatch, Field, Buffer, FieldNode } from '../format/arrow';
export { Schema, RecordBatch, DictionaryBatch };

export function* readJSON(json: any) {
    const schema = schemaFromJSON(json['schema']);
    for (const batch of (json['dictionaries'] || [])) {
        const message = dictionaryBatchFromJSON(batch);
        yield {
            schema, message, reader: new JSONVectorLayoutReader(
                flattenDataSources(batch['data']['columns']),
                (function* (fieldNodes) { yield* fieldNodes; })(message.fieldNodes),
                (function* (buffers) { yield* buffers; })(message.buffers)
            ) as VectorLayoutReader
        };
    }
    for (const batch of (json['batches'] || [])) {
        const message = recordBatchFromJSON(batch);
        yield {
            schema, message, reader: new JSONVectorLayoutReader(
                flattenDataSources(batch['columns']),
                (function* (fieldNodes) { yield* fieldNodes; })(message.fieldNodes),
                (function* (buffers) { yield* buffers; })(message.buffers)
            ) as VectorLayoutReader
        };
    }
}

function flattenDataSources(xs: any[]): any[][] {
    return (xs || []).reduce<any[][]>((buffers, column: any) => [
        ...buffers,
        ...(column['VALIDITY'] && [column['VALIDITY']] || []),
        ...(column['OFFSET'] && [column['OFFSET']] || []),
        ...(column['DATA'] && [column['DATA']] || []),
        ...flattenDataSources(column['children'])
    ], [] as any[][]);
}

class JSONVectorLayoutReader implements VectorLayoutReader {
    constructor(private sources: any[][], private fieldNodes: Iterator<FieldNode>, private buffers: Iterator<Buffer>) {}
    readContainerLayout(field: Field) {
        const { sources, buffers } = this, fieldNode = this.fieldNodes.next().value;
        return {
            field, fieldNode,
            validity: createValidityArray(sources, field, fieldNode, buffers.next().value)
        };
    }
    readFixedWidthLayout<T extends TypedArray>(field: Field, dataType: TypedArrayConstructor<T>) {
        const { sources, buffers } = this, fieldNode = this.fieldNodes.next().value;
        return {
            field, fieldNode,
            validity: createValidityArray(sources, field, fieldNode, buffers.next().value),
            data: createDataArray(sources, field, fieldNode, buffers.next().value, dataType)
        };
    }
    readBinaryLayout(field: Field) {
        const { sources, buffers } = this, fieldNode = this.fieldNodes.next().value;
        return {
            field, fieldNode,
            validity: createValidityArray(sources, field, fieldNode, buffers.next().value),
            offsets: new Int32Array(sources[buffers.next().value.offset.low]),
            data: createDataArray(sources, field, fieldNode, buffers.next().value, Uint8Array)
        };
    }
    readVariableWidthLayout(field: Field) {
        const { sources, buffers } = this, fieldNode = this.fieldNodes.next().value;
        return {
            field, fieldNode,
            validity: createValidityArray(sources, field, fieldNode, buffers.next().value),
            offsets: new Int32Array(sources[buffers.next().value.offset.low]),
        };
    }
}

function createValidityArray(sources: any[][], field: Field, fieldNode: FieldNode, buffer: Buffer) {
    return field.nullable && fieldNode.nullCount.low > 0 &&
        booleanFromJSON(sources[buffer.offset.low]) || null;
}

const encoder = new TextEncoder('utf-8');

function createDataArray<T extends TypedArray>(sources: any[][], field: Field, _fieldNode: FieldNode, buffer: Buffer, ArrayConstructor: TypedArrayConstructor<T>): T {
    let type = field.type, data: ArrayLike<number> | ArrayBufferLike;
    if (type.isTimestamp() === true) {
        data = int64sFromJSON(sources[buffer.offset.low] as string[]);
    } else if ((type.isInt() || type.isTime()) && type.bitWidth === 64) {
        data = int64sFromJSON(sources[buffer.offset.low] as string[]);
    } else if (type.isDate() && type.unit === DateUnit.MILLISECOND) {
        data = int64sFromJSON(sources[buffer.offset.low] as string[]);
    } else if (type.isDecimal() === true) {
        data = decimalFromJSON(sources[buffer.offset.low] as string[]);
    } else if (type.isBinary() === true) {
        data = binaryFromJSON(sources[buffer.offset.low] as string[]);
    } else if (type.isBool() === true) {
        data = booleanFromJSON(sources[buffer.offset.low] as number[]).buffer;
    } else if (type.isUtf8() === true) {
        data = encoder.encode((sources[buffer.offset.low] as string[]).join(''));
    } else {
        data = (sources[buffer.offset.low]).map((x) => +x);
    }
    return new ArrayConstructor(data);
}

function int64sFromJSON(values: string[]) {
    const data = new Uint32Array(values.length * 2);
    for (let i = -1, n = values.length; ++i < n;) {
        // Force all values (even numbers) to be parsed as strings since
        // pulling out high and low bits seems to lose precision sometimes
        // For example:
        //     > -4613034156400212000 >>> 0
        //     721782784
        // The correct lower 32-bits are 721782752
        Int64.fromString(values[i].toString(), new Uint32Array(data.buffer, data.byteOffset + 2 * i * 4, 2));
    }
    return data.buffer;
}

function decimalFromJSON(values: string[]) {
    const data = new Uint32Array(values.length * 4);
    for (let i = -1, n = values.length; ++i < n;) {
        Int128.fromString(values[i], new Uint32Array(data.buffer, data.byteOffset + 4 * 4 * i, 4));
    }
    return data.buffer;
}

function binaryFromJSON(values: string[]) {
    // "DATA": ["49BC7D5B6C47D2","3F5FB6D9322026"]
    // There are definitely more efficient ways to do this... but it gets the
    // job done.
    const joined = values.join('');
    const data = new Uint8Array(joined.length / 2);
    for (let i = 0; i < joined.length; i += 2) {
        data[i >> 1] = parseInt(joined.substr(i, 2), 16);
    }
    return data.buffer;
}

function booleanFromJSON(arr: number[]) {
    let xs = [], n, i = 0;
    let bit = 0, byte = 0;
    for (const value of arr) {
        value && (byte |= 1 << bit);
        if (++bit === 8) {
            xs[i++] = byte;
            byte = bit = 0;
        }
    }
    if (i === 0 || bit > 0) { xs[i++] = byte; }
    if (i % 8 && (n = i + 8 - i % 8)) {
        do { xs[i] = 0; } while (++i < n);
    }
    return new Uint8Array(xs);
}
