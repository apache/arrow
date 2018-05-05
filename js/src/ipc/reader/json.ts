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

import { Vector } from '../../vector';
import { flatbuffers } from 'flatbuffers';
import { TypeDataLoader } from './vector';
import { packBools } from '../../util/bit';
import * as IntUtil from '../../util/int';
import { TextEncoder } from 'text-encoding-utf-8';
import { RecordBatchMetadata, DictionaryBatch, BufferMetadata, FieldMetadata } from '../metadata';
import {
    Schema, Field,
    DataType, Dictionary,
    Null, TimeBitWidth,
    Binary, Bool, Utf8, Decimal,
    Date_, Time, Timestamp, Interval,
    List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
} from '../../type';

import {
    Int8,  Uint8,
    Int16, Uint16,
    Int32, Uint32,
    Int64, Uint64,
    Float16, Float64, Float32,
} from '../../type';

import Long = flatbuffers.Long;

export function* readJSON(json: any) {
    const schema = schemaFromJSON(json['schema']);
    const dictionaries = new Map<number, Vector>();
    for (const batch of (json['dictionaries'] || [])) {
        const message = dictionaryBatchFromJSON(batch);
        yield {
            schema, message,
            loader: new JSONDataLoader(
                flattenDataSources(batch['data']['columns']),
                arrayIterator(message.nodes),
                arrayIterator(message.buffers),
                dictionaries
            )
        };
    }
    for (const batch of (json['batches'] || [])) {
        const message = recordBatchFromJSON(batch);
        yield {
            schema, message,
            loader: new JSONDataLoader(
                flattenDataSources(batch['columns']),
                arrayIterator(message.nodes),
                arrayIterator(message.buffers),
                dictionaries
            )
        };
    }
}

function* arrayIterator(arr: Array<any>) { yield* arr; }
function flattenDataSources(xs: any[]): any[][] {
    return (xs || []).reduce<any[][]>((buffers, column: any) => [
        ...buffers,
        ...(column['VALIDITY'] && [column['VALIDITY']] || []),
        ...(column['OFFSET'] && [column['OFFSET']] || []),
        ...(column['DATA'] && [column['DATA']] || []),
        ...flattenDataSources(column['children'])
    ], [] as any[][]);
}

const utf8Encoder = new TextEncoder('utf-8');

export class JSONDataLoader extends TypeDataLoader {
    constructor(private sources: any[][], nodes: Iterator<FieldMetadata>, buffers: Iterator<BufferMetadata>, dictionaries: Map<number, Vector>) {
        super(nodes, buffers, dictionaries);
    }
    protected readNullBitmap<T extends DataType>(_type: T, nullCount: number, { offset } = this.getBufferMetadata()) {
        return nullCount <= 0 ? new Uint8Array(0) : packBools(this.sources[offset]);
    }
    protected readOffsets<T extends DataType>(_type: T, { offset }: BufferMetadata = this.getBufferMetadata()) {
        return new Int32Array(this.sources[offset]);
    }
    protected readTypeIds<T extends DataType>(_type: T, { offset }: BufferMetadata = this.getBufferMetadata()) {
        return new Int8Array(this.sources[offset]);
    }
    protected readData<T extends DataType>(type: T, { offset }: BufferMetadata = this.getBufferMetadata()) {
        const { sources } = this;
        if (DataType.isTimestamp(type) === true) {
            return new Uint8Array(int64DataFromJSON(sources[offset] as string[]));
        } else if ((DataType.isInt(type) || DataType.isTime(type)) && type.bitWidth === 64) {
            return new Uint8Array(int64DataFromJSON(sources[offset] as string[]));
        } else if (DataType.isDate(type) && type.unit === DateUnit.MILLISECOND) {
            return new Uint8Array(int64DataFromJSON(sources[offset] as string[]));
        } else if (DataType.isDecimal(type) === true) {
            return new Uint8Array(decimalDataFromJSON(sources[offset] as string[]));
        } else if (DataType.isBinary(type) === true || DataType.isFixedSizeBinary(type) === true) {
            return new Uint8Array(binaryDataFromJSON(sources[offset] as string[]));
        } else if (DataType.isBool(type) === true) {
            return new Uint8Array(packBools(sources[offset] as number[]).buffer);
        } else if (DataType.isUtf8(type) === true) {
            return utf8Encoder.encode((sources[offset] as string[]).join(''));
        } else {
            return toTypedArray(type.ArrayType, sources[offset].map((x) => +x)) as any;
        }
    }
}

function int64DataFromJSON(values: string[]) {
    const data = new Uint32Array(values.length * 2);
    for (let i = -1, n = values.length; ++i < n;) {
        // Force all values (even numbers) to be parsed as strings since
        // pulling out high and low bits seems to lose precision sometimes
        // For example:
        //     > -4613034156400212000 >>> 0
        //     721782784
        // The correct lower 32-bits are 721782752
        IntUtil.Int64.fromString(values[i].toString(), new Uint32Array(data.buffer, data.byteOffset + 2 * i * 4, 2));
    }
    return data.buffer;
}

function decimalDataFromJSON(values: string[]) {
    const data = new Uint32Array(values.length * 4);
    for (let i = -1, n = values.length; ++i < n;) {
        IntUtil.Int128.fromString(values[i], new Uint32Array(data.buffer, data.byteOffset + 4 * 4 * i, 4));
    }
    return data.buffer;
}

function binaryDataFromJSON(values: string[]) {
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

import * as Schema_ from '../../fb/Schema';
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
import TimeUnit = Schema_.org.apache.arrow.flatbuf.TimeUnit;
import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
import IntervalUnit = Schema_.org.apache.arrow.flatbuf.IntervalUnit;
import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;
import { toTypedArray } from '../../data';

function schemaFromJSON(s: any): Schema {
    const dictionaryFields = new Map<number, Field<Dictionary>>();
    return new Schema(
        fieldsFromJSON(s['fields'], dictionaryFields),
        customMetadata(s['customMetadata']),
        MetadataVersion.V4, dictionaryFields
    );
}

function recordBatchFromJSON(b: any): RecordBatchMetadata {
    return new RecordBatchMetadata(
        MetadataVersion.V4,
        b['count'],
        fieldNodesFromJSON(b['columns']),
        buffersFromJSON(b['columns'])
    );
}

function dictionaryBatchFromJSON(b: any): DictionaryBatch {
    return new DictionaryBatch(
        MetadataVersion.V4,
        recordBatchFromJSON(b['data']),
        b['id'], b['isDelta']
    );
}

function fieldsFromJSON(fs: any[], dictionaryFields: Map<number, Field<Dictionary>> | null): Field[] {
    return (fs || [])
        .map((f) => fieldFromJSON(f, dictionaryFields))
        .filter((f) => f != null) as Field[];
}

function fieldNodesFromJSON(xs: any[]): FieldMetadata[] {
    return (xs || []).reduce<FieldMetadata[]>((fieldNodes, column: any) => [
        ...fieldNodes,
        new FieldMetadata(
            new Long(column['count'], 0),
            new Long(nullCountFromJSON(column['VALIDITY']), 0)
        ),
        ...fieldNodesFromJSON(column['children'])
    ], [] as FieldMetadata[]);
}

function buffersFromJSON(xs: any[], buffers: BufferMetadata[] = []): BufferMetadata[] {
    for (let i = -1, n = (xs || []).length; ++i < n;) {
        const column = xs[i];
        column['VALIDITY'] && buffers.push(new BufferMetadata(new Long(buffers.length, 0), new Long(column['VALIDITY'].length, 0)));
        column['OFFSET'] && buffers.push(new BufferMetadata(new Long(buffers.length, 0), new Long(column['OFFSET'].length, 0)));
        column['DATA'] && buffers.push(new BufferMetadata(new Long(buffers.length, 0), new Long(column['DATA'].length, 0)));
        buffers = buffersFromJSON(column['children'], buffers);
    }
    return buffers;
}

function nullCountFromJSON(validity: number[]) {
    return (validity || []).reduce((sum, val) => sum + +(val === 0), 0);
}

function fieldFromJSON(f: any, dictionaryFields: Map<number, Field<Dictionary>> | null) {
    let name = f['name'];
    let field: Field | void;
    let nullable = f['nullable'];
    let dataType: DataType<any> | null;
    let id: number, keysMeta: any, dictMeta: any;
    let metadata = customMetadata(f['customMetadata']);
    if (!dictionaryFields || !(dictMeta = f['dictionary'])) {
        if (dataType = typeFromJSON(f['type'], fieldsFromJSON(f['children'], dictionaryFields))) {
            field = new Field(name, dataType, nullable, metadata);
        }
    } else if (dataType = dictionaryFields.has(id = dictMeta['id'])
                        ? dictionaryFields.get(id)!.type.dictionary
                        : typeFromJSON(f['type'], fieldsFromJSON(f['children'], null))) {
        dataType = new Dictionary(dataType,
            // a dictionary index defaults to signed 32 bit int if unspecified
            (keysMeta = dictMeta['indexType']) ? intFromJSON(keysMeta)! : new Int32(),
            id, dictMeta['isOrdered']
        );
        field = new Field(name, dataType, nullable, metadata);
        dictionaryFields.has(id) || dictionaryFields.set(id, field as Field<Dictionary>);
    }
    return field || null;
}

function customMetadata(metadata?: any) {
    return new Map<string, string>(Object.entries(metadata || {}));
}

const namesToTypeMap: { [n: string]: Type }  = {
    'NONE': Type.NONE,
    'null': Type.Null,
    'int': Type.Int,
    'floatingpoint': Type.FloatingPoint,
    'binary': Type.Binary,
    'bool': Type.Bool,
    'utf8': Type.Utf8,
    'decimal': Type.Decimal,
    'date': Type.Date,
    'time': Type.Time,
    'timestamp': Type.Timestamp,
    'interval': Type.Interval,
    'list': Type.List,
    'struct': Type.Struct_,
    'union': Type.Union,
    'fixedsizebinary': Type.FixedSizeBinary,
    'fixedsizelist': Type.FixedSizeList,
    'map': Type.Map,
};

function typeFromJSON(t: any, children?: Field[]) {
    switch (namesToTypeMap[t['name']]) {
        case Type.NONE: return null;
        case Type.Null: return nullFromJSON(t);
        case Type.Int: return intFromJSON(t);
        case Type.FloatingPoint: return floatingPointFromJSON(t);
        case Type.Binary: return binaryFromJSON(t);
        case Type.Utf8: return utf8FromJSON(t);
        case Type.Bool: return boolFromJSON(t);
        case Type.Decimal: return decimalFromJSON(t);
        case Type.Date: return dateFromJSON(t);
        case Type.Time: return timeFromJSON(t);
        case Type.Timestamp: return timestampFromJSON(t);
        case Type.Interval: return intervalFromJSON(t);
        case Type.List: return listFromJSON(t, children || []);
        case Type.Struct_: return structFromJSON(t, children || []);
        case Type.Union: return unionFromJSON(t, children || []);
        case Type.FixedSizeBinary: return fixedSizeBinaryFromJSON(t);
        case Type.FixedSizeList: return fixedSizeListFromJSON(t, children || []);
        case Type.Map: return mapFromJSON(t, children || []);
    }
    throw new Error(`Unrecognized type ${t['name']}`);
}

function nullFromJSON           (_type: any)                    { return new Null();                                                                  }
function intFromJSON            (_type: any)                    { switch (_type['bitWidth']) {
                                                                      case  8: return _type['isSigned'] ? new  Int8() : new  Uint8();
                                                                      case 16: return _type['isSigned'] ? new Int16() : new Uint16();
                                                                      case 32: return _type['isSigned'] ? new Int32() : new Uint32();
                                                                      case 64: return _type['isSigned'] ? new Int64() : new Uint64();
                                                                  }
                                                                  return null;                                                                        }
function floatingPointFromJSON  (_type: any)                    { switch (Precision[_type['precision']] as any) {
                                                                      case Precision.HALF: return new Float16();
                                                                      case Precision.SINGLE: return new Float32();
                                                                      case Precision.DOUBLE: return new Float64();
                                                                  }
                                                                  return null;                                                                        }
function binaryFromJSON         (_type: any)                    { return new Binary();                                                                }
function utf8FromJSON           (_type: any)                    { return new Utf8();                                                                  }
function boolFromJSON           (_type: any)                    { return new Bool();                                                                  }
function decimalFromJSON        (_type: any)                    { return new Decimal(_type['scale'], _type['precision']);                             }
function dateFromJSON           (_type: any)                    { return new Date_(DateUnit[_type['unit']] as any);                                   }
function timeFromJSON           (_type: any)                    { return new Time(TimeUnit[_type['unit']] as any, _type['bitWidth'] as TimeBitWidth); }
function timestampFromJSON      (_type: any)                    { return new Timestamp(TimeUnit[_type['unit']] as any, _type['timezone']);            }
function intervalFromJSON       (_type: any)                    { return new Interval(IntervalUnit[_type['unit']] as any);                            }
function listFromJSON           (_type: any, children: Field[]) { return new List(children);                                                          }
function structFromJSON         (_type: any, children: Field[]) { return new Struct(children);                                                        }
function unionFromJSON          (_type: any, children: Field[]) { return new Union(_type['mode'], (_type['typeIdsArray'] || []) as Type[], children); }
function fixedSizeBinaryFromJSON(_type: any)                    { return new FixedSizeBinary(_type['byteWidth']);                                     }
function fixedSizeListFromJSON  (_type: any, children: Field[]) { return new FixedSizeList(_type['listSize'], children);                              }
function mapFromJSON            (_type: any, children: Field[]) { return new Map_(_type['keysSorted'], children);                                     }
