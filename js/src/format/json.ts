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

import * as Schema_ from './fb/Schema';
import { flatbuffers } from 'flatbuffers';
import Long = flatbuffers.Long;
import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
import TimeUnit = Schema_.org.apache.arrow.flatbuf.TimeUnit;
import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
import IntervalUnit = Schema_.org.apache.arrow.flatbuf.IntervalUnit;
import {
    IntBitWidth, TimeBitWidth,
    Schema, RecordBatch, DictionaryBatch, Field, DictionaryEncoding, Buffer, FieldNode,
    Null, Int, FloatingPoint, Binary, Bool, Utf8, Decimal, Date, Time, Timestamp, Interval, List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
} from './types';

export function schemaFromJSON(s: any): Schema {
    // todo: metadataFromJSON
    return new Schema(
        MetadataVersion.V4,
        fieldsFromJSON(s['fields']),
        customMetadata(s['customMetadata'])
    );
}

export function recordBatchFromJSON(b: any): RecordBatch {
    return new RecordBatch(
        MetadataVersion.V4,
        new Long(b['count'], 0),
        fieldNodesFromJSON(b['columns']),
        buffersFromJSON(b['columns'])
    );
}

export function dictionaryBatchFromJSON(b: any): DictionaryBatch {
    return new DictionaryBatch(
        MetadataVersion.V4,
        recordBatchFromJSON(b['data']),
        new Long(b['id'], 0), b['isDelta']
    );
}

function fieldsFromJSON(fs: any[]): Field[] {
    return (fs || []).map(fieldFromJSON);
}

function fieldNodesFromJSON(xs: any[]): FieldNode[] {
    return (xs || []).reduce<FieldNode[]>((fieldNodes, column: any) => [
        ...fieldNodes,
        new FieldNode(
            new Long(column['count'], 0),
            new Long(nullCountFromJSON(column['VALIDITY']), 0)
        ),
        ...fieldNodesFromJSON(column['children'])
    ], [] as FieldNode[]);
}

function buffersFromJSON(xs: any[], buffers: Buffer[] = []): Buffer[] {
    for (let i = -1, n = (xs || []).length; ++i < n;) {
        const column = xs[i];
        column['VALIDITY'] && buffers.push(new Buffer(new Long(buffers.length, 0), new Long(column['VALIDITY'].length, 0)));
        column['OFFSET'] && buffers.push(new Buffer(new Long(buffers.length, 0), new Long(column['OFFSET'].length, 0)));
        column['DATA'] && buffers.push(new Buffer(new Long(buffers.length, 0), new Long(column['DATA'].length, 0)));
        buffers = buffersFromJSON(column['children'], buffers);
    }
    return buffers;
}

function nullCountFromJSON(validity: number[]) {
    return (validity || []).reduce((sum, val) => sum + +(val === 0), 0);
}

function fieldFromJSON(f: any) {
    return new Field(
        f['name'],
        typeFromJSON(f['type']),
        namesToTypeMap[f['type']['name']],
        f.nullable,
        fieldsFromJSON(f['children']),
        customMetadata(f['customMetadata']),
        dictionaryEncodingFromJSON(f['dictionary'])
    );
}

function dictionaryEncodingFromJSON(d: any) {
    return !d ? null : new DictionaryEncoding(
        d.indexType ? intFromJSON(d.indexType) : null,
        new Long(d.id, 0), d.isOrdered
    );
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

function typeFromJSON(t: any) {
    switch (namesToTypeMap[t['name']]) {
        case Type.NONE: return nullFromJSON(t);
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
        case Type.List: return listFromJSON(t);
        case Type.Struct_: return structFromJSON(t);
        case Type.Union: return unionFromJSON(t);
        case Type.FixedSizeBinary: return fixedSizeBinaryFromJSON(t);
        case Type.FixedSizeList: return fixedSizeListFromJSON(t);
        case Type.Map: return mapFromJSON(t);
    }
    throw new Error(`Unrecognized type ${t['name']}`);
}

function nullFromJSON(_type: any) { return new Null(); }
function intFromJSON(_type: any) { return new Int(_type['isSigned'], _type['bitWidth'] as IntBitWidth); }
function floatingPointFromJSON(_type: any) { return new FloatingPoint(Precision[_type['precision']] as any); }
function binaryFromJSON(_type: any) { return new Binary(); }
function utf8FromJSON(_type: any) { return new Utf8(); }
function boolFromJSON(_type: any) { return new Bool(); }
function decimalFromJSON(_type: any) { return new Decimal(_type['scale'], _type['precision']); }
function dateFromJSON(_type: any) { return new Date(DateUnit[_type['unit']] as any); }
function timeFromJSON(_type: any) { return new Time(TimeUnit[_type['unit']] as any, _type['bitWidth'] as TimeBitWidth); }
function timestampFromJSON(_type: any) { return new Timestamp(TimeUnit[_type['unit']] as any, _type['timezone']); }
function intervalFromJSON(_type: any) { return new Interval(IntervalUnit[_type['unit']] as any); }
function listFromJSON(_type: any) { return new List(); }
function structFromJSON(_type: any) { return new Struct(); }
function unionFromJSON(_type: any) { return new Union(_type['mode'], (_type['typeIdsArray'] || []) as Type[]); }
function fixedSizeBinaryFromJSON(_type: any) { return new FixedSizeBinary(_type['byteWidth']); }
function fixedSizeListFromJSON(_type: any) { return new FixedSizeList(_type['listSize']); }
function mapFromJSON(_type: any) { return new Map_(_type['keysSorted']); }
