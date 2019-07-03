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

import { Data } from '../data';
import * as type from '../type';
import { Field } from '../schema';
import { Vector } from '../vector';
import { DataType } from '../type';
import { Visitor } from '../visitor';
import { packBools } from '../util/bit';
import { encodeUtf8 } from '../util/utf8';
import { Int64, Int128 } from '../util/int';
import { UnionMode, DateUnit } from '../enum';
import { toArrayBufferView } from '../util/buffer';
import { BufferRegion, FieldNode } from '../ipc/metadata/message';

/** @ignore */
export interface VectorLoader extends Visitor {
    visit<T extends DataType>(node: Field<T> | T): Data<T>;
    visitMany<T extends DataType>(nodes: (Field<T> | T)[]): Data<T>[];
}

/** @ignore */
export class VectorLoader extends Visitor {
    private bytes: Uint8Array;
    private nodes: FieldNode[];
    private nodesIndex: number = -1;
    private buffers: BufferRegion[];
    private buffersIndex: number = -1;
    private dictionaries: Map<number, Vector<any>>;
    constructor(bytes: Uint8Array, nodes: FieldNode[], buffers: BufferRegion[], dictionaries: Map<number, Vector<any>>) {
        super();
        this.bytes = bytes;
        this.nodes = nodes;
        this.buffers = buffers;
        this.dictionaries = dictionaries;
    }

    public visit<T extends DataType>(node: Field<T> | T): Data<T> {
        return super.visit(node instanceof Field ? node.type : node);
    }

    public visitNull            <T extends type.Null>            (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Null(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitBool            <T extends type.Bool>            (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Bool(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitInt             <T extends type.Int>             (type: T, { length, nullCount } = this.nextFieldNode()) { return             Data.Int(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitFloat           <T extends type.Float>           (type: T, { length, nullCount } = this.nextFieldNode()) { return           Data.Float(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitUtf8            <T extends type.Utf8>            (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Utf8(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readData(type));                                   }
    public visitBinary          <T extends type.Binary>          (type: T, { length, nullCount } = this.nextFieldNode()) { return          Data.Binary(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readData(type));                                   }
    public visitFixedSizeBinary <T extends type.FixedSizeBinary> (type: T, { length, nullCount } = this.nextFieldNode()) { return Data.FixedSizeBinary(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitDate            <T extends type.Date_>           (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Date(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitTimestamp       <T extends type.Timestamp>       (type: T, { length, nullCount } = this.nextFieldNode()) { return       Data.Timestamp(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitTime            <T extends type.Time>            (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Time(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitDecimal         <T extends type.Decimal>         (type: T, { length, nullCount } = this.nextFieldNode()) { return         Data.Decimal(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitList            <T extends type.List>            (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.List(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.visit(type.children[0]));                          }
    public visitStruct          <T extends type.Struct>          (type: T, { length, nullCount } = this.nextFieldNode()) { return          Data.Struct(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.visitMany(type.children));                                                 }
    public visitUnion           <T extends type.Union>           (type: T                                              ) { return type.mode === UnionMode.Sparse ? this.visitSparseUnion(type as type.SparseUnion) : this.visitDenseUnion(type as type.DenseUnion);                                      }
    public visitDenseUnion      <T extends type.DenseUnion>      (type: T, { length, nullCount } = this.nextFieldNode()) { return           Data.Union(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readTypeIds(type), this.readOffsets(type), this.visitMany(type.children)); }
    public visitSparseUnion     <T extends type.SparseUnion>     (type: T, { length, nullCount } = this.nextFieldNode()) { return           Data.Union(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readTypeIds(type), this.visitMany(type.children));                         }
    public visitDictionary      <T extends type.Dictionary>      (type: T, { length, nullCount } = this.nextFieldNode()) { return      Data.Dictionary(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type.indices), this.readDictionary(type));                        }
    public visitInterval        <T extends type.Interval>        (type: T, { length, nullCount } = this.nextFieldNode()) { return        Data.Interval(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitFixedSizeList   <T extends type.FixedSizeList>   (type: T, { length, nullCount } = this.nextFieldNode()) { return   Data.FixedSizeList(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.visit(type.children[0]));                                                  }
    public visitMap             <T extends type.Map_>            (type: T, { length, nullCount } = this.nextFieldNode()) { return             Data.Map(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.visitMany(type.children));                                                 }

    protected nextFieldNode() { return this.nodes[++this.nodesIndex]; }
    protected nextBufferRange() { return this.buffers[++this.buffersIndex]; }
    protected readNullBitmap<T extends DataType>(type: T, nullCount: number, buffer = this.nextBufferRange()) {
        return nullCount > 0 && this.readData(type, buffer) || new Uint8Array(0);
    }
    protected readOffsets<T extends DataType>(type: T, buffer?: BufferRegion) { return this.readData(type, buffer); }
    protected readTypeIds<T extends DataType>(type: T, buffer?: BufferRegion) { return this.readData(type, buffer); }
    protected readData<T extends DataType>(_type: T, { length, offset } = this.nextBufferRange()) {
        return this.bytes.subarray(offset, offset + length);
    }
    protected readDictionary<T extends type.Dictionary>(type: T): Vector<T['dictionary']> {
        return this.dictionaries.get(type.id)!;
    }
}

/** @ignore */
export class JSONVectorLoader extends VectorLoader {
    private sources: any[][];
    constructor(sources: any[][], nodes: FieldNode[], buffers: BufferRegion[], dictionaries: Map<number, Vector<any>>) {
        super(new Uint8Array(0), nodes, buffers, dictionaries);
        this.sources = sources;
    }
    protected readNullBitmap<T extends DataType>(_type: T, nullCount: number, { offset } = this.nextBufferRange()) {
        return nullCount <= 0 ? new Uint8Array(0) : packBools(this.sources[offset]);
    }
    protected readOffsets<T extends DataType>(_type: T, { offset } = this.nextBufferRange()) {
        return toArrayBufferView(Uint8Array, toArrayBufferView(Int32Array, this.sources[offset]));
    }
    protected readTypeIds<T extends DataType>(type: T, { offset } = this.nextBufferRange()) {
        return toArrayBufferView(Uint8Array, toArrayBufferView(type.ArrayType, this.sources[offset]));
    }
    protected readData<T extends DataType>(type: T, { offset } = this.nextBufferRange()) {
        const { sources } = this;
        if (DataType.isTimestamp(type)) {
            return toArrayBufferView(Uint8Array, Int64.convertArray(sources[offset] as string[]));
        } else if ((DataType.isInt(type) || DataType.isTime(type)) && type.bitWidth === 64) {
            return toArrayBufferView(Uint8Array, Int64.convertArray(sources[offset] as string[]));
        } else if (DataType.isDate(type) && type.unit === DateUnit.MILLISECOND) {
            return toArrayBufferView(Uint8Array, Int64.convertArray(sources[offset] as string[]));
        } else if (DataType.isDecimal(type)) {
            return toArrayBufferView(Uint8Array, Int128.convertArray(sources[offset] as string[]));
        } else if (DataType.isBinary(type) || DataType.isFixedSizeBinary(type)) {
            return binaryDataFromJSON(sources[offset] as string[]);
        } else if (DataType.isBool(type)) {
            return packBools(sources[offset] as number[]);
        } else if (DataType.isUtf8(type)) {
            return encodeUtf8((sources[offset] as string[]).join(''));
        }
        return toArrayBufferView(Uint8Array, toArrayBufferView(type.ArrayType, sources[offset].map((x) => +x)));
    }
}

/** @ignore */
function binaryDataFromJSON(values: string[]) {
    // "DATA": ["49BC7D5B6C47D2","3F5FB6D9322026"]
    // There are definitely more efficient ways to do this... but it gets the
    // job done.
    const joined = values.join('');
    const data = new Uint8Array(joined.length / 2);
    for (let i = 0; i < joined.length; i += 2) {
        data[i >> 1] = parseInt(joined.substr(i, 2), 16);
    }
    return data;
}
