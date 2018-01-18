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

import * as type_ from './type';
import * as data from './data';
import * as vector from './vector';

import { Table, RecordBatch } from './table';
import { Uint64, Int64, Int128 } from './util/int';
import { View, Vector, VectorLike, } from './vector';
import { read, readAsync } from './ipc/reader/arrow';
import { Schema, Field, DataType, Type } from './type';

// closure compiler always erases static method names:
// https://github.com/google/closure-compiler/issues/1776
// set them via string indexers to save them from the mangler
Table['from'] = Table.from;
Table['fromAsync'] = Table.fromAsync;
Table['empty'] = Table.empty;
Vector['create'] = Vector.create;

export import IntBitWidth = type_.IntBitWidth;
export import TimeBitWidth = type_.TimeBitWidth;

export import TypedArray = type_.TypedArray;
export import TypedArrayConstructor = type_.TypedArrayConstructor;

export { read, readAsync };
export { View,  VectorLike };
export { Uint64, Int64, Int128 };
export { type_ as type, data, vector };
export { Table, Field, Schema, Vector, DataType, RecordBatch, Type };

/* These exports are needed for the closure and uglify umd targets */
try {
    let Arrow: any = eval('exports');
    if (Arrow && typeof Arrow === 'object') {
        // string indexers tell closure and uglify not to rename these properties
        Arrow['data'] = data;
        Arrow['type'] = type_;
        Arrow['read'] = read;
        Arrow['readAsync'] = readAsync;
        Arrow['Int64'] = Int64;
        Arrow['Uint64'] = Uint64;
        Arrow['Int128'] = Int128;

        Arrow['Table'] = Table;
        Arrow['Field'] = Field;
        Arrow['Schema'] = Schema;
        Arrow['Vector'] = Vector;
        Arrow['DataType'] = DataType;
        Arrow['RecordBatch'] = RecordBatch;

        Arrow['data']['BaseData'] = data.BaseData;
        Arrow['data']['FlatData'] = data.FlatData;
        Arrow['data']['BoolData'] = data.BoolData;
        Arrow['data']['FlatListData'] = data.FlatListData;
        Arrow['data']['DictionaryData'] = data.DictionaryData;
        Arrow['data']['NestedData'] = data.NestedData;
        Arrow['data']['ListData'] = data.ListData;
        Arrow['data']['UnionData'] = data.UnionData;
        Arrow['data']['SparseUnionData'] = data.SparseUnionData;
        Arrow['data']['DenseUnionData'] = data.DenseUnionData;

        Arrow['type']['DataType'] = type_.DataType;
        Arrow['type']['Null'] = type_.Null;
        Arrow['type']['Bool'] = type_.Bool;
        Arrow['type']['Decimal'] = type_.Decimal;
        Arrow['type']['Int'] = type_.Int;
        Arrow['type']['Int8'] = type_.Int8;
        Arrow['type']['Int16'] = type_.Int16;
        Arrow['type']['Int32'] = type_.Int32;
        Arrow['type']['Int64'] = type_.Int64;
        Arrow['type']['Uint8'] = type_.Uint8;
        Arrow['type']['Uint16'] = type_.Uint16;
        Arrow['type']['Uint32'] = type_.Uint32;
        Arrow['type']['Uint64'] = type_.Uint64;
        Arrow['type']['Float'] = type_.Float;
        Arrow['type']['Float16'] = type_.Float16;
        Arrow['type']['Float32'] = type_.Float32;
        Arrow['type']['Float64'] = type_.Float64;
        Arrow['type']['Date_'] = type_.Date_;
        Arrow['type']['Time'] = type_.Time;
        Arrow['type']['Timestamp'] = type_.Timestamp;
        Arrow['type']['Interval'] = type_.Interval;
        Arrow['type']['Binary'] = type_.Binary;
        Arrow['type']['FixedSizeBinary'] = type_.FixedSizeBinary;
        Arrow['type']['Utf8'] = type_.Utf8;
        Arrow['type']['List'] = type_.List;
        Arrow['type']['FixedSizeList'] = type_.FixedSizeList;
        Arrow['type']['Struct'] = type_.Struct;
        Arrow['type']['Union'] = type_.Union;
        Arrow['type']['Map_'] = type_.Map_;
        Arrow['type']['Dictionary'] = type_.Dictionary;

        Arrow['vector']['Vector'] = vector.Vector;
        Arrow['vector']['FlatVector'] = vector.FlatVector;
        Arrow['vector']['ListVectorBase'] = vector.ListVectorBase;
        Arrow['vector']['NestedVector'] = vector.NestedVector;
        Arrow['vector']['NullVector'] = vector.NullVector;
        Arrow['vector']['BoolVector'] = vector.BoolVector;
        Arrow['vector']['IntVector'] = vector.IntVector;
        Arrow['vector']['FloatVector'] = vector.FloatVector;
        Arrow['vector']['DateVector'] = vector.DateVector;
        Arrow['vector']['DecimalVector'] = vector.DecimalVector;
        Arrow['vector']['TimeVector'] = vector.TimeVector;
        Arrow['vector']['TimestampVector'] = vector.TimestampVector;
        Arrow['vector']['IntervalVector'] = vector.IntervalVector;
        Arrow['vector']['BinaryVector'] = vector.BinaryVector;
        Arrow['vector']['FixedSizeBinaryVector'] = vector.FixedSizeBinaryVector;
        Arrow['vector']['Utf8Vector'] = vector.Utf8Vector;
        Arrow['vector']['ListVector'] = vector.ListVector;
        Arrow['vector']['FixedSizeListVector'] = vector.FixedSizeListVector;
        Arrow['vector']['MapVector'] = vector.MapVector;
        Arrow['vector']['StructVector'] = vector.StructVector;
        Arrow['vector']['UnionVector'] = vector.UnionVector;
        Arrow['vector']['DictionaryVector'] = vector.DictionaryVector;
    }
} catch (e) { /* not the UMD bundle */ }
/* end umd exports */
