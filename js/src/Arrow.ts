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
import * as data_ from './data';
import { Vector } from './vector';
import * as vector_ from './vector';
import * as util_ from './util/int';
import { Schema, Field, Type } from './type';
import { Table, RecordBatch } from './recordbatch';
import { read, readAsync } from './ipc/reader/arrow';

export import View = vector_.View;
export import VectorLike = vector_.VectorLike;
export import TypedArray = type_.TypedArray;
export import IntBitWidth = type_.IntBitWidth;
export import TimeBitWidth = type_.TimeBitWidth;
export import TypedArrayConstructor = type_.TypedArrayConstructor;

export { read, readAsync };
export { Field, Schema, Table, RecordBatch, Vector, Type };

export namespace util {
    export import Uint64 = util_.Uint64;
    export import Int64 = util_.Int64;
    export import Int128 = util_.Int128;
}

export namespace data {
    export import BaseData = data_.BaseData;
    export import FlatData = data_.FlatData;
    export import BoolData = data_.BoolData;
    export import FlatListData = data_.FlatListData;
    export import DictionaryData = data_.DictionaryData;
    export import NestedData = data_.NestedData;
    export import ListData = data_.ListData;
    export import UnionData = data_.UnionData;
    export import SparseUnionData = data_.SparseUnionData;
    export import DenseUnionData = data_.DenseUnionData;
    export import ChunkedData = data_.ChunkedData;
}

export namespace type {
    export import Schema = type_.Schema;
    export import Field = type_.Field;
    export import Null = type_.Null;
    export import Int = type_.Int;
    export import Int8 = type_.Int8;
    export import Int16 = type_.Int16;
    export import Int32 = type_.Int32;
    export import Int64 = type_.Int64;
    export import Uint8 = type_.Uint8;
    export import Uint16 = type_.Uint16;
    export import Uint32 = type_.Uint32;
    export import Uint64 = type_.Uint64;
    export import Float = type_.Float;
    export import Float16 = type_.Float16;
    export import Float32 = type_.Float32;
    export import Float64 = type_.Float64;
    export import Binary = type_.Binary;
    export import Utf8 = type_.Utf8;
    export import Bool = type_.Bool;
    export import Decimal = type_.Decimal;
    export import Date_ = type_.Date_;
    export import Time = type_.Time;
    export import Timestamp = type_.Timestamp;
    export import Interval = type_.Interval;
    export import List = type_.List;
    export import Struct = type_.Struct;
    export import Union = type_.Union;
    export import DenseUnion = type_.DenseUnion;
    export import SparseUnion = type_.SparseUnion;
    export import FixedSizeBinary = type_.FixedSizeBinary;
    export import FixedSizeList = type_.FixedSizeList;
    export import Map_ = type_.Map_;
    export import Dictionary = type_.Dictionary;
}

export namespace vector {
    export import Vector = vector_.Vector;
    export import NullVector = vector_.NullVector;
    export import BoolVector = vector_.BoolVector;
    export import IntVector = vector_.IntVector;
    export import FloatVector = vector_.FloatVector;
    export import DateVector = vector_.DateVector;
    export import DecimalVector = vector_.DecimalVector;
    export import TimeVector = vector_.TimeVector;
    export import TimestampVector = vector_.TimestampVector;
    export import IntervalVector = vector_.IntervalVector;
    export import BinaryVector = vector_.BinaryVector;
    export import FixedSizeBinaryVector = vector_.FixedSizeBinaryVector;
    export import Utf8Vector = vector_.Utf8Vector;
    export import ListVector = vector_.ListVector;
    export import FixedSizeListVector = vector_.FixedSizeListVector;
    export import MapVector = vector_.MapVector;
    export import StructVector = vector_.StructVector;
    export import UnionVector = vector_.UnionVector;
    export import DictionaryVector = vector_.DictionaryVector;
}

/* These exports are needed for the closure and uglify umd targets */
try {
    let Arrow: any = eval('exports');
    if (Arrow && typeof Arrow === 'object') {
        // string indexers tell closure and uglify not to rename these properties
        Arrow['data'] = data;
        Arrow['type'] = type;
        Arrow['util'] = util;
        Arrow['vector'] = vector;

        Arrow['read'] = read;
        Arrow['readAsync'] = readAsync;

        Arrow['Field'] = Field;
        Arrow['Schema'] = Schema;
        Arrow['Table'] = Table;
        Arrow['RecordBatch'] = RecordBatch;
        Arrow['Vector'] = Vector;

    }
} catch (e) { /* not the UMD bundle */ }
/* end umd exports */

// closure compiler always erases static method names:
// https://github.com/google/closure-compiler/issues/1776
// set them via string indexers to save them from the mangler
Table['from'] = Table.from;
Table['fromAsync'] = Table.fromAsync;
Table['empty'] = Table.empty;
Vector['create'] = Vector.create;

util_.Uint64['add'] = util_.Uint64.add;
util_.Uint64['multiply'] = util_.Uint64.multiply;

util_.Int64['add'] = util_.Int64.add;
util_.Int64['multiply'] = util_.Int64.multiply;
util_.Int64['fromString'] = util_.Int64.fromString;

util_.Int128['add'] = util_.Int128.add;
util_.Int128['multiply'] = util_.Int128.multiply;
util_.Int128['fromString'] = util_.Int128.fromString;

data_.ChunkedData['computeOffsets'] = data_.ChunkedData.computeOffsets;

type_.DataType['isNull'] = type_.DataType.isNull;
type_.DataType['isInt'] = type_.DataType.isInt;
type_.DataType['isFloat'] = type_.DataType.isFloat;
type_.DataType['isBinary'] = type_.DataType.isBinary;
type_.DataType['isUtf8'] = type_.DataType.isUtf8;
type_.DataType['isBool'] = type_.DataType.isBool;
type_.DataType['isDecimal'] = type_.DataType.isDecimal;
type_.DataType['isDate'] = type_.DataType.isDate;
type_.DataType['isTime'] = type_.DataType.isTime;
type_.DataType['isTimestamp'] = type_.DataType.isTimestamp;
type_.DataType['isInterval'] = type_.DataType.isInterval;
type_.DataType['isList'] = type_.DataType.isList;
type_.DataType['isStruct'] = type_.DataType.isStruct;
type_.DataType['isUnion'] = type_.DataType.isUnion;
type_.DataType['isDenseUnion'] = type_.DataType.isDenseUnion;
type_.DataType['isSparseUnion'] = type_.DataType.isSparseUnion;
type_.DataType['isFixedSizeBinary'] = type_.DataType.isFixedSizeBinary;
type_.DataType['isFixedSizeList'] = type_.DataType.isFixedSizeList;
type_.DataType['isMap'] = type_.DataType.isMap;
type_.DataType['isDictionary'] = type_.DataType.isDictionary;
