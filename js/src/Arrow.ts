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
import * as vector_ from './vector';
import * as util_int_ from './util/int';
import * as util_bit_ from './util/bit';
import * as util_node from './util/node';
import * as visitor_ from './visitor';
import * as view_ from './vector/view';
import * as predicate_ from './predicate';
import { Vector } from './vector';
import { RecordBatch } from './recordbatch';
import { Schema, Field, Type } from './type';
import { Table, DataFrame, NextFunc, BindFunc, CountByResult } from './table';
import { fromReadableStream } from './ipc/reader/node';
import { read, readAsync, readStream } from './ipc/reader/arrow';
import { serializeFile, serializeStream } from './ipc/writer/binary';

export import View = vector_.View;
export import VectorLike = vector_.VectorLike;
export import TypedArray = type_.TypedArray;
export import IntBitWidth = type_.IntBitWidth;
export import TimeBitWidth = type_.TimeBitWidth;
export import TypedArrayConstructor = type_.TypedArrayConstructor;

export { fromReadableStream };
export { read, readAsync, readStream };
export { serializeFile, serializeStream };
export { Table, DataFrame, NextFunc, BindFunc, CountByResult };
export { Field, Schema, RecordBatch, Vector, Type };

export namespace util {
    export import Uint64 = util_int_.Uint64;
    export import Int64 = util_int_.Int64;
    export import Int128 = util_int_.Int128;
    export import packBools = util_bit_.packBools;
    export import PipeIterator = util_node.PipeIterator;
    export import AsyncPipeIterator = util_node.AsyncPipeIterator;
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

export namespace enum_ {
    export import Type = type_.ArrowType;
    export import DateUnit = type_.DateUnit;
    export import TimeUnit = type_.TimeUnit;
    export import Precision = type_.Precision;
    export import UnionMode = type_.UnionMode;
    export import VectorType = type_.VectorType;
    export import IntervalUnit = type_.IntervalUnit;
    export import MessageHeader = type_.MessageHeader;
    export import MetadataVersion = type_.MetadataVersion;
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

export namespace visitor {
    export import TypeVisitor = visitor_.TypeVisitor;
    export import VectorVisitor = visitor_.VectorVisitor;
}

export namespace view {
    export import ChunkedView = view_.ChunkedView;
    export import DictionaryView = view_.DictionaryView;
    export import ListView = view_.ListView;
    export import FixedSizeListView = view_.FixedSizeListView;
    export import BinaryView = view_.BinaryView;
    export import Utf8View = view_.Utf8View;
    export import UnionView = view_.UnionView;
    export import DenseUnionView = view_.DenseUnionView;
    export import NestedView = view_.NestedView;
    export import StructView = view_.StructView;
    export import MapView = view_.MapView;
    export import FlatView = view_.FlatView;
    export import NullView = view_.NullView;
    export import BoolView = view_.BoolView;
    export import ValidityView = view_.ValidityView;
    export import PrimitiveView = view_.PrimitiveView;
    export import FixedSizeView = view_.FixedSizeView;
    export import Float16View = view_.Float16View;
    export import DateDayView = view_.DateDayView;
    export import DateMillisecondView = view_.DateMillisecondView;
    export import TimestampDayView = view_.TimestampDayView;
    export import TimestampSecondView = view_.TimestampSecondView;
    export import TimestampMillisecondView = view_.TimestampMillisecondView;
    export import TimestampMicrosecondView = view_.TimestampMicrosecondView;
    export import TimestampNanosecondView = view_.TimestampNanosecondView;
    export import IntervalYearMonthView = view_.IntervalYearMonthView;
    export import IntervalYearView = view_.IntervalYearView;
    export import IntervalMonthView = view_.IntervalMonthView;
}

export namespace predicate {
    export import col = predicate_.col;
    export import lit = predicate_.lit;
    export import custom = predicate_.custom;

    export import Or = predicate_.Or;
    export import Col = predicate_.Col;
    export import And = predicate_.And;
    export import Not = predicate_.Not;
    export import GTeq = predicate_.GTeq;
    export import LTeq = predicate_.LTeq;
    export import Value = predicate_.Value;
    export import Equals = predicate_.Equals;
    export import Literal = predicate_.Literal;
    export import Predicate = predicate_.Predicate;

    export import PredicateFunc = predicate_.PredicateFunc;
}

/* These exports are needed for the closure and uglify umd targets */
try {
    let Arrow: any = eval('exports');
    if (Arrow && typeof Arrow === 'object') {
        // string indexers tell closure and uglify not to rename these properties
        Arrow['data'] = data;
        Arrow['type'] = type;
        Arrow['util'] = util;
        Arrow['view'] = view;
        Arrow['enum_'] = enum_;
        Arrow['vector'] = vector;
        Arrow['visitor'] = visitor;
        Arrow['predicate'] = predicate;

        Arrow['read'] = read;
        Arrow['readAsync'] = readAsync;
        Arrow['readStream'] = readStream;
        Arrow['fromReadableStream'] = fromReadableStream;

        Arrow['serializeFile'] = serializeFile;
        Arrow['serializeStream'] = serializeStream;

        Arrow['Type'] = Type;
        Arrow['Field'] = Field;
        Arrow['Schema'] = Schema;
        Arrow['Vector'] = Vector;
        Arrow['RecordBatch'] = RecordBatch;

        Arrow['Table'] = Table;
        Arrow['CountByResult'] = CountByResult;
    }
} catch (e) { /* not the UMD bundle */ }
/* end umd exports */

// closure compiler erases static properties/methods:
// https://github.com/google/closure-compiler/issues/1776
// set them via string indexers to save them from the mangler
Schema['from'] = Schema.from;
Table['from'] = Table.from;
Table['fromAsync'] = Table.fromAsync;
Table['fromStruct'] = Table.fromStruct;
Table['empty'] = Table.empty;
Vector['create'] = Vector.create;
RecordBatch['from'] = RecordBatch.from;

util_int_.Uint64['add'] = util_int_.Uint64.add;
util_int_.Uint64['multiply'] = util_int_.Uint64.multiply;

util_int_.Int64['add'] = util_int_.Int64.add;
util_int_.Int64['multiply'] = util_int_.Int64.multiply;
util_int_.Int64['fromString'] = util_int_.Int64.fromString;

util_int_.Int128['add'] = util_int_.Int128.add;
util_int_.Int128['multiply'] = util_int_.Int128.multiply;
util_int_.Int128['fromString'] = util_int_.Int128.fromString;

data_.ChunkedData['computeOffsets'] = data_.ChunkedData.computeOffsets;

(type_.Type as any)['NONE'] = type_.Type.NONE;
(type_.Type as any)['Null'] = type_.Type.Null;
(type_.Type as any)['Int'] = type_.Type.Int;
(type_.Type as any)['Float'] = type_.Type.Float;
(type_.Type as any)['Binary'] = type_.Type.Binary;
(type_.Type as any)['Utf8'] = type_.Type.Utf8;
(type_.Type as any)['Bool'] = type_.Type.Bool;
(type_.Type as any)['Decimal'] = type_.Type.Decimal;
(type_.Type as any)['Date'] = type_.Type.Date;
(type_.Type as any)['Time'] = type_.Type.Time;
(type_.Type as any)['Timestamp'] = type_.Type.Timestamp;
(type_.Type as any)['Interval'] = type_.Type.Interval;
(type_.Type as any)['List'] = type_.Type.List;
(type_.Type as any)['Struct'] = type_.Type.Struct;
(type_.Type as any)['Union'] = type_.Type.Union;
(type_.Type as any)['FixedSizeBinary'] = type_.Type.FixedSizeBinary;
(type_.Type as any)['FixedSizeList'] = type_.Type.FixedSizeList;
(type_.Type as any)['Map'] = type_.Type.Map;
(type_.Type as any)['Dictionary'] = type_.Type.Dictionary;
(type_.Type as any)['DenseUnion'] = type_.Type.DenseUnion;
(type_.Type as any)['SparseUnion'] = type_.Type.SparseUnion;

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

vector_.BoolVector['from'] = vector_.BoolVector.from;
vector_.IntVector['from'] = vector_.IntVector.from;
vector_.FloatVector['from'] = vector_.FloatVector.from;

visitor_.TypeVisitor['visitTypeInline'] = visitor_.TypeVisitor.visitTypeInline;
visitor_.VectorVisitor['visitTypeInline'] = visitor_.VectorVisitor.visitTypeInline;