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

/* tslint:disable */
// Dynamically load an Arrow target build based on command line arguments

const path = require('path');
const target = process.env.TEST_TARGET!;
const format = process.env.TEST_MODULE!;
const useSrc = process.env.TEST_TS_SOURCE === `true`;

// these are duplicated in the gulpfile :<
const targets = [`es5`, `es2015`, `esnext`];
const formats = [`cjs`, `esm`, `cls`, `umd`];

function throwInvalidImportError(name: string, value: string, values: string[]) {
    throw new Error('Unrecognized ' + name + ' \'' + value + '\'. Please run tests with \'--' + name + ' <any of ' + values.join(', ') + '>\'');
}

let modulePath = ``;

if (useSrc) modulePath = '../src';
else if (target === `ts` || target === `apache-arrow`) modulePath = target;
else if (!~targets.indexOf(target)) throwInvalidImportError('target', target, targets);
else if (!~formats.indexOf(format)) throwInvalidImportError('module', format, formats);
else modulePath = path.join(target, format);

import { read, readAsync } from '../src/Arrow';
export { read, readAsync };
import { View,  VectorLike } from '../src/Arrow';
export { View,  VectorLike };
import { Table, Field, Schema, RecordBatch, Type } from '../src/Arrow';
export { Table, Field, Schema, RecordBatch, Type };

import { Int64, Uint64, Int128 } from '../src/Arrow';
export { Int64, Uint64, Int128 };
import { TypedArray, TypedArrayConstructor, IntBitWidth, TimeBitWidth } from '../src/Arrow';
export { TypedArray, TypedArrayConstructor, IntBitWidth, TimeBitWidth };

import * as Arrow_ from '../src/Arrow';
// import { data as data_, type as type_, vector as vector_ } from '../src/Arrow';

export let Arrow = require(path.resolve(`./targets`, modulePath, `Arrow`)) as typeof Arrow_;
export default Arrow;

// export namespace data {
//     export import Data = data_.Data;
//     export import BaseData = data_.BaseData;
//     export import FlatData = data_.FlatData;
//     export import BoolData = data_.BoolData;
//     export import FlatListData = data_.FlatListData;
//     export import DictionaryData = data_.DictionaryData;
//     export import NestedData = data_.NestedData;
//     export import ListData = data_.ListData;
//     export import UnionData = data_.UnionData;
//     export import SparseUnionData = data_.SparseUnionData;
//     export import DenseUnionData = data_.DenseUnionData;
// }

// export namespace type {
//     export import DataType = type_.DataType;
//     export import Null = type_.Null;
//     export import Bool = type_.Bool;
//     export import Decimal = type_.Decimal;
//     export import Int = type_.Int;
//     export import Int8 = type_.Int8;
//     export import Int16 = type_.Int16;
//     export import Int32 = type_.Int32;
//     export import Int64 = type_.Int64;
//     export import Uint8 = type_.Uint8;
//     export import Uint16 = type_.Uint16;
//     export import Uint32 = type_.Uint32;
//     export import Uint64 = type_.Uint64;
//     export import Float = type_.Float;
//     export import Float16 = type_.Float16;
//     export import Float32 = type_.Float32;
//     export import Float64 = type_.Float64;
//     export import Date_ = type_.Date_;
//     export import Time = type_.Time;
//     export import Timestamp = type_.Timestamp;
//     export import Interval = type_.Interval;
//     export import Binary = type_.Binary;
//     export import FixedSizeBinary = type_.FixedSizeBinary;
//     export import Utf8 = type_.Utf8;
//     export import List = type_.List;
//     export import FixedSizeList = type_.FixedSizeList;
//     export import Struct = type_.Struct;
//     export import Union = type_.Union;
//     export import Map_ = type_.Map_;
//     export import Dictionary = type_.Dictionary;
// }

// export namespace vector {
//     export import Vector = vector_.Vector;
//     export import FlatVector = vector_.FlatVector;
//     export import ListVectorBase = vector_.ListVectorBase;
//     export import NestedVector = vector_.NestedVector;
//     export import NullVector = vector_.NullVector;
//     export import BoolVector = vector_.BoolVector;
//     export import IntVector = vector_.IntVector;
//     export import FloatVector = vector_.FloatVector;
//     export import DateVector = vector_.DateVector;
//     export import DecimalVector = vector_.DecimalVector;
//     export import TimeVector = vector_.TimeVector;
//     export import TimestampVector = vector_.TimestampVector;
//     export import IntervalVector = vector_.IntervalVector;
//     export import BinaryVector = vector_.BinaryVector;
//     export import FixedSizeBinaryVector = vector_.FixedSizeBinaryVector;
//     export import Utf8Vector = vector_.Utf8Vector;
//     export import ListVector = vector_.ListVector;
//     export import FixedSizeListVector = vector_.FixedSizeListVector;
//     export import MapVector = vector_.MapVector;
//     export import StructVector = vector_.StructVector;
//     export import UnionVector = vector_.UnionVector;
//     export import DictionaryVector = vector_.DictionaryVector;
// }

