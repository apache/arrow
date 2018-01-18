// @ts-nocheck
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

/**
 * @fileoverview Closure Compiler externs for Arrow
 * @externs
 * @suppress {duplicate,checkTypes}
 */
/** @type {symbol} */
Symbol.iterator;
/** @type {symbol} */
Symbol.asyncIterator;

var Table = function() {};
/** @type {?} */
Table.from = function() {};
/** @type {?} */
Table.fromAsync = function() {};
/** @type {?} */
Table.empty = function() {};
/** @type {?} */
Table.prototype.schema;
/** @type {?} */
Table.prototype.columns;
/** @type {?} */
Table.prototype.numCols;
/** @type {?} */
Table.prototype.numRows;
/** @type {?} */
Table.prototype.get;
/** @type {?} */
Table.prototype.toArray;
/** @type {?} */
Table.prototype.select;
/** @type {?} */
Table.prototype.rowsToString;

var TableToStringIterator = function() {};
/** @type {?} */
TableToStringIterator.prototype.pipe;

var RecordBatch = function() {};
/** @type {?} */
RecordBatch.prototype.numRows;
/** @type {?} */
RecordBatch.prototype.schema;
/** @type {?} */
RecordBatch.prototype.data;
/** @type {?} */
RecordBatch.prototype.columns;
/** @type {?} */
RecordBatch.prototype.numCols;
/** @type {?} */
RecordBatch.prototype.concat;

var Vector = function() {};
/** @type {?} */
Vector.create = function() {};
/** @type {?} */
Vector.prototype.data;
/** @type {?} */
Vector.prototype.type;
/** @type {?} */
Vector.prototype.length;
/** @type {?} */
Vector.prototype.nullCount;
/** @type {?} */
Vector.prototype.nullBitmap;
/** @type {?} */
Vector.prototype.isValid;
/** @type {?} */
Vector.prototype.get;
/** @type {?} */
Vector.prototype.set;
/** @type {?} */
Vector.prototype.setData;
/** @type {?} */
Vector.prototype.toArray;
/** @type {?} */
Vector.prototype.concat;
/** @type {?} */
Vector.prototype.slice;
/** @type {?} */
Vector.prototype.acceptTypeVisitor;

var BaseInt64 = function() {};
/** @type {?} */
BaseInt64.prototype.lessThan;
/** @type {?} */
BaseInt64.prototype.equals;
/** @type {?} */
BaseInt64.prototype.greaterThan;
/** @type {?} */
BaseInt64.prototype.hex;

var Uint64 = function() {};
/** @type {?} */
Uint64.add = function() {};
/** @type {?} */
Uint64.multiply = function() {};
/** @type {?} */
Uint64.prototype.times;
/** @type {?} */
Uint64.prototype.plus

var Int64 = function() {};
/** @type {?} */
Int64.add = function() {};
/** @type {?} */
Int64.multiply = function() {};
/** @type {?} */
Int64.fromString = function() {};
/** @type {?} */
Int64.prototype.negate
/** @type {?} */
Int64.prototype.times
/** @type {?} */
Int64.prototype.plus
/** @type {?} */
Int64.prototype.lessThan

var Int128 = function() {};
/** @type {?} */
Int128.add = function() {};
/** @type {?} */
Int128.multiply = function() {};
/** @type {?} */
Int128.fromString = function() {};
/** @type {?} */
Int128.prototype.negate
/** @type {?} */
Int128.prototype.times
/** @type {?} */
Int128.prototype.plus
/** @type {?} */
Int128.prototype.hex

var DataType = function() {};
/** @type {?} */
DataType.isNull = function() {};
/** @type {?} */
DataType.isInt = function() {};
/** @type {?} */
DataType.isFloat = function() {};
/** @type {?} */
DataType.isBinary = function() {};
/** @type {?} */
DataType.isUtf8 = function() {};
/** @type {?} */
DataType.isBool = function() {};
/** @type {?} */
DataType.isDecimal = function() {};
/** @type {?} */
DataType.isDate = function() {};
/** @type {?} */
DataType.isTime = function() {};
/** @type {?} */
DataType.isTimestamp = function() {};
/** @type {?} */
DataType.isInterval = function() {};
/** @type {?} */
DataType.isList = function() {};
/** @type {?} */
DataType.isStruct = function() {};
/** @type {?} */
DataType.isUnion = function() {};
/** @type {?} */
DataType.isDenseUnion = function() {};
/** @type {?} */
DataType.isSparseUnion = function() {};
/** @type {?} */
DataType.isFixedSizeBinary = function() {};
/** @type {?} */
DataType.isFixedSizeList = function() {};
/** @type {?} */
DataType.isMap = function() {};
/** @type {?} */
DataType.isDictionary = function() {};
/** @type {?} */
DataType.prototype.ArrayType;

var Schema = function() {};
var Field = function() {};
var Null = function() {};
var Int8 = function() {};
var Int16 = function() {};
var Int32 = function() {};
var Int64 = function() {};
var Uint8 = function() {};
var Uint16 = function() {};
var Uint32 = function() {};
var Uint64 = function() {};
var Float16 = function() {};
var Float32 = function() {};
var Float64 = function() {};
var Binary = function() {};
var Utf8 = function() {};
var Bool = function() {};
var Decimal = function() {};
var Date_ = function() {};
var Time = function() {};
var Timestamp = function() {};
var Interval = function() {};
var List = function() {};
var Struct = function() {};
var Union = function() {};
var DenseUnion = function() {};
var SparseUnion = function() {};
var FixedSizeBinary = function() {};
var FixedSizeList = function() {};
var Map_ = function() {};
var Dictionary = function() {};

var BaseData = function() {};
/** @type {?} */
BaseData.prototype.type;
/** @type {?} */
BaseData.prototype.clone;
/** @type {?} */
BaseData.prototype.slice;
/** @type {?} */
BaseData.prototype.length;
/** @type {?} */
BaseData.prototype.offset;
/** @type {?} */
BaseData.prototype.typeId;
/** @type {?} */
BaseData.prototype.childData;
/** @type {?} */
BaseData.prototype.nullBitmap;
/** @type {?} */
BaseData.prototype.nullCount;

var BoolData = function() {};
var NestedData = function() {};
var SparseUnionData = function() {};
var ChunkedData = function() {};

var FlatData = function() {};
/** @type {?} */
FlatData.prototype.values;

var FlatListData = function() {};
/** @type {?} */
FlatListData.prototype.values;
/** @type {?} */
FlatListData.prototype.valueOffsets;

var DictionaryData = function() {};
/** @type {?} */
DictionaryData.prototype.indicies;
/** @type {?} */
DictionaryData.prototype.dictionary;

var ListData = function() {};
/** @type {?} */
ListData.prototype.values;
/** @type {?} */
ListData.prototype.valueOffsets;

var UnionData = function() {};
/** @type {?} */
UnionData.prototype.typeIds;

var DenseUnionData = function() {};
/** @type {?} */
DenseUnionData.prototype.valueOffsets;

var ChunkedData = function() {};
/** @type {?} */
ChunkedData.computeOffsets = function() {};

var FlatVector = function() {};
/** @type {?} */
FlatVector.prototype.values;

var ListVectorBase = function() {};
/** @type {?} */
ListVectorBase.prototype.values;
/** @type {?} */
ListVectorBase.prototype.valueOffsets;
/** @type {?} */
ListVectorBase.prototype.getValueOffset;
/** @type {?} */
ListVectorBase.prototype.getValueLength;

var NestedVector = function() {};
/** @type {?} */
NestedVector.prototype.childData;
/** @type {?} */
NestedVector.prototype.getChildAt;

var NullVector = function() {};
var BoolVector = function() {};
var IntVector = function() {};
var FloatVector = function() {};
var DateVector = function() {};
var DecimalVector = function() {};
var TimeVector = function() {};
var TimestampVector = function() {};
var IntervalVector = function() {};
var BinaryVector = function() {};
var FixedSizeBinaryVector = function() {};
var Utf8Vector = function() {};
var ListVector = function() {};
var FixedSizeListVector = function() {};
var MapVector = function() {};
var StructVector = function() {};
var UnionVector = function() {};

var DictionaryVector = function() {};
/** @type {?} */
DictionaryVector.prototype.getKey;
/** @type {?} */
DictionaryVector.prototype.getValue;

var FlatView = function() {};
/** @type {?} */
FlatView.prototype.get;
/** @type {?} */
FlatView.prototype.isValid;
/** @type {?} */
FlatView.prototype.toArray;
/** @type {?} */
FlatView.prototype.set;
/** @type {?} */
FlatView.prototype.setData;

var NullView = function() {};
/** @type {?} */
NullView.prototype.get;
/** @type {?} */
NullView.prototype.isValid;
/** @type {?} */
NullView.prototype.toArray;
/** @type {?} */
NullView.prototype.set;
/** @type {?} */
NullView.prototype.setData;

var BoolView = function() {};
/** @type {?} */
BoolView.prototype.get;
/** @type {?} */
BoolView.prototype.isValid;
/** @type {?} */
BoolView.prototype.toArray;
/** @type {?} */
BoolView.prototype.set;
/** @type {?} */
BoolView.prototype.setData;

var ValidityView = function() {};
/** @type {?} */
ValidityView.prototype.get;
/** @type {?} */
ValidityView.prototype.isValid;
/** @type {?} */
ValidityView.prototype.toArray;
/** @type {?} */
ValidityView.prototype.set;
/** @type {?} */
ValidityView.prototype.setData;

var DictionaryView = function() {};
/** @type {?} */
DictionaryView.prototype.get;
/** @type {?} */
DictionaryView.prototype.isValid;
/** @type {?} */
DictionaryView.prototype.toArray;
/** @type {?} */
DictionaryView.prototype.set;
/** @type {?} */
DictionaryView.prototype.setData;

var ListViewBase = function() {};
/** @type {?} */
ListViewBase.prototype.get;
/** @type {?} */
ListViewBase.prototype.isValid;
/** @type {?} */
ListViewBase.prototype.toArray;
/** @type {?} */
ListViewBase.prototype.set;
/** @type {?} */
ListViewBase.prototype.setData;

var NestedView = function() {};
/** @type {?} */
NestedView.prototype.get;
/** @type {?} */
NestedView.prototype.isValid;
/** @type {?} */
NestedView.prototype.toArray;
/** @type {?} */
NestedView.prototype.set;
/** @type {?} */
NestedView.prototype.setData;

var ChunkedView = function() {};
/** @type {?} */
ChunkedView.prototype.get;
/** @type {?} */
ChunkedView.prototype.isValid;
/** @type {?} */
ChunkedView.prototype.toArray;
/** @type {?} */
ChunkedView.prototype.set;
/** @type {?} */
ChunkedView.prototype.setData;

var TypeVisitor = function() {};
/** @type {?} */
TypeVisitor.visitTypeInline = function() {};
/** @type {?} */
TypeVisitor.prototype.visit;
/** @type {?} */
TypeVisitor.prototype.visitMany;
/** @type {?} */
TypeVisitor.prototype.visitNull;
/** @type {?} */
TypeVisitor.prototype.visitBool;
/** @type {?} */
TypeVisitor.prototype.visitInt;
/** @type {?} */
TypeVisitor.prototype.visitFloat;
/** @type {?} */
TypeVisitor.prototype.visitUtf8;
/** @type {?} */
TypeVisitor.prototype.visitBinary;
/** @type {?} */
TypeVisitor.prototype.visitFixedSizeBinary;
/** @type {?} */
TypeVisitor.prototype.visitDate;
/** @type {?} */
TypeVisitor.prototype.visitTimestamp;
/** @type {?} */
TypeVisitor.prototype.visitTime;
/** @type {?} */
TypeVisitor.prototype.visitDecimal;
/** @type {?} */
TypeVisitor.prototype.visitList;
/** @type {?} */
TypeVisitor.prototype.visitStruct;
/** @type {?} */
TypeVisitor.prototype.visitUnion;
/** @type {?} */
TypeVisitor.prototype.visitDictionary;
/** @type {?} */
TypeVisitor.prototype.visitInterval;
/** @type {?} */
TypeVisitor.prototype.visitFixedSizeList;
/** @type {?} */
TypeVisitor.prototype.visitMap;

var VectorVisitor = function() {};
/** @type {?} */
VectorVisitor.visitTypeInline = function() {};
/** @type {?} */
VectorVisitor.prototype.visit;
/** @type {?} */
VectorVisitor.prototype.visitMany;
/** @type {?} */
VectorVisitor.prototype.visitNullVector;
/** @type {?} */
VectorVisitor.prototype.visitBoolVector;
/** @type {?} */
VectorVisitor.prototype.visitIntVector;
/** @type {?} */
VectorVisitor.prototype.visitFloatVector;
/** @type {?} */
VectorVisitor.prototype.visitUtf8Vector;
/** @type {?} */
VectorVisitor.prototype.visitBinaryVector;
/** @type {?} */
VectorVisitor.prototype.visitFixedSizeBinaryVector;
/** @type {?} */
VectorVisitor.prototype.visitDateVector;
/** @type {?} */
VectorVisitor.prototype.visitTimestampVector;
/** @type {?} */
VectorVisitor.prototype.visitTimeVector;
/** @type {?} */
VectorVisitor.prototype.visitDecimalVector;
/** @type {?} */
VectorVisitor.prototype.visitListVector;
/** @type {?} */
VectorVisitor.prototype.visitStructVector;
/** @type {?} */
VectorVisitor.prototype.visitUnionVector;
/** @type {?} */
VectorVisitor.prototype.visitDictionaryVector;
/** @type {?} */
VectorVisitor.prototype.visitIntervalVector;
/** @type {?} */
VectorVisitor.prototype.visitFixedSizeListVector;
/** @type {?} */
VectorVisitor.prototype.visitMapVector;