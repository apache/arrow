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
Table.fromStruct = function() {};
/** @type {?} */
Table.empty = function() {};
/** @type {?} */
Table.prototype.schema;
/** @type {?} */
Table.prototype.length;
/** @type {?} */
Table.prototype.numCols;
/** @type {?} */
Table.prototype.get;
/** @type {?} */
Table.prototype.getColumn;
/** @type {?} */
Table.prototype.getColumnAt;
/** @type {?} */
Table.prototype.getColumnIndex;
/** @type {?} */
Table.prototype.toArray;
/** @type {?} */
Table.prototype.select;
/** @type {?} */
Table.prototype.rowsToString;
/** @type {?} */
Table.prototype.batchesUnion;
/** @type {?} */
Table.prototype.batches;
/** @type {?} */
Table.prototype.countBy;
/** @type {?} */
Table.prototype.scan;

var CountByResult = function() {};
/** @type {?} */
CountByResult.prototype.asJSON;

var col = function () {};
var lit = function () {};
var custom = function () {};

var Value = function() {};
/** @type {?} */
Value.prototype.ge;
/** @type {?} */
Value.prototype.le;
/** @type {?} */
Value.prototype.eq;
/** @type {?} */
Value.prototype.lt;
/** @type {?} */
Value.prototype.gt;
/** @type {?} */
Value.prototype.ne;

var Col = function() {};
/** @type {?} */
Col.prototype.bind;
var Or = function() {};
var And = function() {};
var Not = function() {};
var GTeq = function () {};
/** @type {?} */
GTeq.prototype.and;
/** @type {?} */
GTeq.prototype.or;
var LTeq = function () {};
/** @type {?} */
LTeq.prototype.and;
/** @type {?} */
LTeq.prototype.or;
var Equals = function () {};
/** @type {?} */
Equals.prototype.and;
/** @type {?} */
Equals.prototype.or;
var Predicate = function() {};
/** @type {?} */
Predicate.prototype.bind;
/** @type {?} */
Predicate.prototype.and;
/** @type {?} */
Predicate.prototype.or;
/** @type {?} */
Predicate.prototype.not;
/** @type {?} */
Predicate.prototype.ands;
var Literal = function() {};

var TableToStringIterator = function() {};
/** @type {?} */
TableToStringIterator.prototype.pipe;

var RecordBatch = function() {};
/** @type {?} */
RecordBatch.from = function() {};
/** @type {?} */
RecordBatch.prototype.numCols;
/** @type {?} */
RecordBatch.prototype.length;
/** @type {?} */
RecordBatch.prototype.schema;
/** @type {?} */
RecordBatch.prototype.columns;
/** @type {?} */
RecordBatch.prototype.select;

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

var packBools = function() {};

var Type = function() {};
/** @type {?} */
Type.NONE = function() {};
/** @type {?} */
Type.Null = function() {};
/** @type {?} */
Type.Int = function() {};
/** @type {?} */
Type.Float = function() {};
/** @type {?} */
Type.Binary = function() {};
/** @type {?} */
Type.Utf8 = function() {};
/** @type {?} */
Type.Bool = function() {};
/** @type {?} */
Type.Decimal = function() {};
/** @type {?} */
Type.Date = function() {};
/** @type {?} */
Type.Time = function() {};
/** @type {?} */
Type.Timestamp = function() {};
/** @type {?} */
Type.Interval = function() {};
/** @type {?} */
Type.List = function() {};
/** @type {?} */
Type.Struct = function() {};
/** @type {?} */
Type.Union = function() {};
/** @type {?} */
Type.FixedSizeBinary = function() {};
/** @type {?} */
Type.FixedSizeList = function() {};
/** @type {?} */
Type.Map = function() {};
/** @type {?} */
Type.Dictionary = function() {};
/** @type {?} */
Type.DenseUnion = function() {};
/** @type {?} */
Type.SparseUnion = function() {};

var DateUnit = function() {};
/** @type {?} */
DateUnit.DAY = function() {};
/** @type {?} */
DateUnit.MILLISECOND = function() {};
var TimeUnit = function() {};
/** @type {?} */
TimeUnit.SECOND = function() {};
/** @type {?} */
TimeUnit.MILLISECOND = function() {};
/** @type {?} */
TimeUnit.MICROSECOND = function() {};
/** @type {?} */
TimeUnit.NANOSECOND = function() {};
var Precision = function() {};
/** @type {?} */
Precision.HALF = function() {};
/** @type {?} */
Precision.SINGLE = function() {};
/** @type {?} */
Precision.DOUBLE = function() {};
var UnionMode = function() {};
/** @type {?} */
UnionMode.Sparse = function() {};
/** @type {?} */
UnionMode.Dense = function() {};
var VectorType = function() {};
/** @type {?} */
VectorType.OFFSET = function() {};
/** @type {?} */
VectorType.DATA = function() {};
/** @type {?} */
VectorType.VALIDITY = function() {};
/** @type {?} */
VectorType.TYPE = function() {};
var IntervalUnit = function() {};
/** @type {?} */
IntervalUnit.YEAR_MONTH = function() {};
/** @type {?} */
IntervalUnit.DAY_TIME = function() {};
var MessageHeader = function() {};
/** @type {?} */
MessageHeader.NONE = function() {};
/** @type {?} */
MessageHeader.Schema = function() {};
/** @type {?} */
MessageHeader.DictionaryBatch = function() {};
/** @type {?} */
MessageHeader.RecordBatch = function() {};
/** @type {?} */
MessageHeader.Tensor = function() {};
var MetadataVersion = function() {};
/** @type {?} */
MetadataVersion.V1 = function() {};
/** @type {?} */
MetadataVersion.V2 = function() {};
/** @type {?} */
MetadataVersion.V3 = function() {};
/** @type {?} */
MetadataVersion.V4 = function() {};

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
/** @type {?} */
Schema.from = function() {};
/** @type {?} */
Schema.prototype.fields;
/** @type {?} */
Schema.prototype.version;
/** @type {?} */
Schema.prototype.metadata;
/** @type {?} */
Schema.prototype.dictionaries;
/** @type {?} */
Schema.prototype.select;
var Field = function() {};
/** @type {?} */
Field.prototype.name;
/** @type {?} */
Field.prototype.type;
/** @type {?} */
Field.prototype.nullable;
/** @type {?} */
Field.prototype.metadata;
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
DictionaryData.prototype.indices;
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
/** @type {?} */
FlatVector.prototype.lows;
/** @type {?} */
FlatVector.prototype.highs;
/** @type {?} */
FlatVector.prototype.asInt32;

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
/** @type {?} */
BoolVector.from = function() {};
/** @type {?} */
BoolVector.prototype.values;
var IntVector = function() {};
/** @type {?} */
IntVector.from = function() {};

var FloatVector = function() {};
/** @type {?} */
FloatVector.from = function() {};

var DateVector = function() {};
/** @type {?} */
DateVector.prototype.asEpochMilliseconds;
var DecimalVector = function() {};
var TimeVector = function() {};
var TimestampVector = function() {};
/** @type {?} */
TimestampVector.prototype.asEpochMilliseconds;
var IntervalVector = function() {};
var BinaryVector = function() {};
/** @type {?} */
BinaryVector.prototype.asUtf8;
var FixedSizeBinaryVector = function() {};
var Utf8Vector = function() {};
/** @type {?} */
Utf8Vector.prototype.asBinary;
var ListVector = function() {};
var FixedSizeListVector = function() {};
var MapVector = function() {};
/** @type {?} */
MapVector.prototype.asStruct;
var StructVector = function() {};
/** @type {?} */
StructVector.prototype.asMap;
var UnionVector = function() {};

var DictionaryVector = function() {};
/** @type {?} */
DictionaryVector.prototype.indices;
/** @type {?} */
DictionaryVector.prototype.dictionary;
/** @type {?} */
DictionaryVector.prototype.getKey;
/** @type {?} */
DictionaryVector.prototype.getValue;
/** @type {?} */
DictionaryVector.prototype.reverseLookup;

var FlatView = function() {};
/** @type {?} */
FlatView.prototype.get;
/** @type {?} */
FlatView.prototype.clone;
/** @type {?} */
FlatView.prototype.isValid;
/** @type {?} */
FlatView.prototype.toArray;
/** @type {?} */
FlatView.prototype.set;

var PrimitiveView = function() {};
/** @type {?} */
PrimitiveView.prototype.size;
/** @type {?} */
PrimitiveView.prototype.clone;

var NullView = function() {};
/** @type {?} */
NullView.prototype.get;
/** @type {?} */
NullView.prototype.clone;
/** @type {?} */
NullView.prototype.isValid;
/** @type {?} */
NullView.prototype.toArray;
/** @type {?} */
NullView.prototype.set;

var BoolView = function() {};
/** @type {?} */
BoolView.prototype.get;
/** @type {?} */
BoolView.prototype.clone;
/** @type {?} */
BoolView.prototype.isValid;
/** @type {?} */
BoolView.prototype.toArray;
/** @type {?} */
BoolView.prototype.set;

var ValidityView = function() {};
/** @type {?} */
ValidityView.prototype.get;
/** @type {?} */
ValidityView.prototype.clone;
/** @type {?} */
ValidityView.prototype.isValid;
/** @type {?} */
ValidityView.prototype.toArray;
/** @type {?} */
ValidityView.prototype.set;

var DictionaryView = function() {};
/** @type {?} */
DictionaryView.prototype.get;
/** @type {?} */
DictionaryView.prototype.clone;
/** @type {?} */
DictionaryView.prototype.isValid;
/** @type {?} */
DictionaryView.prototype.toArray;
/** @type {?} */
DictionaryView.prototype.set;

var ListViewBase = function() {};
/** @type {?} */
ListViewBase.prototype.get;
/** @type {?} */
ListViewBase.prototype.clone;
/** @type {?} */
ListViewBase.prototype.isValid;
/** @type {?} */
ListViewBase.prototype.toArray;
/** @type {?} */
ListViewBase.prototype.set;

var NestedView = function() {};
/** @type {?} */
NestedView.prototype.get;
/** @type {?} */
NestedView.prototype.clone;
/** @type {?} */
NestedView.prototype.isValid;
/** @type {?} */
NestedView.prototype.toArray;
/** @type {?} */
NestedView.prototype.set;

var ChunkedView = function() {};
/** @type {?} */
ChunkedView.prototype.get;
/** @type {?} */
ChunkedView.prototype.clone;
/** @type {?} */
ChunkedView.prototype.isValid;
/** @type {?} */
ChunkedView.prototype.toArray;
/** @type {?} */
ChunkedView.prototype.set;

var ListView = function() {};
var FixedSizeListView = function() {};
var BinaryView = function() {};
var Utf8View = function() {};
var UnionView = function() {};
var DenseUnionView = function() {};
var StructView = function() {};
var MapView = function() {};
var NullView = function() {};
var FixedSizeView = function() {};
var Float16View = function() {};
var DateDayView = function() {};
var DateMillisecondView = function() {};
var TimestampDayView = function() {};
var TimestampSecondView = function() {};
var TimestampMillisecondView = function() {};
var TimestampMicrosecondView = function() {};
var TimestampNanosecondView = function() {};
var IntervalYearMonthView = function() {};
var IntervalYearView = function() {};
var IntervalMonthView = function() {};

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
VectorVisitor.prototype.visitNull;
/** @type {?} */
VectorVisitor.prototype.visitBool;
/** @type {?} */
VectorVisitor.prototype.visitInt;
/** @type {?} */
VectorVisitor.prototype.visitFloat;
/** @type {?} */
VectorVisitor.prototype.visitUtf8;
/** @type {?} */
VectorVisitor.prototype.visitBinary;
/** @type {?} */
VectorVisitor.prototype.visitFixedSizeBinary;
/** @type {?} */
VectorVisitor.prototype.visitDate;
/** @type {?} */
VectorVisitor.prototype.visitTimestamp;
/** @type {?} */
VectorVisitor.prototype.visitTime;
/** @type {?} */
VectorVisitor.prototype.visitDecimal;
/** @type {?} */
VectorVisitor.prototype.visitList;
/** @type {?} */
VectorVisitor.prototype.visitStruct;
/** @type {?} */
VectorVisitor.prototype.visitUnion;
/** @type {?} */
VectorVisitor.prototype.visitDictionary;
/** @type {?} */
VectorVisitor.prototype.visitInterval;
/** @type {?} */
VectorVisitor.prototype.visitFixedSizeList;
/** @type {?} */
VectorVisitor.prototype.visitMap;
