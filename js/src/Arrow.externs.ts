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

/**
 * @fileoverview Closure Compiler externs for Arrow
 * @externs
 * @suppress {duplicate,checkTypes}
 */
/** @type {symbol} */
Symbol.iterator;
/** @type {symbol} */
Symbol.asyncIterator;

let Table = function() {};
/** @type {?} */
(<any> Table).from;
/** @type {?} */
(<any> Table).fromAsync;
/** @type {?} */
(<any> Table).empty;
/** @type {?} */
Table.prototype.columns;
/** @type {?} */
Table.prototype.length;
/** @type {?} */
Table.prototype.select;
/** @type {?} */
Table.prototype.toString;

let Vector = function() {};
/** @type {?} */
(<any> Vector).create;
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

let DataType = function() {};
/** @type {?} */
(<any> DataType).isNull;
/** @type {?} */
(<any> DataType).isInt;
/** @type {?} */
(<any> DataType).isFloat;
/** @type {?} */
(<any> DataType).isBinary;
/** @type {?} */
(<any> DataType).isUtf8;
/** @type {?} */
(<any> DataType).isBool;
/** @type {?} */
(<any> DataType).isDecimal;
/** @type {?} */
(<any> DataType).isDate;
/** @type {?} */
(<any> DataType).isTime;
/** @type {?} */
(<any> DataType).isTimestamp;
/** @type {?} */
(<any> DataType).isInterval;
/** @type {?} */
(<any> DataType).isList;
/** @type {?} */
(<any> DataType).isStruct;
/** @type {?} */
(<any> DataType).isUnion;
/** @type {?} */
(<any> DataType).isDenseUnion;
/** @type {?} */
(<any> DataType).isSparseUnion;
/** @type {?} */
(<any> DataType).isFixedSizeBinary;
/** @type {?} */
(<any> DataType).isFixedSizeList;
/** @type {?} */
(<any> DataType).isMap;
/** @type {?} */
(<any> DataType).isDictionary;

let BaseData = function() {};
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

let FlatData = function() {};
/** @type {?} */
FlatData.prototype.values;

let FlatListData = function() {};
/** @type {?} */
FlatListData.prototype.values;
/** @type {?} */
FlatListData.prototype.valueOffsets;

let DictionaryData = function() {};
/** @type {?} */
DictionaryData.prototype.indicies;
/** @type {?} */
DictionaryData.prototype.dictionary;

let ListData = function() {};
/** @type {?} */
ListData.prototype.values;
/** @type {?} */
ListData.prototype.valueOffsets;

let UnionData = function() {};
/** @type {?} */
UnionData.prototype.typeIds;

let DenseUnionData = function() {};
/** @type {?} */
DenseUnionData.prototype.valueOffsets;

let ChunkedData = function() {};
/** @type {?} */
(<any> ChunkedData).computeOffsets;

let FlatVector = function() {};
/** @type {?} */
FlatVector.prototype.values;

let ListVectorBase = function() {};
/** @type {?} */
ListVectorBase.prototype.values;
/** @type {?} */
ListVectorBase.prototype.valueOffsets;
/** @type {?} */
ListVectorBase.prototype.getValueOffset;
/** @type {?} */
ListVectorBase.prototype.getValueLength;

let NestedVector = function() {};
/** @type {?} */
NestedVector.prototype.childData;
/** @type {?} */
NestedVector.prototype.getChildAt;

let DictionaryVector = function() {};
/** @type {?} */
DictionaryVector.prototype.getKey;
/** @type {?} */
DictionaryVector.prototype.getValue;

let FlatView = function() {};
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

let NullView = function() {};
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

let BoolView = function() {};
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

let ValidityView = function() {};
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

let DictionaryView = function() {};
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

let ListViewBase = function() {};
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

let NestedView = function() {};
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

let ChunkedView = function() {};
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

let TypeVisitor = function() {};
/** @type {?} */
(<any> TypeVisitor).visitTypeInline;
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

let VectorVisitor = function() {};
/** @type {?} */
(<any> VectorVisitor).visitTypeInline;
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