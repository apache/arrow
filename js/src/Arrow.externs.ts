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
Table.prototype.rows;
/** @type {?} */
Table.prototype.cols;
/** @type {?} */
Table.prototype.getRow;
/** @type {?} */
Table.prototype.getCell;
/** @type {?} */
Table.prototype.getCellAt;
/** @type {?} */
Table.prototype.getColumn;
/** @type {?} */
Table.prototype.getColumnAt;
/** @type {?} */
Table.prototype.toString;

let Vector = function() {};
/** @type {?} */
Vector.prototype.length;
/** @type {?} */
Vector.prototype.name;
/** @type {?} */
Vector.prototype.type;
/** @type {?} */
Vector.prototype.props;
/** @type {?} */
Vector.prototype.get;
/** @type {?} */
Vector.prototype.concat;
/** @type {?} */
Vector.prototype.slice;

let TypedVector = function() {};
/** @type {?} */
TypedVector.prototype.arrayType;

let ValidityVector = function() {};
/** @type {?} */
(<any> ValidityVector).pack;
