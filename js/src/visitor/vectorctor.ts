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
import { Type } from '../enum';
import { DataType } from '../type';
import { Visitor } from '../visitor';
import { VectorType, VectorCtor } from '../interfaces';

import { BinaryVector } from '../vector/binary';
import { BoolVector } from '../vector/bool';
import { DateVector, DateDayVector, DateMillisecondVector } from '../vector/date';
import { DecimalVector } from '../vector/decimal';
import { DictionaryVector } from '../vector/dictionary';
import { FixedSizeBinaryVector } from '../vector/fixedsizebinary';
import { FixedSizeListVector } from '../vector/fixedsizelist';
import { FloatVector, Float16Vector, Float32Vector, Float64Vector } from '../vector/float';
import { IntervalVector, IntervalDayTimeVector, IntervalYearMonthVector } from '../vector/interval';
import { IntVector, Int8Vector, Int16Vector, Int32Vector, Int64Vector, Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector } from '../vector/int';
import { ListVector } from '../vector/list';
import { MapVector } from '../vector/map';
import { NullVector } from '../vector/null';
import { StructVector } from '../vector/struct';
import { TimestampVector, TimestampSecondVector, TimestampMillisecondVector, TimestampMicrosecondVector, TimestampNanosecondVector } from '../vector/timestamp';
import { TimeVector, TimeSecondVector, TimeMillisecondVector, TimeMicrosecondVector, TimeNanosecondVector } from '../vector/time';
import { UnionVector, DenseUnionVector, SparseUnionVector } from '../vector/union';
import { Utf8Vector } from '../vector/utf8';

/** @ignore */
export interface GetVectorConstructor extends Visitor {
    visit<T extends Type>(node: T): VectorCtor<T>;
    visitMany <T extends Type>(nodes: T[]): VectorCtor<T>[];
    getVisitFn<T extends Type>(node: T): () => VectorCtor<T>;
    getVisitFn<T extends DataType>(node: VectorType<T> | Data<T> | T): () => VectorCtor<T>;
}

/** @ignore */
export class GetVectorConstructor extends Visitor {
    public visitNull                 () { return NullVector; }
    public visitBool                 () { return BoolVector; }
    public visitInt                  () { return IntVector; }
    public visitInt8                 () { return Int8Vector; }
    public visitInt16                () { return Int16Vector; }
    public visitInt32                () { return Int32Vector; }
    public visitInt64                () { return Int64Vector; }
    public visitUint8                () { return Uint8Vector; }
    public visitUint16               () { return Uint16Vector; }
    public visitUint32               () { return Uint32Vector; }
    public visitUint64               () { return Uint64Vector; }
    public visitFloat                () { return FloatVector; }
    public visitFloat16              () { return Float16Vector; }
    public visitFloat32              () { return Float32Vector; }
    public visitFloat64              () { return Float64Vector; }
    public visitUtf8                 () { return Utf8Vector; }
    public visitBinary               () { return BinaryVector; }
    public visitFixedSizeBinary      () { return FixedSizeBinaryVector; }
    public visitDate                 () { return DateVector; }
    public visitDateDay              () { return DateDayVector; }
    public visitDateMillisecond      () { return DateMillisecondVector; }
    public visitTimestamp            () { return TimestampVector; }
    public visitTimestampSecond      () { return TimestampSecondVector; }
    public visitTimestampMillisecond () { return TimestampMillisecondVector; }
    public visitTimestampMicrosecond () { return TimestampMicrosecondVector; }
    public visitTimestampNanosecond  () { return TimestampNanosecondVector; }
    public visitTime                 () { return TimeVector; }
    public visitTimeSecond           () { return TimeSecondVector; }
    public visitTimeMillisecond      () { return TimeMillisecondVector; }
    public visitTimeMicrosecond      () { return TimeMicrosecondVector; }
    public visitTimeNanosecond       () { return TimeNanosecondVector; }
    public visitDecimal              () { return DecimalVector; }
    public visitList                 () { return ListVector; }
    public visitStruct               () { return StructVector; }
    public visitUnion                () { return UnionVector; }
    public visitDenseUnion           () { return DenseUnionVector; }
    public visitSparseUnion          () { return SparseUnionVector; }
    public visitDictionary           () { return DictionaryVector; }
    public visitInterval             () { return IntervalVector; }
    public visitIntervalDayTime      () { return IntervalDayTimeVector; }
    public visitIntervalYearMonth    () { return IntervalYearMonthVector; }
    public visitFixedSizeList        () { return FixedSizeListVector; }
    public visitMap                  () { return MapVector; }
}

/** @ignore */
export const instance = new GetVectorConstructor();
