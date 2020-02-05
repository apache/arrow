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
import { Visitor } from '../visitor';
import { VectorType } from '../interfaces';
import { instance as iteratorVisitor } from './iterator';
import {
    DataType, Dictionary,
    Bool, Null, Utf8, Binary, Decimal, FixedSizeBinary, List, FixedSizeList, Map_, Struct,
    Float, Float16, Float32, Float64,
    Int, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64,
    Date_, DateDay, DateMillisecond,
    Interval, IntervalDayTime, IntervalYearMonth,
    Time, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    Timestamp, TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Union, DenseUnion, SparseUnion,
} from '../type';

/** @ignore */
export interface ToArrayVisitor extends Visitor {
    visit<T extends VectorType>(node: T): T['TArray'];
    visitMany<T extends VectorType>(nodes: T[]): T['TArray'][];
    getVisitFn<T extends Type>(node: T): (vector: VectorType<T>) => VectorType<T>['TArray'];
    getVisitFn<T extends DataType>(node: VectorType<T> | Data<T> | T): (vector: VectorType<T>) => VectorType<T>['TArray'];
    visitNull                                    <T extends Null>                (vector: VectorType<T>): VectorType<T>['TArray'];
    visitBool                                    <T extends Bool>                (vector: VectorType<T>): VectorType<T>['TArray'];
    visitInt                                     <T extends Int>                 (vector: VectorType<T>): VectorType<T>['TArray'];
    visitInt8                                    <T extends Int8>                (vector: VectorType<T>): VectorType<T>['TArray'];
    visitInt16                                   <T extends Int16>               (vector: VectorType<T>): VectorType<T>['TArray'];
    visitInt32                                   <T extends Int32>               (vector: VectorType<T>): VectorType<T>['TArray'];
    visitInt64                                   <T extends Int64>               (vector: VectorType<T>): VectorType<T>['TArray'];
    visitUint8                                   <T extends Uint8>               (vector: VectorType<T>): VectorType<T>['TArray'];
    visitUint16                                  <T extends Uint16>              (vector: VectorType<T>): VectorType<T>['TArray'];
    visitUint32                                  <T extends Uint32>              (vector: VectorType<T>): VectorType<T>['TArray'];
    visitUint64                                  <T extends Uint64>              (vector: VectorType<T>): VectorType<T>['TArray'];
    visitFloat                                   <T extends Float>               (vector: VectorType<T>): VectorType<T>['TArray'];
    visitFloat16                                 <T extends Float16>             (vector: VectorType<T>): VectorType<T>['TArray'];
    visitFloat32                                 <T extends Float32>             (vector: VectorType<T>): VectorType<T>['TArray'];
    visitFloat64                                 <T extends Float64>             (vector: VectorType<T>): VectorType<T>['TArray'];
    visitUtf8                                    <T extends Utf8>                (vector: VectorType<T>): VectorType<T>['TArray'];
    visitBinary                                  <T extends Binary>              (vector: VectorType<T>): VectorType<T>['TArray'];
    visitFixedSizeBinary                         <T extends FixedSizeBinary>     (vector: VectorType<T>): VectorType<T>['TArray'];
    visitDate                                    <T extends Date_>               (vector: VectorType<T>): VectorType<T>['TArray'];
    visitDateDay                                 <T extends DateDay>             (vector: VectorType<T>): VectorType<T>['TArray'];
    visitDateMillisecond                         <T extends DateMillisecond>     (vector: VectorType<T>): VectorType<T>['TArray'];
    visitTimestamp                               <T extends Timestamp>           (vector: VectorType<T>): VectorType<T>['TArray'];
    visitTimestampSecond                         <T extends TimestampSecond>     (vector: VectorType<T>): VectorType<T>['TArray'];
    visitTimestampMillisecond                    <T extends TimestampMillisecond>(vector: VectorType<T>): VectorType<T>['TArray'];
    visitTimestampMicrosecond                    <T extends TimestampMicrosecond>(vector: VectorType<T>): VectorType<T>['TArray'];
    visitTimestampNanosecond                     <T extends TimestampNanosecond> (vector: VectorType<T>): VectorType<T>['TArray'];
    visitTime                                    <T extends Time>                (vector: VectorType<T>): VectorType<T>['TArray'];
    visitTimeSecond                              <T extends TimeSecond>          (vector: VectorType<T>): VectorType<T>['TArray'];
    visitTimeMillisecond                         <T extends TimeMillisecond>     (vector: VectorType<T>): VectorType<T>['TArray'];
    visitTimeMicrosecond                         <T extends TimeMicrosecond>     (vector: VectorType<T>): VectorType<T>['TArray'];
    visitTimeNanosecond                          <T extends TimeNanosecond>      (vector: VectorType<T>): VectorType<T>['TArray'];
    visitDecimal                                 <T extends Decimal>             (vector: VectorType<T>): VectorType<T>['TArray'];
    visitList                <R extends DataType, T extends List<R>>             (vector: VectorType<T>): VectorType<T>['TArray'];
    visitStruct                                  <T extends Struct>              (vector: VectorType<T>): VectorType<T>['TArray'];
    visitUnion                                   <T extends Union>               (vector: VectorType<T>): VectorType<T>['TArray'];
    visitDenseUnion                              <T extends DenseUnion>          (vector: VectorType<T>): VectorType<T>['TArray'];
    visitSparseUnion                             <T extends SparseUnion>         (vector: VectorType<T>): VectorType<T>['TArray'];
    visitDictionary          <R extends DataType, T extends Dictionary<R>>       (vector: VectorType<T>): VectorType<T>['TArray'];
    visitInterval                                <T extends Interval>            (vector: VectorType<T>): VectorType<T>['TArray'];
    visitIntervalDayTime                         <T extends IntervalDayTime>     (vector: VectorType<T>): VectorType<T>['TArray'];
    visitIntervalYearMonth                       <T extends IntervalYearMonth>   (vector: VectorType<T>): VectorType<T>['TArray'];
    visitFixedSizeList       <R extends DataType, T extends FixedSizeList<R>>    (vector: VectorType<T>): VectorType<T>['TArray'];
    visitMap                                     <T extends Map_>                (vector: VectorType<T>): VectorType<T>['TArray'];
}

/** @ignore */
export class ToArrayVisitor extends Visitor {}

/** @ignore */
function arrayOfVector<T extends DataType>(vector: VectorType<T>): T['TArray'] {

    const { type, length, stride } = vector;

    // Fast case, return subarray if possible
    switch (type.typeId) {
        case Type.Int:
        case Type.Float: case Type.Decimal:
        case Type.Time: case Type.Timestamp:
            return vector.values.subarray(0, length * stride);
    }

    // Otherwise if not primitive, slow copy
    return [...iteratorVisitor.visit(vector)] as T['TArray'];
}

ToArrayVisitor.prototype.visitNull                 = arrayOfVector;
ToArrayVisitor.prototype.visitBool                 = arrayOfVector;
ToArrayVisitor.prototype.visitInt                  = arrayOfVector;
ToArrayVisitor.prototype.visitInt8                 = arrayOfVector;
ToArrayVisitor.prototype.visitInt16                = arrayOfVector;
ToArrayVisitor.prototype.visitInt32                = arrayOfVector;
ToArrayVisitor.prototype.visitInt64                = arrayOfVector;
ToArrayVisitor.prototype.visitUint8                = arrayOfVector;
ToArrayVisitor.prototype.visitUint16               = arrayOfVector;
ToArrayVisitor.prototype.visitUint32               = arrayOfVector;
ToArrayVisitor.prototype.visitUint64               = arrayOfVector;
ToArrayVisitor.prototype.visitFloat                = arrayOfVector;
ToArrayVisitor.prototype.visitFloat16              = arrayOfVector;
ToArrayVisitor.prototype.visitFloat32              = arrayOfVector;
ToArrayVisitor.prototype.visitFloat64              = arrayOfVector;
ToArrayVisitor.prototype.visitUtf8                 = arrayOfVector;
ToArrayVisitor.prototype.visitBinary               = arrayOfVector;
ToArrayVisitor.prototype.visitFixedSizeBinary      = arrayOfVector;
ToArrayVisitor.prototype.visitDate                 = arrayOfVector;
ToArrayVisitor.prototype.visitDateDay              = arrayOfVector;
ToArrayVisitor.prototype.visitDateMillisecond      = arrayOfVector;
ToArrayVisitor.prototype.visitTimestamp            = arrayOfVector;
ToArrayVisitor.prototype.visitTimestampSecond      = arrayOfVector;
ToArrayVisitor.prototype.visitTimestampMillisecond = arrayOfVector;
ToArrayVisitor.prototype.visitTimestampMicrosecond = arrayOfVector;
ToArrayVisitor.prototype.visitTimestampNanosecond  = arrayOfVector;
ToArrayVisitor.prototype.visitTime                 = arrayOfVector;
ToArrayVisitor.prototype.visitTimeSecond           = arrayOfVector;
ToArrayVisitor.prototype.visitTimeMillisecond      = arrayOfVector;
ToArrayVisitor.prototype.visitTimeMicrosecond      = arrayOfVector;
ToArrayVisitor.prototype.visitTimeNanosecond       = arrayOfVector;
ToArrayVisitor.prototype.visitDecimal              = arrayOfVector;
ToArrayVisitor.prototype.visitList                 = arrayOfVector;
ToArrayVisitor.prototype.visitStruct               = arrayOfVector;
ToArrayVisitor.prototype.visitUnion                = arrayOfVector;
ToArrayVisitor.prototype.visitDenseUnion           = arrayOfVector;
ToArrayVisitor.prototype.visitSparseUnion          = arrayOfVector;
ToArrayVisitor.prototype.visitDictionary           = arrayOfVector;
ToArrayVisitor.prototype.visitInterval             = arrayOfVector;
ToArrayVisitor.prototype.visitIntervalDayTime      = arrayOfVector;
ToArrayVisitor.prototype.visitIntervalYearMonth    = arrayOfVector;
ToArrayVisitor.prototype.visitFixedSizeList        = arrayOfVector;
ToArrayVisitor.prototype.visitMap                  = arrayOfVector;

/** @ignore */
export const instance = new ToArrayVisitor();
