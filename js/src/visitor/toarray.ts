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
import { Visitor } from '../visitor';
import { Vector } from '../interfaces';
import { Type, Precision } from '../enum';
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

export interface ToArrayVisitor extends Visitor {
    visit<T extends Vector>(node: T): T['TArray'];
    visitMany<T extends Vector>(nodes: T[]): T['TArray'][];
    getVisitFn<T extends Type>(node: T): (vector: Vector<T>) => Vector<T>['TArray'];
    getVisitFn<T extends DataType>(node: Vector<T> | Data<T> | T): (vector: Vector<T>) => Vector<T>['TArray'];
    visitNull                                    <T extends Null>                (vector: Vector<T>): Vector<T>['TArray'];
    visitBool                                    <T extends Bool>                (vector: Vector<T>): Vector<T>['TArray'];
    visitInt                                     <T extends Int>                 (vector: Vector<T>): Vector<T>['TArray'];
    visitInt8                                    <T extends Int8>                (vector: Vector<T>): Vector<T>['TArray'];
    visitInt16                                   <T extends Int16>               (vector: Vector<T>): Vector<T>['TArray'];
    visitInt32                                   <T extends Int32>               (vector: Vector<T>): Vector<T>['TArray'];
    visitInt64                                   <T extends Int64>               (vector: Vector<T>): Vector<T>['TArray'];
    visitUint8                                   <T extends Uint8>               (vector: Vector<T>): Vector<T>['TArray'];
    visitUint16                                  <T extends Uint16>              (vector: Vector<T>): Vector<T>['TArray'];
    visitUint32                                  <T extends Uint32>              (vector: Vector<T>): Vector<T>['TArray'];
    visitUint64                                  <T extends Uint64>              (vector: Vector<T>): Vector<T>['TArray'];
    visitFloat                                   <T extends Float>               (vector: Vector<T>): Vector<T>['TArray'];
    visitFloat16                                 <T extends Float16>             (vector: Vector<T>): Vector<T>['TArray'];
    visitFloat32                                 <T extends Float32>             (vector: Vector<T>): Vector<T>['TArray'];
    visitFloat64                                 <T extends Float64>             (vector: Vector<T>): Vector<T>['TArray'];
    visitUtf8                                    <T extends Utf8>                (vector: Vector<T>): Vector<T>['TArray'];
    visitBinary                                  <T extends Binary>              (vector: Vector<T>): Vector<T>['TArray'];
    visitFixedSizeBinary                         <T extends FixedSizeBinary>     (vector: Vector<T>): Vector<T>['TArray'];
    visitDate                                    <T extends Date_>               (vector: Vector<T>): Vector<T>['TArray'];
    visitDateDay                                 <T extends DateDay>             (vector: Vector<T>): Vector<T>['TArray'];
    visitDateMillisecond                         <T extends DateMillisecond>     (vector: Vector<T>): Vector<T>['TArray'];
    visitTimestamp                               <T extends Timestamp>           (vector: Vector<T>): Vector<T>['TArray'];
    visitTimestampSecond                         <T extends TimestampSecond>     (vector: Vector<T>): Vector<T>['TArray'];
    visitTimestampMillisecond                    <T extends TimestampMillisecond>(vector: Vector<T>): Vector<T>['TArray'];
    visitTimestampMicrosecond                    <T extends TimestampMicrosecond>(vector: Vector<T>): Vector<T>['TArray'];
    visitTimestampNanosecond                     <T extends TimestampNanosecond> (vector: Vector<T>): Vector<T>['TArray'];
    visitTime                                    <T extends Time>                (vector: Vector<T>): Vector<T>['TArray'];
    visitTimeSecond                              <T extends TimeSecond>          (vector: Vector<T>): Vector<T>['TArray'];
    visitTimeMillisecond                         <T extends TimeMillisecond>     (vector: Vector<T>): Vector<T>['TArray'];
    visitTimeMicrosecond                         <T extends TimeMicrosecond>     (vector: Vector<T>): Vector<T>['TArray'];
    visitTimeNanosecond                          <T extends TimeNanosecond>      (vector: Vector<T>): Vector<T>['TArray'];
    visitDecimal                                 <T extends Decimal>             (vector: Vector<T>): Vector<T>['TArray'];
    visitList                <R extends DataType, T extends List<R>>             (vector: Vector<T>): Vector<T>['TArray'];
    visitStruct                                  <T extends Struct>              (vector: Vector<T>): Vector<T>['TArray'];
    visitUnion                                   <T extends Union>               (vector: Vector<T>): Vector<T>['TArray'];
    visitDenseUnion                              <T extends DenseUnion>          (vector: Vector<T>): Vector<T>['TArray'];
    visitSparseUnion                             <T extends SparseUnion>         (vector: Vector<T>): Vector<T>['TArray'];
    visitDictionary          <R extends DataType, T extends Dictionary<R>>       (vector: Vector<T>): Vector<T>['TArray'];
    visitInterval                                <T extends Interval>            (vector: Vector<T>): Vector<T>['TArray'];
    visitIntervalDayTime                         <T extends IntervalDayTime>     (vector: Vector<T>): Vector<T>['TArray'];
    visitIntervalYearMonth                       <T extends IntervalYearMonth>   (vector: Vector<T>): Vector<T>['TArray'];
    visitFixedSizeList       <R extends DataType, T extends FixedSizeList<R>>    (vector: Vector<T>): Vector<T>['TArray'];
    visitMap                                     <T extends Map_>                (vector: Vector<T>): Vector<T>['TArray'];
}

export class ToArrayVisitor extends Visitor {}

/** @ignore */
function arrayOfVector<T extends DataType>(vector: Vector<T>): T['TArray'] {

    const { type, length, stride } = vector;

    // Fast case, return subarray if possible
    switch (type.typeId) {
        case Type.Int: case Type.Decimal:
        case Type.Time: case Type.Timestamp:
            return vector.values.subarray(0, length * stride);
        case Type.Float:
            return (type as Float).precision === Precision.HALF /* Precision.HALF */
                ? new Float32Array(vector[Symbol.iterator]())
                : vector.values.subarray(0, length * stride);
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

export const instance = new ToArrayVisitor();
