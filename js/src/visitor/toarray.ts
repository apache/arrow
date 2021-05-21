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
import { TypeToDataType } from '../interfaces';
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
    visit<T extends Data>(node: T): T['TArray'];
    visitMany<T extends Data>(nodes: T[]): T['TArray'][];
    getVisitFn<T extends DataType>(node: Data<T> | T): (data: Data<T>) => T['TArray'];
    getVisitFn<T extends Type>(node: T): (data: Data<TypeToDataType<T>>) => TypeToDataType<T>['TArray'];
    visitNull                                    <T extends Null>                (data: Data<T>): T['TArray'];
    visitBool                                    <T extends Bool>                (data: Data<T>): T['TArray'];
    visitInt                                     <T extends Int>                 (data: Data<T>): T['TArray'];
    visitInt8                                    <T extends Int8>                (data: Data<T>): T['TArray'];
    visitInt16                                   <T extends Int16>               (data: Data<T>): T['TArray'];
    visitInt32                                   <T extends Int32>               (data: Data<T>): T['TArray'];
    visitInt64                                   <T extends Int64>               (data: Data<T>): T['TArray'];
    visitUint8                                   <T extends Uint8>               (data: Data<T>): T['TArray'];
    visitUint16                                  <T extends Uint16>              (data: Data<T>): T['TArray'];
    visitUint32                                  <T extends Uint32>              (data: Data<T>): T['TArray'];
    visitUint64                                  <T extends Uint64>              (data: Data<T>): T['TArray'];
    visitFloat                                   <T extends Float>               (data: Data<T>): T['TArray'];
    visitFloat16                                 <T extends Float16>             (data: Data<T>): T['TArray'];
    visitFloat32                                 <T extends Float32>             (data: Data<T>): T['TArray'];
    visitFloat64                                 <T extends Float64>             (data: Data<T>): T['TArray'];
    visitUtf8                                    <T extends Utf8>                (data: Data<T>): T['TArray'];
    visitBinary                                  <T extends Binary>              (data: Data<T>): T['TArray'];
    visitFixedSizeBinary                         <T extends FixedSizeBinary>     (data: Data<T>): T['TArray'];
    visitDate                                    <T extends Date_>               (data: Data<T>): T['TArray'];
    visitDateDay                                 <T extends DateDay>             (data: Data<T>): T['TArray'];
    visitDateMillisecond                         <T extends DateMillisecond>     (data: Data<T>): T['TArray'];
    visitTimestamp                               <T extends Timestamp>           (data: Data<T>): T['TArray'];
    visitTimestampSecond                         <T extends TimestampSecond>     (data: Data<T>): T['TArray'];
    visitTimestampMillisecond                    <T extends TimestampMillisecond>(data: Data<T>): T['TArray'];
    visitTimestampMicrosecond                    <T extends TimestampMicrosecond>(data: Data<T>): T['TArray'];
    visitTimestampNanosecond                     <T extends TimestampNanosecond> (data: Data<T>): T['TArray'];
    visitTime                                    <T extends Time>                (data: Data<T>): T['TArray'];
    visitTimeSecond                              <T extends TimeSecond>          (data: Data<T>): T['TArray'];
    visitTimeMillisecond                         <T extends TimeMillisecond>     (data: Data<T>): T['TArray'];
    visitTimeMicrosecond                         <T extends TimeMicrosecond>     (data: Data<T>): T['TArray'];
    visitTimeNanosecond                          <T extends TimeNanosecond>      (data: Data<T>): T['TArray'];
    visitDecimal                                 <T extends Decimal>             (data: Data<T>): T['TArray'];
    visitList                <R extends DataType, T extends List<R>>             (data: Data<T>): T['TArray'];
    visitStruct                                  <T extends Struct>              (data: Data<T>): T['TArray'];
    visitUnion                                   <T extends Union>               (data: Data<T>): T['TArray'];
    visitDenseUnion                              <T extends DenseUnion>          (data: Data<T>): T['TArray'];
    visitSparseUnion                             <T extends SparseUnion>         (data: Data<T>): T['TArray'];
    visitDictionary          <R extends DataType, T extends Dictionary<R>>       (data: Data<T>): T['TArray'];
    visitInterval                                <T extends Interval>            (data: Data<T>): T['TArray'];
    visitIntervalDayTime                         <T extends IntervalDayTime>     (data: Data<T>): T['TArray'];
    visitIntervalYearMonth                       <T extends IntervalYearMonth>   (data: Data<T>): T['TArray'];
    visitFixedSizeList       <R extends DataType, T extends FixedSizeList<R>>    (data: Data<T>): T['TArray'];
    visitMap                                     <T extends Map_>                (data: Data<T>): T['TArray'];
}

/** @ignore */
export class ToArrayVisitor extends Visitor {}

/** @ignore */
function arrayOfVector<T extends DataType>(data: Data<T>): T['TArray'] {

    const { type, length, stride } = data;

    // Fast case, return subarray if possible
    switch (type.typeId) {
        case Type.Int:
        case Type.Float:
        case Type.Decimal:
        case Type.Time:
        case Type.Timestamp:
            return data.values.subarray(0, length * stride);
    }

    // Otherwise if not primitive, slow copy
    return [...iteratorVisitor.visit(data)] as T['TArray'];
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
