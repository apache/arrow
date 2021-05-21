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
import { BitIterator } from '../util/bit';
import { Type, Precision } from '../enum';
import { TypeToDataType } from '../interfaces';
import { instance as getVisitor } from './get';
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
export interface IteratorVisitor extends Visitor {
    visit<T extends Data>(node: T): IterableIterator<T['TValue'] | null>;
    visitMany <T extends Data>(nodes: T[]): IterableIterator<T['TValue'] | null>[];
    getVisitFn<T extends DataType>(node: Data<T> | T): (data: Data<T>) => IterableIterator<T['TValue'] | null>;
    getVisitFn<T extends Type>(node: T): (data: Data<TypeToDataType<T>>) => IterableIterator<TypeToDataType<T>['TValue'] | null>;
    visitNull                 <T extends Null>                 (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitBool                 <T extends Bool>                 (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitInt                  <T extends Int>                  (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitInt8                 <T extends Int8>                 (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitInt16                <T extends Int16>                (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitInt32                <T extends Int32>                (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitInt64                <T extends Int64>                (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitUint8                <T extends Uint8>                (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitUint16               <T extends Uint16>               (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitUint32               <T extends Uint32>               (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitUint64               <T extends Uint64>               (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitFloat                <T extends Float>                (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitFloat16              <T extends Float16>              (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitFloat32              <T extends Float32>              (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitFloat64              <T extends Float64>              (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitUtf8                 <T extends Utf8>                 (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitBinary               <T extends Binary>               (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitFixedSizeBinary      <T extends FixedSizeBinary>      (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitDate                 <T extends Date_>                (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitDateDay              <T extends DateDay>              (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitDateMillisecond      <T extends DateMillisecond>      (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTimestamp            <T extends Timestamp>            (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTimestampSecond      <T extends TimestampSecond>      (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTimestampMillisecond <T extends TimestampMillisecond> (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTimestampMicrosecond <T extends TimestampMicrosecond> (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTimestampNanosecond  <T extends TimestampNanosecond>  (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTime                 <T extends Time>                 (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTimeSecond           <T extends TimeSecond>           (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTimeMillisecond      <T extends TimeMillisecond>      (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTimeMicrosecond      <T extends TimeMicrosecond>      (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitTimeNanosecond       <T extends TimeNanosecond>       (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitDecimal              <T extends Decimal>              (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitList                 <T extends List>                 (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitStruct               <T extends Struct>               (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitUnion                <T extends Union>                (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitDenseUnion           <T extends DenseUnion>           (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitSparseUnion          <T extends SparseUnion>          (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitDictionary           <T extends Dictionary>           (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitInterval             <T extends Interval>             (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitIntervalDayTime      <T extends IntervalDayTime>      (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitIntervalYearMonth    <T extends IntervalYearMonth>    (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitFixedSizeList        <T extends FixedSizeList>        (data: Data<T>): IterableIterator<T['TValue'] | null>;
    visitMap                  <T extends Map_>                 (data: Data<T>): IterableIterator<T['TValue'] | null>;
}

/** @ignore */
export class IteratorVisitor extends Visitor {}

/** @ignore */
function nullableIterator<T extends DataType>(data: Data<T>): IterableIterator<T['TValue'] | null> {
    const getFn = getVisitor.getVisitFn(data);
    return new BitIterator<T['TValue'] | null>(
        data.nullBitmap, data.offset, data.length, data,
        (vec: Data<T>, idx: number, nullByte: number, nullBit: number) =>
            ((nullByte & 1 << nullBit) !== 0) ? getFn(vec, idx) : null
    );
}

/** @ignore */
function vectorIterator<T extends DataType>(data: Data<T>): IterableIterator<T['TValue'] | null> {

    // If nullable, iterate manually
    if (data.nullCount > 0) {
        return nullableIterator<T>(data);
    }

    const { type, typeId, length } = data;

    // Fast case, defer to native iterators if possible
    if (data.stride === 1 && (
        (typeId === Type.Timestamp) ||
        (type instanceof Int && (type as Int).bitWidth !== 64) ||
        (type instanceof Time && (type as Time).bitWidth !== 64) ||
        (type instanceof Float && (type as Float).precision !== Precision.HALF)
    )) {
        return data.values.subarray(0, length)[Symbol.iterator]();
    }

    // Otherwise, iterate manually
    return (function* (getFn) {
        for (let index = -1; ++index < length;) {
            yield getFn(data, index);
        }
    })(getVisitor.getVisitFn(data));
}

IteratorVisitor.prototype.visitNull                 = vectorIterator;
IteratorVisitor.prototype.visitBool                 = vectorIterator;
IteratorVisitor.prototype.visitInt                  = vectorIterator;
IteratorVisitor.prototype.visitInt8                 = vectorIterator;
IteratorVisitor.prototype.visitInt16                = vectorIterator;
IteratorVisitor.prototype.visitInt32                = vectorIterator;
IteratorVisitor.prototype.visitInt64                = vectorIterator;
IteratorVisitor.prototype.visitUint8                = vectorIterator;
IteratorVisitor.prototype.visitUint16               = vectorIterator;
IteratorVisitor.prototype.visitUint32               = vectorIterator;
IteratorVisitor.prototype.visitUint64               = vectorIterator;
IteratorVisitor.prototype.visitFloat                = vectorIterator;
IteratorVisitor.prototype.visitFloat16              = vectorIterator;
IteratorVisitor.prototype.visitFloat32              = vectorIterator;
IteratorVisitor.prototype.visitFloat64              = vectorIterator;
IteratorVisitor.prototype.visitUtf8                 = vectorIterator;
IteratorVisitor.prototype.visitBinary               = vectorIterator;
IteratorVisitor.prototype.visitFixedSizeBinary      = vectorIterator;
IteratorVisitor.prototype.visitDate                 = vectorIterator;
IteratorVisitor.prototype.visitDateDay              = vectorIterator;
IteratorVisitor.prototype.visitDateMillisecond      = vectorIterator;
IteratorVisitor.prototype.visitTimestamp            = vectorIterator;
IteratorVisitor.prototype.visitTimestampSecond      = vectorIterator;
IteratorVisitor.prototype.visitTimestampMillisecond = vectorIterator;
IteratorVisitor.prototype.visitTimestampMicrosecond = vectorIterator;
IteratorVisitor.prototype.visitTimestampNanosecond  = vectorIterator;
IteratorVisitor.prototype.visitTime                 = vectorIterator;
IteratorVisitor.prototype.visitTimeSecond           = vectorIterator;
IteratorVisitor.prototype.visitTimeMillisecond      = vectorIterator;
IteratorVisitor.prototype.visitTimeMicrosecond      = vectorIterator;
IteratorVisitor.prototype.visitTimeNanosecond       = vectorIterator;
IteratorVisitor.prototype.visitDecimal              = vectorIterator;
IteratorVisitor.prototype.visitList                 = vectorIterator;
IteratorVisitor.prototype.visitStruct               = vectorIterator;
IteratorVisitor.prototype.visitUnion                = vectorIterator;
IteratorVisitor.prototype.visitDenseUnion           = vectorIterator;
IteratorVisitor.prototype.visitSparseUnion          = vectorIterator;
IteratorVisitor.prototype.visitDictionary           = vectorIterator;
IteratorVisitor.prototype.visitInterval             = vectorIterator;
IteratorVisitor.prototype.visitIntervalDayTime      = vectorIterator;
IteratorVisitor.prototype.visitIntervalYearMonth    = vectorIterator;
IteratorVisitor.prototype.visitFixedSizeList        = vectorIterator;
IteratorVisitor.prototype.visitMap                  = vectorIterator;

/** @ignore */
export const instance = new IteratorVisitor();
