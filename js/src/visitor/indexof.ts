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
import { Vector } from '../interfaces';
import { getBool, iterateBits } from '../util/bit';
import { createElementComparator } from '../util/vector';
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

export interface IndexOfVisitor extends Visitor {
    visit<T extends Vector>  (node: T, value: T['TValue'] | null, index?: number): number;
    visitMany <T extends Vector>  (nodes: T[], values: (T['TValue'] | null)[], indices: (number | undefined)[]): number[];
    getVisitFn<T extends Type>    (node: T): (vector: Vector<T>, value: Vector<T>['TValue'] | null, index?: number) => number;
    getVisitFn<T extends DataType>(node: Vector<T> | Data<T> | T): (vector: Vector<T>, value:         T['TValue'] | null, index?: number) => number;
    visitNull                 <T extends Null>                (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitBool                 <T extends Bool>                (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitInt                  <T extends Int>                 (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitInt8                 <T extends Int8>                (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitInt16                <T extends Int16>               (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitInt32                <T extends Int32>               (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitInt64                <T extends Int64>               (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitUint8                <T extends Uint8>               (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitUint16               <T extends Uint16>              (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitUint32               <T extends Uint32>              (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitUint64               <T extends Uint64>              (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitFloat                <T extends Float>               (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitFloat16              <T extends Float16>             (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitFloat32              <T extends Float32>             (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitFloat64              <T extends Float64>             (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitUtf8                 <T extends Utf8>                (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitBinary               <T extends Binary>              (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitFixedSizeBinary      <T extends FixedSizeBinary>     (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitDate                 <T extends Date_>               (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitDateDay              <T extends DateDay>             (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitDateMillisecond      <T extends DateMillisecond>     (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTimestamp            <T extends Timestamp>           (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTimestampSecond      <T extends TimestampSecond>     (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTimestampMillisecond <T extends TimestampMillisecond>(vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTimestampMicrosecond <T extends TimestampMicrosecond>(vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTimestampNanosecond  <T extends TimestampNanosecond> (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTime                 <T extends Time>                (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTimeSecond           <T extends TimeSecond>          (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTimeMillisecond      <T extends TimeMillisecond>     (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTimeMicrosecond      <T extends TimeMicrosecond>     (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitTimeNanosecond       <T extends TimeNanosecond>      (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitDecimal              <T extends Decimal>             (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitList                 <T extends List>                (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitStruct               <T extends Struct>              (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitUnion                <T extends Union>               (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitDenseUnion           <T extends DenseUnion>          (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitSparseUnion          <T extends SparseUnion>         (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitDictionary           <T extends Dictionary>          (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitInterval             <T extends Interval>            (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitIntervalDayTime      <T extends IntervalDayTime>     (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitIntervalYearMonth    <T extends IntervalYearMonth>   (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitFixedSizeList        <T extends FixedSizeList>       (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
    visitMap                  <T extends Map_>                (vector: Vector<T>, value: T['TValue'] | null, index?: number): number;
}

export class IndexOfVisitor extends Visitor {
}

/** @ignore */
function nullIndexOf(vector: Vector<Null>, searchElement?: null) {
    // if you're looking for nulls and the vector isn't empty, we've got 'em!
    return searchElement === null && vector.length > 0 ? 0 : -1;
}

/** @ignore */
function indexOfNull<T extends DataType>(vector: Vector<T>, fromIndex?: number): number {
    const { nullBitmap } = vector;
    if (!nullBitmap || vector.nullCount <= 0) {
        return -1;
    }
    let i = 0;
    for (const isValid of iterateBits(nullBitmap, vector.data.offset + (fromIndex || 0), vector.length, nullBitmap, getBool)) {
        if (!isValid) { return i; }
        ++i;
    }
    return -1;
}

/** @ignore */
function indexOfValue<T extends DataType>(vector: Vector<T>, searchElement?: T['TValue'] | null, fromIndex?: number): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    const compare = createElementComparator(searchElement);
    for (let i = (fromIndex || 0) - 1, n = vector.length; ++i < n;) {
        if (compare(vector.get(i))) {
            return i;
        }
    }
    return -1;
}

/** @ignore */
function indexOfUnion<T extends DataType>(vector: Vector<T>, searchElement?: T['TValue'] | null, fromIndex?: number): number {
    // Unions are special -- they do have a nullBitmap, but so can their children.
    // If the searchElement is null, we don't know whether it came from the Union's
    // bitmap or one of its childrens'. So we don't interrogate the Union's bitmap,
    // since that will report the wrong index if a child has a null before the Union.
    const compare = createElementComparator(searchElement);
    for (let i = (fromIndex || 0) - 1, n = vector.length; ++i < n;) {
        if (compare(vector.get(i))) {
            return i;
        }
    }
    return -1;
}

IndexOfVisitor.prototype.visitNull                 =  nullIndexOf;
IndexOfVisitor.prototype.visitBool                 = indexOfValue;
IndexOfVisitor.prototype.visitInt                  = indexOfValue;
IndexOfVisitor.prototype.visitInt8                 = indexOfValue;
IndexOfVisitor.prototype.visitInt16                = indexOfValue;
IndexOfVisitor.prototype.visitInt32                = indexOfValue;
IndexOfVisitor.prototype.visitInt64                = indexOfValue;
IndexOfVisitor.prototype.visitUint8                = indexOfValue;
IndexOfVisitor.prototype.visitUint16               = indexOfValue;
IndexOfVisitor.prototype.visitUint32               = indexOfValue;
IndexOfVisitor.prototype.visitUint64               = indexOfValue;
IndexOfVisitor.prototype.visitFloat                = indexOfValue;
IndexOfVisitor.prototype.visitFloat16              = indexOfValue;
IndexOfVisitor.prototype.visitFloat32              = indexOfValue;
IndexOfVisitor.prototype.visitFloat64              = indexOfValue;
IndexOfVisitor.prototype.visitUtf8                 = indexOfValue;
IndexOfVisitor.prototype.visitBinary               = indexOfValue;
IndexOfVisitor.prototype.visitFixedSizeBinary      = indexOfValue;
IndexOfVisitor.prototype.visitDate                 = indexOfValue;
IndexOfVisitor.prototype.visitDateDay              = indexOfValue;
IndexOfVisitor.prototype.visitDateMillisecond      = indexOfValue;
IndexOfVisitor.prototype.visitTimestamp            = indexOfValue;
IndexOfVisitor.prototype.visitTimestampSecond      = indexOfValue;
IndexOfVisitor.prototype.visitTimestampMillisecond = indexOfValue;
IndexOfVisitor.prototype.visitTimestampMicrosecond = indexOfValue;
IndexOfVisitor.prototype.visitTimestampNanosecond  = indexOfValue;
IndexOfVisitor.prototype.visitTime                 = indexOfValue;
IndexOfVisitor.prototype.visitTimeSecond           = indexOfValue;
IndexOfVisitor.prototype.visitTimeMillisecond      = indexOfValue;
IndexOfVisitor.prototype.visitTimeMicrosecond      = indexOfValue;
IndexOfVisitor.prototype.visitTimeNanosecond       = indexOfValue;
IndexOfVisitor.prototype.visitDecimal              = indexOfValue;
IndexOfVisitor.prototype.visitList                 = indexOfValue;
IndexOfVisitor.prototype.visitStruct               = indexOfValue;
IndexOfVisitor.prototype.visitUnion                = indexOfValue;
IndexOfVisitor.prototype.visitDenseUnion           = indexOfUnion;
IndexOfVisitor.prototype.visitSparseUnion          = indexOfUnion;
IndexOfVisitor.prototype.visitDictionary           = indexOfValue;
IndexOfVisitor.prototype.visitInterval             = indexOfValue;
IndexOfVisitor.prototype.visitIntervalDayTime      = indexOfValue;
IndexOfVisitor.prototype.visitIntervalYearMonth    = indexOfValue;
IndexOfVisitor.prototype.visitFixedSizeList        = indexOfValue;
IndexOfVisitor.prototype.visitMap                  = indexOfValue;

/** @ignore */
export const instance = new IndexOfVisitor();
