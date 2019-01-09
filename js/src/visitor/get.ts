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
import { BN } from '../util/bn';
import { Visitor } from '../visitor';
import { Vector } from '../interfaces';
import { decodeUtf8 } from '../util/utf8';
import { Type, UnionMode, Precision, DateUnit, TimeUnit, IntervalUnit } from '../enum';
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

export interface GetVisitor extends Visitor {
    visit<T extends Vector>  (node: T, index: number): T['TValue'];
    visitMany<T extends Vector>  (nodes: T[], indices: number[]): T['TValue'][];
    getVisitFn<T extends Type>    (node: T): (vector: Vector<T>, index: number) => Vector<T>['TValue'];
    getVisitFn<T extends DataType>(node: Vector<T> | Data<T> | T): (vector: Vector<T>, index: number) => Vector<T>['TValue'];
    visitNull                 <T extends Null>                 (vector: Vector<T>, index: number): T['TValue'];
    visitBool                 <T extends Bool>                 (vector: Vector<T>, index: number): T['TValue'];
    visitInt                  <T extends Int>                  (vector: Vector<T>, index: number): T['TValue'];
    visitInt8                 <T extends Int8>                 (vector: Vector<T>, index: number): T['TValue'];
    visitInt16                <T extends Int16>                (vector: Vector<T>, index: number): T['TValue'];
    visitInt32                <T extends Int32>                (vector: Vector<T>, index: number): T['TValue'];
    visitInt64                <T extends Int64>                (vector: Vector<T>, index: number): T['TValue'];
    visitUint8                <T extends Uint8>                (vector: Vector<T>, index: number): T['TValue'];
    visitUint16               <T extends Uint16>               (vector: Vector<T>, index: number): T['TValue'];
    visitUint32               <T extends Uint32>               (vector: Vector<T>, index: number): T['TValue'];
    visitUint64               <T extends Uint64>               (vector: Vector<T>, index: number): T['TValue'];
    visitFloat                <T extends Float>                (vector: Vector<T>, index: number): T['TValue'];
    visitFloat16              <T extends Float16>              (vector: Vector<T>, index: number): T['TValue'];
    visitFloat32              <T extends Float32>              (vector: Vector<T>, index: number): T['TValue'];
    visitFloat64              <T extends Float64>              (vector: Vector<T>, index: number): T['TValue'];
    visitUtf8                 <T extends Utf8>                 (vector: Vector<T>, index: number): T['TValue'];
    visitBinary               <T extends Binary>               (vector: Vector<T>, index: number): T['TValue'];
    visitFixedSizeBinary      <T extends FixedSizeBinary>      (vector: Vector<T>, index: number): T['TValue'];
    visitDate                 <T extends Date_>                (vector: Vector<T>, index: number): T['TValue'];
    visitDateDay              <T extends DateDay>              (vector: Vector<T>, index: number): T['TValue'];
    visitDateMillisecond      <T extends DateMillisecond>      (vector: Vector<T>, index: number): T['TValue'];
    visitTimestamp            <T extends Timestamp>            (vector: Vector<T>, index: number): T['TValue'];
    visitTimestampSecond      <T extends TimestampSecond>      (vector: Vector<T>, index: number): T['TValue'];
    visitTimestampMillisecond <T extends TimestampMillisecond> (vector: Vector<T>, index: number): T['TValue'];
    visitTimestampMicrosecond <T extends TimestampMicrosecond> (vector: Vector<T>, index: number): T['TValue'];
    visitTimestampNanosecond  <T extends TimestampNanosecond>  (vector: Vector<T>, index: number): T['TValue'];
    visitTime                 <T extends Time>                 (vector: Vector<T>, index: number): T['TValue'];
    visitTimeSecond           <T extends TimeSecond>           (vector: Vector<T>, index: number): T['TValue'];
    visitTimeMillisecond      <T extends TimeMillisecond>      (vector: Vector<T>, index: number): T['TValue'];
    visitTimeMicrosecond      <T extends TimeMicrosecond>      (vector: Vector<T>, index: number): T['TValue'];
    visitTimeNanosecond       <T extends TimeNanosecond>       (vector: Vector<T>, index: number): T['TValue'];
    visitDecimal              <T extends Decimal>              (vector: Vector<T>, index: number): T['TValue'];
    visitList                 <T extends List>                 (vector: Vector<T>, index: number): T['TValue'];
    visitStruct               <T extends Struct>               (vector: Vector<T>, index: number): T['TValue'];
    visitUnion                <T extends Union>                (vector: Vector<T>, index: number): T['TValue'];
    visitDenseUnion           <T extends DenseUnion>           (vector: Vector<T>, index: number): T['TValue'];
    visitSparseUnion          <T extends SparseUnion>          (vector: Vector<T>, index: number): T['TValue'];
    visitDictionary           <T extends Dictionary>           (vector: Vector<T>, index: number): T['TValue'];
    visitInterval             <T extends Interval>             (vector: Vector<T>, index: number): T['TValue'];
    visitIntervalDayTime      <T extends IntervalDayTime>      (vector: Vector<T>, index: number): T['TValue'];
    visitIntervalYearMonth    <T extends IntervalYearMonth>    (vector: Vector<T>, index: number): T['TValue'];
    visitFixedSizeList        <T extends FixedSizeList>        (vector: Vector<T>, index: number): T['TValue'];
    visitMap                  <T extends Map_>                 (vector: Vector<T>, index: number): T['TValue'];
}

export class GetVisitor extends Visitor {}

/** @ignore */const epochDaysToMs = (data: Int32Array, index: number) => 86400000 * data[index];
/** @ignore */const epochMillisecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1]) + (data[index] >>> 0);
/** @ignore */const epochMicrosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000) + ((data[index] >>> 0) / 1000);
/** @ignore */const epochNanosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000000) + ((data[index] >>> 0) / 1000000);

/** @ignore */const epochMillisecondsToDate = (epochMs: number) => new Date(epochMs);
/** @ignore */const epochDaysToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochDaysToMs(data, index));
/** @ignore */const epochMillisecondsLongToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochMillisecondsLongToMs(data, index));

/** @ignore */
const getNull = <T extends Null>(_vector: Vector<T>, _index: number): T['TValue'] => null;
/** @ignore */
const getVariableWidthBytes = (values: Uint8Array, valueOffsets: Int32Array, index: number) => {
    const { [index]: x, [index + 1]: y } = valueOffsets;
    return x != null && y != null ? values.subarray(x, y) : null as any;
};

/** @ignore */
const getBool = <T extends Bool>({ offset, values }: Vector<T>, index: number): T['TValue'] => {
    const idx = offset + index;
    const byte = values[idx >> 3];
    return (byte & 1 << (idx % 8)) !== 0;
};

/** @ignore */
type Numeric1X = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32 | Float32 | Float64;
/** @ignore */
type Numeric2X = Int64 | Uint64;

/** @ignore */
const getDateDay         = <T extends DateDay>        ({ values         }: Vector<T>, index: number): T['TValue'] => epochDaysToDate(values, index);
/** @ignore */
const getDateMillisecond = <T extends DateMillisecond>({ values         }: Vector<T>, index: number): T['TValue'] => epochMillisecondsLongToDate(values, index * 2);
/** @ignore */
const getNumeric         = <T extends Numeric1X>      ({ stride, values }: Vector<T>, index: number): T['TValue'] => values[stride * index];
/** @ignore */
const getFloat16         = <T extends Float16>        ({ stride, values }: Vector<T>, index: number): T['TValue'] => (values[stride * index] - 32767) / 32767;
/** @ignore */
const getBigInts         = <T extends Numeric2X>({ stride, values, type }: Vector<T>, index: number): T['TValue'] => BN.new(values.subarray(stride * index, stride * (index + 1)), type.isSigned);
/** @ignore */
const getFixedSizeBinary = <T extends FixedSizeBinary>({ stride, values }: Vector<T>, index: number): T['TValue'] => values.subarray(stride * index, stride * (index + 1));

/** @ignore */
const getBinary = <T extends Binary>({ values, valueOffsets }: Vector<T>, index: number): T['TValue'] => getVariableWidthBytes(values, valueOffsets, index);
/** @ignore */
const getUtf8 = <T extends Utf8>({ values, valueOffsets }: Vector<T>, index: number): T['TValue'] => {
    const bytes = getVariableWidthBytes(values, valueOffsets, index);
    return bytes !== null ? decodeUtf8(bytes) : null as any;
};

/* istanbul ignore next */
/** @ignore */
const getInt = <T extends Int>(vector: Vector<T>, index: number): T['TValue'] => (
    vector.type.bitWidth < 64
        ? getNumeric(<any> vector, index)
        : getBigInts(<any> vector, index)
);

/* istanbul ignore next */
/** @ignore */
const getFloat = <T extends Float> (vector: Vector<T>, index: number): T['TValue'] => (
    vector.type.precision !== Precision.HALF
        ? getNumeric(vector as any, index)
        : getFloat16(vector as any, index)
);

/* istanbul ignore next */
/** @ignore */
const getDate = <T extends Date_> (vector: Vector<T>, index: number): T['TValue'] => (
    vector.type.unit === DateUnit.DAY
        ? getDateDay(vector as any, index)
        : getDateMillisecond(vector as any, index)
);

/** @ignore */
const getTimestampSecond      = <T extends TimestampSecond>     ({ values }: Vector<T>, index: number): T['TValue'] => 1000 * epochMillisecondsLongToMs(values, index * 2);
/** @ignore */
const getTimestampMillisecond = <T extends TimestampMillisecond>({ values }: Vector<T>, index: number): T['TValue'] => epochMillisecondsLongToMs(values, index * 2);
/** @ignore */
const getTimestampMicrosecond = <T extends TimestampMicrosecond>({ values }: Vector<T>, index: number): T['TValue'] => epochMicrosecondsLongToMs(values, index * 2);
/** @ignore */
const getTimestampNanosecond  = <T extends TimestampNanosecond> ({ values }: Vector<T>, index: number): T['TValue'] => epochNanosecondsLongToMs(values, index * 2);
/* istanbul ignore next */
/** @ignore */
const getTimestamp            = <T extends Timestamp>(vector: Vector<T>, index: number): T['TValue'] => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      getTimestampSecond(vector as Vector<TimestampSecond>, index);
        case TimeUnit.MILLISECOND: return getTimestampMillisecond(vector as Vector<TimestampMillisecond>, index);
        case TimeUnit.MICROSECOND: return getTimestampMicrosecond(vector as Vector<TimestampMicrosecond>, index);
        case TimeUnit.NANOSECOND:  return  getTimestampNanosecond(vector as Vector<TimestampNanosecond>, index);
    }
};

/** @ignore */
const getTimeSecond      = <T extends TimeSecond>     ({ values, stride }: Vector<T>, index: number): T['TValue'] => values[stride * index];
/** @ignore */
const getTimeMillisecond = <T extends TimeMillisecond>({ values, stride }: Vector<T>, index: number): T['TValue'] => values[stride * index];
/** @ignore */
const getTimeMicrosecond = <T extends TimeMicrosecond>({ values         }: Vector<T>, index: number): T['TValue'] => BN.new(values.subarray(2 * index, 2 * (index + 1)), true);
/** @ignore */
const getTimeNanosecond  = <T extends TimeNanosecond> ({ values         }: Vector<T>, index: number): T['TValue'] => BN.new(values.subarray(2 * index, 2 * (index + 1)), true);
/* istanbul ignore next */
/** @ignore */
const getTime            = <T extends Time>(vector: Vector<T>, index: number): T['TValue'] => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      getTimeSecond(vector as Vector<TimeSecond>, index);
        case TimeUnit.MILLISECOND: return getTimeMillisecond(vector as Vector<TimeMillisecond>, index);
        case TimeUnit.MICROSECOND: return getTimeMicrosecond(vector as Vector<TimeMicrosecond>, index);
        case TimeUnit.NANOSECOND:  return  getTimeNanosecond(vector as Vector<TimeNanosecond>, index);
    }
};

/** @ignore */
const getDecimal = <T extends Decimal>({ values }: Vector<T>, index: number): T['TValue'] => BN.new(values.subarray(4 * index, 4 * (index + 1)), false);

/** @ignore */
const getList = <T extends List>(vector: Vector<T>, index: number): T['TValue'] => {
    const child = vector.getChildAt(0)!, { valueOffsets, stride } = vector;
    return child.slice(valueOffsets[index * stride], valueOffsets[(index * stride) + 1]) as T['TValue'];
};

/** @ignore */
const getNested = <
    S extends { [key: string]: DataType },
    V extends Vector<Map_<S>> | Vector<Struct<S>>
>(vector: V, index: number): V['TValue'] => {
    return vector.rowProxy.bind(vector, index);
};

/* istanbul ignore next */
/** @ignore */
const getUnion = <
    V extends Vector<Union> | Vector<DenseUnion> | Vector<SparseUnion>
>(vector: V, index: number): V['TValue'] => {
    return vector.type.mode === UnionMode.Dense ?
        getDenseUnion(vector as Vector<DenseUnion>, index) :
        getSparseUnion(vector as Vector<SparseUnion>, index);
};

/** @ignore */
const getDenseUnion = <T extends DenseUnion>(vector: Vector<T>, index: number): T['TValue'] => {
    const { typeIds, type: { typeIdToChildIndex } } = vector;
    const child = vector.getChildAt(typeIdToChildIndex[typeIds[index]]);
    return child ? child.get(vector.valueOffsets[index]) : null;
};

/** @ignore */
const getSparseUnion = <T extends SparseUnion>(vector: Vector<T>, index: number): T['TValue'] => {
    const { typeIds, type: { typeIdToChildIndex } } = vector;
    const child = vector.getChildAt(typeIdToChildIndex[typeIds[index]]);
    return child ? child.get(index) : null;
};

/** @ignore */
const getDictionary = <T extends Dictionary>(vector: Vector<T>, index: number): T['TValue'] => {
    return vector.getValue(vector.getKey(index)!);
};

/* istanbul ignore next */
/** @ignore */
const getInterval = <T extends Interval>(vector: Vector<T>, index: number): T['TValue'] =>
    (vector.type.unit === IntervalUnit.DAY_TIME)
        ? getIntervalDayTime(vector as any, index)
        : getIntervalYearMonth(vector as any, index);

/** @ignore */
const getIntervalDayTime = <T extends IntervalDayTime>({ values }: Vector<T>, index: number): T['TValue'] => values.subarray(2 * index, 2 * (index + 1));

/** @ignore */
const getIntervalYearMonth = <T extends IntervalYearMonth>({ values }: Vector<T>, index: number): T['TValue'] => {
    const interval = values[index];
    const int32s = new Int32Array(2);
    int32s[0] = interval / 12 | 0; /* years */
    int32s[1] = interval % 12 | 0; /* months */
    return int32s;
};

/** @ignore */
const getFixedSizeList = <T extends FixedSizeList>(vector: Vector<T>, index: number): T['TValue'] => {
    const child = vector.getChildAt(0)!, { stride } = vector;
    return child.slice(index * stride, (index + 1) * stride) as T['TValue'];
};

GetVisitor.prototype.visitNull                 =                 getNull;
GetVisitor.prototype.visitBool                 =                 getBool;
GetVisitor.prototype.visitInt                  =                  getInt;
GetVisitor.prototype.visitInt8                 =              getNumeric;
GetVisitor.prototype.visitInt16                =              getNumeric;
GetVisitor.prototype.visitInt32                =              getNumeric;
GetVisitor.prototype.visitInt64                =              getBigInts;
GetVisitor.prototype.visitUint8                =              getNumeric;
GetVisitor.prototype.visitUint16               =              getNumeric;
GetVisitor.prototype.visitUint32               =              getNumeric;
GetVisitor.prototype.visitUint64               =              getBigInts;
GetVisitor.prototype.visitFloat                =                getFloat;
GetVisitor.prototype.visitFloat16              =              getFloat16;
GetVisitor.prototype.visitFloat32              =              getNumeric;
GetVisitor.prototype.visitFloat64              =              getNumeric;
GetVisitor.prototype.visitUtf8                 =                 getUtf8;
GetVisitor.prototype.visitBinary               =               getBinary;
GetVisitor.prototype.visitFixedSizeBinary      =      getFixedSizeBinary;
GetVisitor.prototype.visitDate                 =                 getDate;
GetVisitor.prototype.visitDateDay              =              getDateDay;
GetVisitor.prototype.visitDateMillisecond      =      getDateMillisecond;
GetVisitor.prototype.visitTimestamp            =            getTimestamp;
GetVisitor.prototype.visitTimestampSecond      =      getTimestampSecond;
GetVisitor.prototype.visitTimestampMillisecond = getTimestampMillisecond;
GetVisitor.prototype.visitTimestampMicrosecond = getTimestampMicrosecond;
GetVisitor.prototype.visitTimestampNanosecond  =  getTimestampNanosecond;
GetVisitor.prototype.visitTime                 =                 getTime;
GetVisitor.prototype.visitTimeSecond           =           getTimeSecond;
GetVisitor.prototype.visitTimeMillisecond      =      getTimeMillisecond;
GetVisitor.prototype.visitTimeMicrosecond      =      getTimeMicrosecond;
GetVisitor.prototype.visitTimeNanosecond       =       getTimeNanosecond;
GetVisitor.prototype.visitDecimal              =              getDecimal;
GetVisitor.prototype.visitList                 =                 getList;
GetVisitor.prototype.visitStruct               =               getNested;
GetVisitor.prototype.visitUnion                =                getUnion;
GetVisitor.prototype.visitDenseUnion           =           getDenseUnion;
GetVisitor.prototype.visitSparseUnion          =          getSparseUnion;
GetVisitor.prototype.visitDictionary           =           getDictionary;
GetVisitor.prototype.visitInterval             =             getInterval;
GetVisitor.prototype.visitIntervalDayTime      =      getIntervalDayTime;
GetVisitor.prototype.visitIntervalYearMonth    =    getIntervalYearMonth;
GetVisitor.prototype.visitFixedSizeList        =        getFixedSizeList;
GetVisitor.prototype.visitMap                  =               getNested;

export const instance = new GetVisitor();
