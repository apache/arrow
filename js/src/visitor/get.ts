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
import { decodeUtf8 } from '../util/utf8';
import { VectorType } from '../interfaces';
import { uint16ToFloat64 } from '../util/math';
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

/** @ignore */
export interface GetVisitor extends Visitor {
    visit<T extends VectorType>  (node: T, index: number): T['TValue'];
    visitMany<T extends VectorType>  (nodes: T[], indices: number[]): T['TValue'][];
    getVisitFn<T extends Type>    (node: T): (vector: VectorType<T>, index: number) => VectorType<T>['TValue'];
    getVisitFn<T extends DataType>(node: VectorType<T> | Data<T> | T): (vector: VectorType<T>, index: number) => VectorType<T>['TValue'];
    visitNull                 <T extends Null>                 (vector: VectorType<T>, index: number): T['TValue'];
    visitBool                 <T extends Bool>                 (vector: VectorType<T>, index: number): T['TValue'];
    visitInt                  <T extends Int>                  (vector: VectorType<T>, index: number): T['TValue'];
    visitInt8                 <T extends Int8>                 (vector: VectorType<T>, index: number): T['TValue'];
    visitInt16                <T extends Int16>                (vector: VectorType<T>, index: number): T['TValue'];
    visitInt32                <T extends Int32>                (vector: VectorType<T>, index: number): T['TValue'];
    visitInt64                <T extends Int64>                (vector: VectorType<T>, index: number): T['TValue'];
    visitUint8                <T extends Uint8>                (vector: VectorType<T>, index: number): T['TValue'];
    visitUint16               <T extends Uint16>               (vector: VectorType<T>, index: number): T['TValue'];
    visitUint32               <T extends Uint32>               (vector: VectorType<T>, index: number): T['TValue'];
    visitUint64               <T extends Uint64>               (vector: VectorType<T>, index: number): T['TValue'];
    visitFloat                <T extends Float>                (vector: VectorType<T>, index: number): T['TValue'];
    visitFloat16              <T extends Float16>              (vector: VectorType<T>, index: number): T['TValue'];
    visitFloat32              <T extends Float32>              (vector: VectorType<T>, index: number): T['TValue'];
    visitFloat64              <T extends Float64>              (vector: VectorType<T>, index: number): T['TValue'];
    visitUtf8                 <T extends Utf8>                 (vector: VectorType<T>, index: number): T['TValue'];
    visitBinary               <T extends Binary>               (vector: VectorType<T>, index: number): T['TValue'];
    visitFixedSizeBinary      <T extends FixedSizeBinary>      (vector: VectorType<T>, index: number): T['TValue'];
    visitDate                 <T extends Date_>                (vector: VectorType<T>, index: number): T['TValue'];
    visitDateDay              <T extends DateDay>              (vector: VectorType<T>, index: number): T['TValue'];
    visitDateMillisecond      <T extends DateMillisecond>      (vector: VectorType<T>, index: number): T['TValue'];
    visitTimestamp            <T extends Timestamp>            (vector: VectorType<T>, index: number): T['TValue'];
    visitTimestampSecond      <T extends TimestampSecond>      (vector: VectorType<T>, index: number): T['TValue'];
    visitTimestampMillisecond <T extends TimestampMillisecond> (vector: VectorType<T>, index: number): T['TValue'];
    visitTimestampMicrosecond <T extends TimestampMicrosecond> (vector: VectorType<T>, index: number): T['TValue'];
    visitTimestampNanosecond  <T extends TimestampNanosecond>  (vector: VectorType<T>, index: number): T['TValue'];
    visitTime                 <T extends Time>                 (vector: VectorType<T>, index: number): T['TValue'];
    visitTimeSecond           <T extends TimeSecond>           (vector: VectorType<T>, index: number): T['TValue'];
    visitTimeMillisecond      <T extends TimeMillisecond>      (vector: VectorType<T>, index: number): T['TValue'];
    visitTimeMicrosecond      <T extends TimeMicrosecond>      (vector: VectorType<T>, index: number): T['TValue'];
    visitTimeNanosecond       <T extends TimeNanosecond>       (vector: VectorType<T>, index: number): T['TValue'];
    visitDecimal              <T extends Decimal>              (vector: VectorType<T>, index: number): T['TValue'];
    visitList                 <T extends List>                 (vector: VectorType<T>, index: number): T['TValue'];
    visitStruct               <T extends Struct>               (vector: VectorType<T>, index: number): T['TValue'];
    visitUnion                <T extends Union>                (vector: VectorType<T>, index: number): T['TValue'];
    visitDenseUnion           <T extends DenseUnion>           (vector: VectorType<T>, index: number): T['TValue'];
    visitSparseUnion          <T extends SparseUnion>          (vector: VectorType<T>, index: number): T['TValue'];
    visitDictionary           <T extends Dictionary>           (vector: VectorType<T>, index: number): T['TValue'];
    visitInterval             <T extends Interval>             (vector: VectorType<T>, index: number): T['TValue'];
    visitIntervalDayTime      <T extends IntervalDayTime>      (vector: VectorType<T>, index: number): T['TValue'];
    visitIntervalYearMonth    <T extends IntervalYearMonth>    (vector: VectorType<T>, index: number): T['TValue'];
    visitFixedSizeList        <T extends FixedSizeList>        (vector: VectorType<T>, index: number): T['TValue'];
    visitMap                  <T extends Map_>                 (vector: VectorType<T>, index: number): T['TValue'];
}

/** @ignore */
export class GetVisitor extends Visitor {}

/** @ignore */const epochDaysToMs = (data: Int32Array, index: number) => 86400000 * data[index];
/** @ignore */const epochMillisecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1]) + (data[index] >>> 0);
/** @ignore */const epochMicrosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000) + ((data[index] >>> 0) / 1000);
/** @ignore */const epochNanosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000000) + ((data[index] >>> 0) / 1000000);

/** @ignore */const epochMillisecondsToDate = (epochMs: number) => new Date(epochMs);
/** @ignore */const epochDaysToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochDaysToMs(data, index));
/** @ignore */const epochMillisecondsLongToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochMillisecondsLongToMs(data, index));

/** @ignore */
const getNull = <T extends Null>(_vector: VectorType<T>, _index: number): T['TValue'] => null;
/** @ignore */
const getVariableWidthBytes = (values: Uint8Array, valueOffsets: Int32Array, index: number) => {
    const { [index]: x, [index + 1]: y } = valueOffsets;
    return x != null && y != null ? values.subarray(x, y) : null as any;
};

/** @ignore */
const getBool = <T extends Bool>({ offset, values }: VectorType<T>, index: number): T['TValue'] => {
    const idx = offset + index;
    const byte = values[idx >> 3];
    return (byte & 1 << (idx % 8)) !== 0;
};

/** @ignore */
type Numeric1X = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32 | Float32 | Float64;
/** @ignore */
type Numeric2X = Int64 | Uint64;

/** @ignore */
const getDateDay         = <T extends DateDay>        ({ values         }: VectorType<T>, index: number): T['TValue'] => epochDaysToDate(values, index);
/** @ignore */
const getDateMillisecond = <T extends DateMillisecond>({ values         }: VectorType<T>, index: number): T['TValue'] => epochMillisecondsLongToDate(values, index * 2);
/** @ignore */
const getNumeric         = <T extends Numeric1X>      ({ stride, values }: VectorType<T>, index: number): T['TValue'] => values[stride * index];
/** @ignore */
const getFloat16         = <T extends Float16>        ({ stride, values }: VectorType<T>, index: number): T['TValue'] => uint16ToFloat64(values[stride * index]);
/** @ignore */
const getBigInts         = <T extends Numeric2X>({ stride, values, type }: VectorType<T>, index: number): T['TValue'] => <any> BN.new(values.subarray(stride * index, stride * (index + 1)), type.isSigned);
/** @ignore */
const getFixedSizeBinary = <T extends FixedSizeBinary>({ stride, values }: VectorType<T>, index: number): T['TValue'] => values.subarray(stride * index, stride * (index + 1));

/** @ignore */
const getBinary = <T extends Binary>({ values, valueOffsets }: VectorType<T>, index: number): T['TValue'] => getVariableWidthBytes(values, valueOffsets, index);
/** @ignore */
const getUtf8 = <T extends Utf8>({ values, valueOffsets }: VectorType<T>, index: number): T['TValue'] => {
    const bytes = getVariableWidthBytes(values, valueOffsets, index);
    return bytes !== null ? decodeUtf8(bytes) : null as any;
};

/* istanbul ignore next */
/** @ignore */
const getInt = <T extends Int>(vector: VectorType<T>, index: number): T['TValue'] => (
    vector.type.bitWidth < 64
        ? getNumeric(vector as VectorType<Numeric1X>, index)
        : getBigInts(vector as VectorType<Numeric2X>, index)
);

/* istanbul ignore next */
/** @ignore */
const getFloat = <T extends Float> (vector: VectorType<T>, index: number): T['TValue'] => (
    vector.type.precision !== Precision.HALF
        ? getNumeric(vector as VectorType<Numeric1X>, index)
        : getFloat16(vector as VectorType<Float16>, index)
);

/* istanbul ignore next */
/** @ignore */
const getDate = <T extends Date_> (vector: VectorType<T>, index: number): T['TValue'] => (
    vector.type.unit === DateUnit.DAY
        ? getDateDay(vector as VectorType<DateDay>, index)
        : getDateMillisecond(vector as VectorType<DateMillisecond>, index)
);

/** @ignore */
const getTimestampSecond      = <T extends TimestampSecond>     ({ values }: VectorType<T>, index: number): T['TValue'] => 1000 * epochMillisecondsLongToMs(values, index * 2);
/** @ignore */
const getTimestampMillisecond = <T extends TimestampMillisecond>({ values }: VectorType<T>, index: number): T['TValue'] => epochMillisecondsLongToMs(values, index * 2);
/** @ignore */
const getTimestampMicrosecond = <T extends TimestampMicrosecond>({ values }: VectorType<T>, index: number): T['TValue'] => epochMicrosecondsLongToMs(values, index * 2);
/** @ignore */
const getTimestampNanosecond  = <T extends TimestampNanosecond> ({ values }: VectorType<T>, index: number): T['TValue'] => epochNanosecondsLongToMs(values, index * 2);
/* istanbul ignore next */
/** @ignore */
const getTimestamp            = <T extends Timestamp>(vector: VectorType<T>, index: number): T['TValue'] => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      getTimestampSecond(vector as VectorType<TimestampSecond>, index);
        case TimeUnit.MILLISECOND: return getTimestampMillisecond(vector as VectorType<TimestampMillisecond>, index);
        case TimeUnit.MICROSECOND: return getTimestampMicrosecond(vector as VectorType<TimestampMicrosecond>, index);
        case TimeUnit.NANOSECOND:  return  getTimestampNanosecond(vector as VectorType<TimestampNanosecond>, index);
    }
};

/** @ignore */
const getTimeSecond      = <T extends TimeSecond>     ({ values, stride }: VectorType<T>, index: number): T['TValue'] => values[stride * index];
/** @ignore */
const getTimeMillisecond = <T extends TimeMillisecond>({ values, stride }: VectorType<T>, index: number): T['TValue'] => values[stride * index];
/** @ignore */
const getTimeMicrosecond = <T extends TimeMicrosecond>({ values         }: VectorType<T>, index: number): T['TValue'] => BN.signed(values.subarray(2 * index, 2 * (index + 1)));
/** @ignore */
const getTimeNanosecond  = <T extends TimeNanosecond> ({ values         }: VectorType<T>, index: number): T['TValue'] => BN.signed(values.subarray(2 * index, 2 * (index + 1)));
/* istanbul ignore next */
/** @ignore */
const getTime            = <T extends Time>(vector: VectorType<T>, index: number): T['TValue'] => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      getTimeSecond(vector as VectorType<TimeSecond>, index);
        case TimeUnit.MILLISECOND: return getTimeMillisecond(vector as VectorType<TimeMillisecond>, index);
        case TimeUnit.MICROSECOND: return getTimeMicrosecond(vector as VectorType<TimeMicrosecond>, index);
        case TimeUnit.NANOSECOND:  return  getTimeNanosecond(vector as VectorType<TimeNanosecond>, index);
    }
};

/** @ignore */
const getDecimal = <T extends Decimal>({ values }: VectorType<T>, index: number): T['TValue'] => BN.decimal(values.subarray(4 * index, 4 * (index + 1)));

/** @ignore */
const getList = <T extends List>(vector: VectorType<T>, index: number): T['TValue'] => {
    const child = vector.getChildAt(0)!, { valueOffsets, stride } = vector;
    return child.slice(valueOffsets[index * stride], valueOffsets[(index * stride) + 1]) as T['TValue'];
};

/** @ignore */
const getNested = <
    S extends { [key: string]: DataType },
    V extends VectorType<Map_<S>> | VectorType<Struct<S>>
>(vector: V, index: number): V['TValue'] => {
    return vector.rowProxy.bind(index) as V['TValue'];
};

/* istanbul ignore next */
/** @ignore */
const getUnion = <
    V extends VectorType<Union> | VectorType<DenseUnion> | VectorType<SparseUnion>
>(vector: V, index: number): V['TValue'] => {
    return vector.type.mode === UnionMode.Dense ?
        getDenseUnion(vector as VectorType<DenseUnion>, index) :
        getSparseUnion(vector as VectorType<SparseUnion>, index);
};

/** @ignore */
const getDenseUnion = <T extends DenseUnion>(vector: VectorType<T>, index: number): T['TValue'] => {
    const childIndex = vector.typeIdToChildIndex[vector.typeIds[index]];
    const child = vector.getChildAt(childIndex);
    return child ? child.get(vector.valueOffsets[index]) : null;
};

/** @ignore */
const getSparseUnion = <T extends SparseUnion>(vector: VectorType<T>, index: number): T['TValue'] => {
    const childIndex = vector.typeIdToChildIndex[vector.typeIds[index]];
    const child = vector.getChildAt(childIndex);
    return child ? child.get(index) : null;
};

/** @ignore */
const getDictionary = <T extends Dictionary>(vector: VectorType<T>, index: number): T['TValue'] => {
    return vector.getValue(vector.getKey(index)!);
};

/* istanbul ignore next */
/** @ignore */
const getInterval = <T extends Interval>(vector: VectorType<T>, index: number): T['TValue'] =>
    (vector.type.unit === IntervalUnit.DAY_TIME)
        ? getIntervalDayTime(vector as VectorType<IntervalDayTime>, index)
        : getIntervalYearMonth(vector as VectorType<IntervalYearMonth>, index);

/** @ignore */
const getIntervalDayTime = <T extends IntervalDayTime>({ values }: VectorType<T>, index: number): T['TValue'] => values.subarray(2 * index, 2 * (index + 1));

/** @ignore */
const getIntervalYearMonth = <T extends IntervalYearMonth>({ values }: VectorType<T>, index: number): T['TValue'] => {
    const interval = values[index];
    const int32s = new Int32Array(2);
    int32s[0] = interval / 12 | 0; /* years */
    int32s[1] = interval % 12 | 0; /* months */
    return int32s;
};

/** @ignore */
const getFixedSizeList = <T extends FixedSizeList>(vector: VectorType<T>, index: number): T['TValue'] => {
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

/** @ignore */
export const instance = new GetVisitor();
