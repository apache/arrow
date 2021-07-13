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
import { Vector } from '../vector';
import { Visitor } from '../visitor';
import { decodeUtf8 } from '../util/utf8';
import { TypeToDataType } from '../interfaces';
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
    visit<T extends DataType>      (node: Data<T>, index: number): T['TValue'];
    visitMany<T extends DataType>  (nodes: Data<T>[], indices: number[]): T['TValue'][];
    getVisitFn<T extends DataType> (node: Data<T> | T): (data: Data<T>, index: number) => T['TValue'];
    getVisitFn<T extends Type>     (node: T): (data: Data<TypeToDataType<T>>, index: number) => TypeToDataType<T>['TValue'];
    visitNull                 <T extends Null>                 (data: Data<T>, index: number): T['TValue'];
    visitBool                 <T extends Bool>                 (data: Data<T>, index: number): T['TValue'];
    visitInt                  <T extends Int>                  (data: Data<T>, index: number): T['TValue'];
    visitInt8                 <T extends Int8>                 (data: Data<T>, index: number): T['TValue'];
    visitInt16                <T extends Int16>                (data: Data<T>, index: number): T['TValue'];
    visitInt32                <T extends Int32>                (data: Data<T>, index: number): T['TValue'];
    visitInt64                <T extends Int64>                (data: Data<T>, index: number): T['TValue'];
    visitUint8                <T extends Uint8>                (data: Data<T>, index: number): T['TValue'];
    visitUint16               <T extends Uint16>               (data: Data<T>, index: number): T['TValue'];
    visitUint32               <T extends Uint32>               (data: Data<T>, index: number): T['TValue'];
    visitUint64               <T extends Uint64>               (data: Data<T>, index: number): T['TValue'];
    visitFloat                <T extends Float>                (data: Data<T>, index: number): T['TValue'];
    visitFloat16              <T extends Float16>              (data: Data<T>, index: number): T['TValue'];
    visitFloat32              <T extends Float32>              (data: Data<T>, index: number): T['TValue'];
    visitFloat64              <T extends Float64>              (data: Data<T>, index: number): T['TValue'];
    visitUtf8                 <T extends Utf8>                 (data: Data<T>, index: number): T['TValue'];
    visitBinary               <T extends Binary>               (data: Data<T>, index: number): T['TValue'];
    visitFixedSizeBinary      <T extends FixedSizeBinary>      (data: Data<T>, index: number): T['TValue'];
    visitDate                 <T extends Date_>                (data: Data<T>, index: number): T['TValue'];
    visitDateDay              <T extends DateDay>              (data: Data<T>, index: number): T['TValue'];
    visitDateMillisecond      <T extends DateMillisecond>      (data: Data<T>, index: number): T['TValue'];
    visitTimestamp            <T extends Timestamp>            (data: Data<T>, index: number): T['TValue'];
    visitTimestampSecond      <T extends TimestampSecond>      (data: Data<T>, index: number): T['TValue'];
    visitTimestampMillisecond <T extends TimestampMillisecond> (data: Data<T>, index: number): T['TValue'];
    visitTimestampMicrosecond <T extends TimestampMicrosecond> (data: Data<T>, index: number): T['TValue'];
    visitTimestampNanosecond  <T extends TimestampNanosecond>  (data: Data<T>, index: number): T['TValue'];
    visitTime                 <T extends Time>                 (data: Data<T>, index: number): T['TValue'];
    visitTimeSecond           <T extends TimeSecond>           (data: Data<T>, index: number): T['TValue'];
    visitTimeMillisecond      <T extends TimeMillisecond>      (data: Data<T>, index: number): T['TValue'];
    visitTimeMicrosecond      <T extends TimeMicrosecond>      (data: Data<T>, index: number): T['TValue'];
    visitTimeNanosecond       <T extends TimeNanosecond>       (data: Data<T>, index: number): T['TValue'];
    visitDecimal              <T extends Decimal>              (data: Data<T>, index: number): T['TValue'];
    visitList                 <T extends List>                 (data: Data<T>, index: number): T['TValue'];
    visitStruct               <T extends Struct>               (data: Data<T>, index: number): T['TValue'];
    visitUnion                <T extends Union>                (data: Data<T>, index: number): T['TValue'];
    visitDenseUnion           <T extends DenseUnion>           (data: Data<T>, index: number): T['TValue'];
    visitSparseUnion          <T extends SparseUnion>          (data: Data<T>, index: number): T['TValue'];
    visitDictionary           <T extends Dictionary>           (data: Data<T>, index: number): T['TValue'];
    visitInterval             <T extends Interval>             (data: Data<T>, index: number): T['TValue'];
    visitIntervalDayTime      <T extends IntervalDayTime>      (data: Data<T>, index: number): T['TValue'];
    visitIntervalYearMonth    <T extends IntervalYearMonth>    (data: Data<T>, index: number): T['TValue'];
    visitFixedSizeList        <T extends FixedSizeList>        (data: Data<T>, index: number): T['TValue'];
    visitMap                  <T extends Map_>                 (data: Data<T>, index: number): T['TValue'];
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
const getNull = <T extends Null>(_data: Data<T>, _index: number): T['TValue'] => null;
/** @ignore */
const getVariableWidthBytes = (values: Uint8Array, valueOffsets: Int32Array, index: number) => {
    const { [index]: x, [index + 1]: y } = valueOffsets;
    return x != null && y != null ? values.subarray(x, y) : null as any;
};

/** @ignore */
const getBool = <T extends Bool>({ offset, values }: Data<T>, index: number): T['TValue'] => {
    const idx = offset + index;
    const byte = values[idx >> 3];
    return (byte & 1 << (idx % 8)) !== 0;
};

/** @ignore */
type Numeric1X = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32 | Float32 | Float64;
/** @ignore */
type Numeric2X = Int64 | Uint64;

/** @ignore */
const getDateDay         = <T extends DateDay>        ({ values         }: Data<T>, index: number): T['TValue'] => epochDaysToDate(values, index);
/** @ignore */
const getDateMillisecond = <T extends DateMillisecond>({ values         }: Data<T>, index: number): T['TValue'] => epochMillisecondsLongToDate(values, index * 2);
/** @ignore */
const getNumeric         = <T extends Numeric1X>      ({ stride, values }: Data<T>, index: number): T['TValue'] => values[stride * index];
/** @ignore */
const getFloat16         = <T extends Float16>        ({ stride, values }: Data<T>, index: number): T['TValue'] => uint16ToFloat64(values[stride * index]);
/** @ignore */
const getBigInts         = <T extends Numeric2X>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const getFixedSizeBinary = <T extends FixedSizeBinary>({ stride, values }: Data<T>, index: number): T['TValue'] => values.subarray(stride * index, stride * (index + 1));

/** @ignore */
const getBinary = <T extends Binary>({ values, valueOffsets }: Data<T>, index: number): T['TValue'] => getVariableWidthBytes(values, valueOffsets, index);
/** @ignore */
const getUtf8 = <T extends Utf8>({ values, valueOffsets }: Data<T>, index: number): T['TValue'] => {
    const bytes = getVariableWidthBytes(values, valueOffsets, index);
    return bytes !== null ? decodeUtf8(bytes) : null as any;
};

/* istanbul ignore next */
/** @ignore */
const getInt = <T extends Int>({ values }: Data<T>, index: number): T['TValue'] => values[index];

/* istanbul ignore next */
/** @ignore */
const getFloat = <T extends Float> ({ type, values }: Data<T>, index: number): T['TValue'] => (
    type.precision !== Precision.HALF ? values[index] : uint16ToFloat64(values[index])
);

/* istanbul ignore next */
/** @ignore */
const getDate = <T extends Date_> (data: Data<T>, index: number): T['TValue'] => (
    data.type.unit === DateUnit.DAY
        ? getDateDay(data as Data<DateDay>, index)
        : getDateMillisecond(data as Data<DateMillisecond>, index)
);

/** @ignore */
const getTimestampSecond      = <T extends TimestampSecond>     ({ values }: Data<T>, index: number): T['TValue'] => 1000 * epochMillisecondsLongToMs(values, index * 2);
/** @ignore */
const getTimestampMillisecond = <T extends TimestampMillisecond>({ values }: Data<T>, index: number): T['TValue'] => epochMillisecondsLongToMs(values, index * 2);
/** @ignore */
const getTimestampMicrosecond = <T extends TimestampMicrosecond>({ values }: Data<T>, index: number): T['TValue'] => epochMicrosecondsLongToMs(values, index * 2);
/** @ignore */
const getTimestampNanosecond  = <T extends TimestampNanosecond> ({ values }: Data<T>, index: number): T['TValue'] => epochNanosecondsLongToMs(values, index * 2);
/* istanbul ignore next */
/** @ignore */
const getTimestamp            = <T extends Timestamp>(data: Data<T>, index: number): T['TValue'] => {
    switch (data.type.unit) {
        case TimeUnit.SECOND:      return      getTimestampSecond(data as Data<TimestampSecond>, index);
        case TimeUnit.MILLISECOND: return getTimestampMillisecond(data as Data<TimestampMillisecond>, index);
        case TimeUnit.MICROSECOND: return getTimestampMicrosecond(data as Data<TimestampMicrosecond>, index);
        case TimeUnit.NANOSECOND:  return  getTimestampNanosecond(data as Data<TimestampNanosecond>, index);
    }
};

/** @ignore */
const getTimeSecond      = <T extends TimeSecond>     ({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const getTimeMillisecond = <T extends TimeMillisecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const getTimeMicrosecond = <T extends TimeMicrosecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const getTimeNanosecond  = <T extends TimeNanosecond> ({ values }: Data<T>, index: number): T['TValue'] => values[index];
/* istanbul ignore next */
/** @ignore */
const getTime            = <T extends Time>(data: Data<T>, index: number): T['TValue'] => {
    switch (data.type.unit) {
        case TimeUnit.SECOND:      return      getTimeSecond(data as Data<TimeSecond>, index);
        case TimeUnit.MILLISECOND: return getTimeMillisecond(data as Data<TimeMillisecond>, index);
        case TimeUnit.MICROSECOND: return getTimeMicrosecond(data as Data<TimeMicrosecond>, index);
        case TimeUnit.NANOSECOND:  return  getTimeNanosecond(data as Data<TimeNanosecond>, index);
    }
};

/** @ignore */
const getDecimal = <T extends Decimal>({ values }: Data<T>, index: number): T['TValue'] => BN.decimal(values.subarray(4 * index, 4 * (index + 1)));

/** @ignore */
const getList = <T extends List>(data: Data<T>, index: number): T['TValue'] => {
    const { valueOffsets, stride } = data;
    const child: Data<T['valueType']> = data.children[0];
    const slice = child.slice(valueOffsets[index * stride], valueOffsets[index * stride + 1]);
    return new Vector([slice]) as T['TValue'];
};

/** @ignore */
const getMap = <T extends Map_>(data: Data<T>, index: number): T['TValue'] => {
    return data.type.createRow(data, index) as T['TValue'];
};

/** @ignore */
const getStruct = <T extends Struct>(data: Data<T>, index: number): T['TValue'] => {
    return data.type.createRow(data, index);
};

/* istanbul ignore next */
/** @ignore */
const getUnion = <
    D extends Data<Union> | Data<DenseUnion> | Data<SparseUnion>
>(data: D, index: number): D['TValue'] => {
    return data.type.mode === UnionMode.Dense ?
        getDenseUnion(data as Data<DenseUnion>, index) :
        getSparseUnion(data as Data<SparseUnion>, index);
};

/** @ignore */
const getDenseUnion = <T extends DenseUnion>(data: Data<T>, index: number): T['TValue'] => {
    const childIndex = data.type.typeIdToChildIndex[data.typeIds[index]];
    const child = data.children[childIndex];
    return instance.visit(child, data.valueOffsets[index]);
};

/** @ignore */
const getSparseUnion = <T extends SparseUnion>(data: Data<T>, index: number): T['TValue'] => {
    const childIndex = data.type.typeIdToChildIndex[data.typeIds[index]];
    const child = data.children[childIndex];
    return instance.visit(child, index);
};

/** @ignore */
const getDictionary = <T extends Dictionary>(data: Data<T>, index: number): T['TValue'] => {
    return data.dictionary?.get(data.values[index]);
};

/* istanbul ignore next */
/** @ignore */
const getInterval = <T extends Interval>(data: Data<T>, index: number): T['TValue'] =>
    (data.type.unit === IntervalUnit.DAY_TIME)
        ? getIntervalDayTime(data as Data<IntervalDayTime>, index)
        : getIntervalYearMonth(data as Data<IntervalYearMonth>, index);

/** @ignore */
const getIntervalDayTime = <T extends IntervalDayTime>({ values }: Data<T>, index: number): T['TValue'] => values.subarray(2 * index, 2 * (index + 1));

/** @ignore */
const getIntervalYearMonth = <T extends IntervalYearMonth>({ values }: Data<T>, index: number): T['TValue'] => {
    const interval = values[index];
    const int32s = new Int32Array(2);
    int32s[0] = interval / 12 | 0; /* years */
    int32s[1] = interval % 12 | 0; /* months */
    return int32s;
};

/** @ignore */
const getFixedSizeList = <T extends FixedSizeList>(data: Data<T>, index: number): T['TValue'] => {
    const { stride } = data;
    const child: Data<T['valueType']> = data.children[0];
    const slice = child.slice(index * stride, (index + 1) * stride);
    return new Vector([slice]);
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
GetVisitor.prototype.visitStruct               =               getStruct;
GetVisitor.prototype.visitUnion                =                getUnion;
GetVisitor.prototype.visitDenseUnion           =           getDenseUnion;
GetVisitor.prototype.visitSparseUnion          =          getSparseUnion;
GetVisitor.prototype.visitDictionary           =           getDictionary;
GetVisitor.prototype.visitInterval             =             getInterval;
GetVisitor.prototype.visitIntervalDayTime      =      getIntervalDayTime;
GetVisitor.prototype.visitIntervalYearMonth    =    getIntervalYearMonth;
GetVisitor.prototype.visitFixedSizeList        =        getFixedSizeList;
GetVisitor.prototype.visitMap                  =                  getMap;

/** @ignore */
export const instance = new GetVisitor();
