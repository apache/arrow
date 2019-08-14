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
import { encodeUtf8 } from '../util/utf8';
import { VectorType } from '../interfaces';
import { float64ToUint16 } from '../util/math';
import { toArrayBufferView } from '../util/buffer';
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
export interface SetVisitor extends Visitor {
    visit<T extends VectorType>(node: T, index: number, value: T['TValue']): void;
    visitMany<T extends VectorType>(nodes: T[], indices: number[], values: T['TValue'][]): void[];
    getVisitFn<T extends Type>(node: T): (vector: VectorType<T>, index: number, value: VectorType<T>['TValue']) => void;
    getVisitFn<T extends DataType>(node: VectorType<T> | Data<T> | T): (vector: VectorType<T>, index: number, value: VectorType<T>['TValue']) => void;
    visitNull                 <T extends Null>                (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitBool                 <T extends Bool>                (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitInt                  <T extends Int>                 (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitInt8                 <T extends Int8>                (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitInt16                <T extends Int16>               (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitInt32                <T extends Int32>               (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitInt64                <T extends Int64>               (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitUint8                <T extends Uint8>               (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitUint16               <T extends Uint16>              (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitUint32               <T extends Uint32>              (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitUint64               <T extends Uint64>              (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitFloat                <T extends Float>               (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitFloat16              <T extends Float16>             (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitFloat32              <T extends Float32>             (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitFloat64              <T extends Float64>             (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitUtf8                 <T extends Utf8>                (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitBinary               <T extends Binary>              (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitFixedSizeBinary      <T extends FixedSizeBinary>     (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitDate                 <T extends Date_>               (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitDateDay              <T extends DateDay>             (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitDateMillisecond      <T extends DateMillisecond>     (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTimestamp            <T extends Timestamp>           (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTimestampSecond      <T extends TimestampSecond>     (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTimestampMillisecond <T extends TimestampMillisecond>(vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTimestampMicrosecond <T extends TimestampMicrosecond>(vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTimestampNanosecond  <T extends TimestampNanosecond> (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTime                 <T extends Time>                (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTimeSecond           <T extends TimeSecond>          (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTimeMillisecond      <T extends TimeMillisecond>     (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTimeMicrosecond      <T extends TimeMicrosecond>     (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitTimeNanosecond       <T extends TimeNanosecond>      (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitDecimal              <T extends Decimal>             (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitList                 <T extends List>                (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitStruct               <T extends Struct>              (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitUnion                <T extends Union>               (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitDenseUnion           <T extends DenseUnion>          (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitSparseUnion          <T extends SparseUnion>         (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitDictionary           <T extends Dictionary>          (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitInterval             <T extends Interval>            (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitIntervalDayTime      <T extends IntervalDayTime>     (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitIntervalYearMonth    <T extends IntervalYearMonth>   (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitFixedSizeList        <T extends FixedSizeList>       (vector: VectorType<T>, index: number, value: T['TValue']): void;
    visitMap                  <T extends Map_>                (vector: VectorType<T>, index: number, value: T['TValue']): void;
}

/** @ignore */
export class SetVisitor extends Visitor {}

/** @ignore */
const setEpochMsToDays = (data: Int32Array, index: number, epochMs: number) => { data[index] = (epochMs / 86400000) | 0; };
/** @ignore */
const setEpochMsToMillisecondsLong = (data: Int32Array, index: number, epochMs: number) => {
    data[index] = (epochMs % 4294967296) | 0;
    data[index + 1] = (epochMs / 4294967296) | 0;
};
/** @ignore */
const setEpochMsToMicrosecondsLong = (data: Int32Array, index: number, epochMs: number) => {
    data[index] = ((epochMs * 1000) % 4294967296) | 0;
    data[index + 1] = ((epochMs * 1000) / 4294967296) | 0;
};
/** @ignore */
const setEpochMsToNanosecondsLong = (data: Int32Array, index: number, epochMs: number) => {
    data[index] = ((epochMs * 1000000) % 4294967296) | 0;
    data[index + 1] = ((epochMs * 1000000) / 4294967296) | 0;
};

/** @ignore */
const setVariableWidthBytes = (values: Uint8Array, valueOffsets: Int32Array, index: number, value: Uint8Array) => {
    const { [index]: x, [index + 1]: y } = valueOffsets;
    if (x != null && y != null) {
        values.set(value.subarray(0, y - x), x);
    }
};

/** @ignore */
const setBool = <T extends Bool>({ offset, values }: VectorType<T>, index: number, val: boolean) => {
    const idx = offset + index;
    val ? (values[idx >> 3] |=  (1 << (idx % 8)))  // true
        : (values[idx >> 3] &= ~(1 << (idx % 8))); // false

};

/** @ignore */ type Numeric1X = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32 | Float32 | Float64;
/** @ignore */ type Numeric2X = Int64 | Uint64;

/** @ignore */
const setDateDay         = <T extends DateDay>        ({ values         }: VectorType<T>, index: number, value: T['TValue']): void => { setEpochMsToDays(values, index, value.valueOf()); };
/** @ignore */
const setDateMillisecond = <T extends DateMillisecond>({ values         }: VectorType<T>, index: number, value: T['TValue']): void => { setEpochMsToMillisecondsLong(values, index * 2, value.valueOf()); };
/** @ignore */
const setNumeric         = <T extends Numeric1X>      ({ stride, values }: VectorType<T>, index: number, value: T['TValue']): void => { values[stride * index] = value; };
/** @ignore */
const setFloat16         = <T extends Float16>        ({ stride, values }: VectorType<T>, index: number, value: T['TValue']): void => { values[stride * index] = float64ToUint16(value); };
/** @ignore */
const setNumericX2       = <T extends Numeric2X>      (vector: VectorType<T>, index: number, value: T['TValue']): void => {
    switch (typeof value) {
        case 'bigint': vector.values64[index] = value; break;
        case 'number': vector.values[index * vector.stride] = value; break;
        default:
            const val = value as T['TArray'];
            const { stride, ArrayType } = vector;
            const long = toArrayBufferView<T['TArray']>(ArrayType, val);
            vector.values.set(long.subarray(0, stride), stride * index);
    }
};
/** @ignore */
const setFixedSizeBinary = <T extends FixedSizeBinary>({ stride, values }: VectorType<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, stride), stride * index); };

/** @ignore */
const setBinary = <T extends Binary>({ values, valueOffsets }: VectorType<T>, index: number, value: T['TValue']) => setVariableWidthBytes(values, valueOffsets, index, value);
/** @ignore */
const setUtf8 = <T extends Utf8>({ values, valueOffsets }: VectorType<T>, index: number, value: T['TValue']) => {
    setVariableWidthBytes(values, valueOffsets, index, encodeUtf8(value));
};

/* istanbul ignore next */
/** @ignore */
const setInt = <T extends Int>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    vector.type.bitWidth < 64
        ? setNumeric(vector as VectorType<Numeric1X>, index, value as Numeric1X['TValue'])
        : setNumericX2(vector as VectorType<Numeric2X>, index, value as Numeric2X['TValue']);
};

/* istanbul ignore next */
/** @ignore */
const setFloat = <T extends Float>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    vector.type.precision !== Precision.HALF
        ? setNumeric(vector as VectorType<Numeric1X>, index, value)
        : setFloat16(vector as VectorType<Float16>, index, value);
};

/* istanbul ignore next */
const setDate = <T extends Date_> (vector: VectorType<T>, index: number, value: T['TValue']): void => {
    vector.type.unit === DateUnit.DAY
        ? setDateDay(vector as VectorType<DateDay>, index, value)
        : setDateMillisecond(vector as VectorType<DateMillisecond>, index, value);
};

/** @ignore */
const setTimestampSecond      = <T extends TimestampSecond>     ({ values }: VectorType<T>, index: number, value: T['TValue']): void => setEpochMsToMillisecondsLong(values, index * 2, value / 1000);
/** @ignore */
const setTimestampMillisecond = <T extends TimestampMillisecond>({ values }: VectorType<T>, index: number, value: T['TValue']): void => setEpochMsToMillisecondsLong(values, index * 2, value);
/** @ignore */
const setTimestampMicrosecond = <T extends TimestampMicrosecond>({ values }: VectorType<T>, index: number, value: T['TValue']): void => setEpochMsToMicrosecondsLong(values, index * 2, value);
/** @ignore */
const setTimestampNanosecond  = <T extends TimestampNanosecond> ({ values }: VectorType<T>, index: number, value: T['TValue']): void => setEpochMsToNanosecondsLong(values, index * 2, value);
/* istanbul ignore next */
/** @ignore */
const setTimestamp            = <T extends Timestamp>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      setTimestampSecond(vector as VectorType<TimestampSecond>, index, value);
        case TimeUnit.MILLISECOND: return setTimestampMillisecond(vector as VectorType<TimestampMillisecond>, index, value);
        case TimeUnit.MICROSECOND: return setTimestampMicrosecond(vector as VectorType<TimestampMicrosecond>, index, value);
        case TimeUnit.NANOSECOND:  return  setTimestampNanosecond(vector as VectorType<TimestampNanosecond>, index, value);
    }
};

/** @ignore */
const setTimeSecond      = <T extends TimeSecond>     ({ values, stride }: VectorType<T>, index: number, value: T['TValue']): void => { values[stride * index] = value; };
/** @ignore */
const setTimeMillisecond = <T extends TimeMillisecond>({ values, stride }: VectorType<T>, index: number, value: T['TValue']): void => { values[stride * index] = value; };
/** @ignore */
const setTimeMicrosecond = <T extends TimeMicrosecond>({ values         }: VectorType<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 2), 2 * index); };
/** @ignore */
const setTimeNanosecond  = <T extends TimeNanosecond> ({ values         }: VectorType<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 2), 2 * index); };
/* istanbul ignore next */
/** @ignore */
const setTime            = <T extends Time>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      setTimeSecond(vector as VectorType<TimeSecond>, index, value as TimeSecond['TValue']);
        case TimeUnit.MILLISECOND: return setTimeMillisecond(vector as VectorType<TimeMillisecond>, index, value as TimeMillisecond['TValue']);
        case TimeUnit.MICROSECOND: return setTimeMicrosecond(vector as VectorType<TimeMicrosecond>, index, value as TimeMicrosecond['TValue']);
        case TimeUnit.NANOSECOND:  return  setTimeNanosecond(vector as VectorType<TimeNanosecond>, index, value as TimeNanosecond['TValue']);
    }
};

/** @ignore */
const setDecimal = <T extends Decimal>({ values }: VectorType<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 4), 4 * index); };

/** @ignore */
const setList = <T extends List>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    const values = vector.getChildAt(0)!;
    const { valueOffsets } = vector;
    let idx = -1, offset = valueOffsets[index];
    let end = Math.min(offset + value.length, valueOffsets[index + 1]);
    while (offset < end) {
        values.set(offset++, value.get(++idx));
    }
};

/** @ignore */
const setStruct = <
    S extends { [key: string]: DataType },
    V extends VectorType<Map_<S>> | VectorType<Struct<S>>
>(vector: V, index: number, value: V['TValue']) => {
    vector.type.children.forEach((_field, idx) => {
        const child = vector.getChildAt(idx);
        child && child.set(index, value[idx]);
    });
};

/** @ignore */
const setMap = <
    S extends { [key: string]: DataType },
    V extends VectorType<Map_<S>> | VectorType<Struct<S>>
>(vector: V, index: number, value: V['TValue']) => {
    vector.type.children.forEach(({ name }, idx) => {
        const child = vector.getChildAt(idx);
        child && child.set(index, value[name]);
    });
};

/* istanbul ignore next */
/** @ignore */
const setUnion = <
    V extends VectorType<Union> | VectorType<DenseUnion> | VectorType<SparseUnion>
>(vector: V, index: number, value: V['TValue']) => {
    vector.type.mode === UnionMode.Dense ?
        setDenseUnion(vector as VectorType<DenseUnion>, index, value) :
        setSparseUnion(vector as VectorType<SparseUnion>, index, value);
};

/** @ignore */
const setDenseUnion = <T extends DenseUnion>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    const childIndex = vector.typeIdToChildIndex[vector.typeIds[index]];
    const child = vector.getChildAt(childIndex);
    child && child.set(vector.valueOffsets[index], value);
};

/** @ignore */
const setSparseUnion = <T extends SparseUnion>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    const childIndex = vector.typeIdToChildIndex[vector.typeIds[index]];
    const child = vector.getChildAt(childIndex);
    child && child.set(index, value);
};

/** @ignore */
const setDictionary = <T extends Dictionary>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    const key = vector.getKey(index);
    if (key !== null) {
        vector.setValue(key, value);
    }
};

/* istanbul ignore next */
/** @ignore */
const setIntervalValue = <T extends Interval>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    (vector.type.unit === IntervalUnit.DAY_TIME)
        ? setIntervalDayTime(vector as VectorType<IntervalDayTime>, index, value)
        : setIntervalYearMonth(vector as VectorType<IntervalYearMonth>, index, value);
};

/** @ignore */
const setIntervalDayTime = <T extends IntervalDayTime>({ values }: VectorType<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 2), 2 * index); };
/** @ignore */
const setIntervalYearMonth = <T extends IntervalYearMonth>({ values }: VectorType<T>, index: number, value: T['TValue']): void => { values[index] = (value[0] * 12) + (value[1] % 12); };

/** @ignore */
const setFixedSizeList = <T extends FixedSizeList>(vector: VectorType<T>, index: number, value: T['TValue']): void => {
    const child = vector.getChildAt(0)!, { stride } = vector;
    for (let idx = -1, offset = index * stride; ++idx < stride;) {
        child.set(offset + idx, value.get(idx));
    }
};

SetVisitor.prototype.visitBool                 =                 setBool;
SetVisitor.prototype.visitInt                  =                  setInt;
SetVisitor.prototype.visitInt8                 =              setNumeric;
SetVisitor.prototype.visitInt16                =              setNumeric;
SetVisitor.prototype.visitInt32                =              setNumeric;
SetVisitor.prototype.visitInt64                =            setNumericX2;
SetVisitor.prototype.visitUint8                =              setNumeric;
SetVisitor.prototype.visitUint16               =              setNumeric;
SetVisitor.prototype.visitUint32               =              setNumeric;
SetVisitor.prototype.visitUint64               =            setNumericX2;
SetVisitor.prototype.visitFloat                =                setFloat;
SetVisitor.prototype.visitFloat16              =              setFloat16;
SetVisitor.prototype.visitFloat32              =              setNumeric;
SetVisitor.prototype.visitFloat64              =              setNumeric;
SetVisitor.prototype.visitUtf8                 =                 setUtf8;
SetVisitor.prototype.visitBinary               =               setBinary;
SetVisitor.prototype.visitFixedSizeBinary      =      setFixedSizeBinary;
SetVisitor.prototype.visitDate                 =                 setDate;
SetVisitor.prototype.visitDateDay              =              setDateDay;
SetVisitor.prototype.visitDateMillisecond      =      setDateMillisecond;
SetVisitor.prototype.visitTimestamp            =            setTimestamp;
SetVisitor.prototype.visitTimestampSecond      =      setTimestampSecond;
SetVisitor.prototype.visitTimestampMillisecond = setTimestampMillisecond;
SetVisitor.prototype.visitTimestampMicrosecond = setTimestampMicrosecond;
SetVisitor.prototype.visitTimestampNanosecond  =  setTimestampNanosecond;
SetVisitor.prototype.visitTime                 =                 setTime;
SetVisitor.prototype.visitTimeSecond           =           setTimeSecond;
SetVisitor.prototype.visitTimeMillisecond      =      setTimeMillisecond;
SetVisitor.prototype.visitTimeMicrosecond      =      setTimeMicrosecond;
SetVisitor.prototype.visitTimeNanosecond       =       setTimeNanosecond;
SetVisitor.prototype.visitDecimal              =              setDecimal;
SetVisitor.prototype.visitList                 =                 setList;
SetVisitor.prototype.visitStruct               =               setStruct;
SetVisitor.prototype.visitUnion                =                setUnion;
SetVisitor.prototype.visitDenseUnion           =           setDenseUnion;
SetVisitor.prototype.visitSparseUnion          =          setSparseUnion;
SetVisitor.prototype.visitDictionary           =           setDictionary;
SetVisitor.prototype.visitInterval             =        setIntervalValue;
SetVisitor.prototype.visitIntervalDayTime      =      setIntervalDayTime;
SetVisitor.prototype.visitIntervalYearMonth    =    setIntervalYearMonth;
SetVisitor.prototype.visitFixedSizeList        =        setFixedSizeList;
SetVisitor.prototype.visitMap                  =                  setMap;

/** @ignore */
export const instance = new SetVisitor();
