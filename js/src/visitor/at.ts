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

import { Data } from '../data.js';
import { BN } from '../util/bn.js';
import { Vector } from '../vector.js';
import { Visitor } from '../visitor.js';
import { MapRow } from '../row/map.js';
import { StructRow, StructRowProxy } from '../row/struct.js';
import { bigIntToNumber } from '../util/bigint.js';
import { decodeUtf8 } from '../util/utf8.js';
import { TypeToDataType } from '../interfaces.js';
import { uint16ToFloat64 } from '../util/math.js';
import { Type, UnionMode, Precision, DateUnit, TimeUnit, IntervalUnit } from '../enum.js';
import {
    DataType, Dictionary,
    Bool, Null, Utf8, LargeUtf8, Binary, LargeBinary, Decimal, FixedSizeBinary, List, FixedSizeList, Map_, Struct,
    Float, Float16, Float32, Float64,
    Int, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64,
    Date_, DateDay, DateMillisecond,
    Interval, IntervalDayTime, IntervalYearMonth,
    Time, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    Timestamp, TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Duration, DurationSecond, DurationMillisecond, DurationMicrosecond, DurationNanosecond,
    Union, DenseUnion, SparseUnion,
} from '../type.js';

/** @ignore */
export interface AtVisitor extends Visitor {
    visit<T extends DataType>(node: Data<T>, index: number): T['TValue'] | null;
    visitMany<T extends DataType>(nodes: Data<T>[], indices: number[]): (T['TValue'] | null)[];
    atVisitFn<T extends DataType>(node: Vector<T> | Data<T> | T): (data: Data<T>, index: number) => T['TValue'] | null;
    atVisitFn<T extends Type>(node: T): (data: Data<TypeToDataType<T>>, index: number) => TypeToDataType<T>['TValue'];
    visitNull<T extends Null>(data: Data<T>, index: number): T['TValue'] | null;
    visitBool<T extends Bool>(data: Data<T>, index: number): T['TValue'] | null;
    visitInt<T extends Int>(data: Data<T>, index: number): T['TValue'] | null;
    visitInt8<T extends Int8>(data: Data<T>, index: number): T['TValue'] | null;
    visitInt16<T extends Int16>(data: Data<T>, index: number): T['TValue'] | null;
    visitInt32<T extends Int32>(data: Data<T>, index: number): T['TValue'] | null;
    visitInt64<T extends Int64>(data: Data<T>, index: number): T['TValue'] | null;
    visitUint8<T extends Uint8>(data: Data<T>, index: number): T['TValue'] | null;
    visitUint16<T extends Uint16>(data: Data<T>, index: number): T['TValue'] | null;
    visitUint32<T extends Uint32>(data: Data<T>, index: number): T['TValue'] | null;
    visitUint64<T extends Uint64>(data: Data<T>, index: number): T['TValue'] | null;
    visitFloat<T extends Float>(data: Data<T>, index: number): T['TValue'] | null;
    visitFloat16<T extends Float16>(data: Data<T>, index: number): T['TValue'] | null;
    visitFloat32<T extends Float32>(data: Data<T>, index: number): T['TValue'] | null;
    visitFloat64<T extends Float64>(data: Data<T>, index: number): T['TValue'] | null;
    visitUtf8<T extends Utf8>(data: Data<T>, index: number): T['TValue'] | null;
    visitLargeUtf8<T extends LargeUtf8>(data: Data<T>, index: number): T['TValue'] | null;
    visitBinary<T extends Binary>(data: Data<T>, index: number): T['TValue'] | null;
    visitLargeBinary<T extends LargeBinary>(data: Data<T>, index: number): T['TValue'] | null;
    visitFixedSizeBinary<T extends FixedSizeBinary>(data: Data<T>, index: number): T['TValue'] | null;
    visitDate<T extends Date_>(data: Data<T>, index: number): T['TValue'] | null;
    visitDateDay<T extends DateDay>(data: Data<T>, index: number): T['TValue'] | null;
    visitDateMillisecond<T extends DateMillisecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitTimestamp<T extends Timestamp>(data: Data<T>, index: number): T['TValue'] | null;
    visitTimestampSecond<T extends TimestampSecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitTimestampMillisecond<T extends TimestampMillisecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitTimestampMicrosecond<T extends TimestampMicrosecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitTimestampNanosecond<T extends TimestampNanosecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitTime<T extends Time>(data: Data<T>, index: number): T['TValue'] | null;
    visitTimeSecond<T extends TimeSecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitTimeMillisecond<T extends TimeMillisecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitTimeMicrosecond<T extends TimeMicrosecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitTimeNanosecond<T extends TimeNanosecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitDecimal<T extends Decimal>(data: Data<T>, index: number): T['TValue'] | null;
    visitList<T extends List>(data: Data<T>, index: number): T['TValue'] | null;
    visitStruct<T extends Struct>(data: Data<T>, index: number): T['TValue'] | null;
    visitUnion<T extends Union>(data: Data<T>, index: number): T['TValue'] | null;
    visitDenseUnion<T extends DenseUnion>(data: Data<T>, index: number): T['TValue'] | null;
    visitSparseUnion<T extends SparseUnion>(data: Data<T>, index: number): T['TValue'] | null;
    visitDictionary<T extends Dictionary>(data: Data<T>, index: number): T['TValue'] | null;
    visitInterval<T extends Interval>(data: Data<T>, index: number): T['TValue'] | null;
    visitIntervalDayTime<T extends IntervalDayTime>(data: Data<T>, index: number): T['TValue'] | null;
    visitIntervalYearMonth<T extends IntervalYearMonth>(data: Data<T>, index: number): T['TValue'] | null;
    visitDuration<T extends Duration>(data: Data<T>, index: number): T['TValue'] | null;
    visitDurationSecond<T extends DurationSecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitDurationMillisecond<T extends DurationMillisecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitDurationMicrosecond<T extends DurationMicrosecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitDurationNanosecond<T extends DurationNanosecond>(data: Data<T>, index: number): T['TValue'] | null;
    visitFixedSizeList<T extends FixedSizeList>(data: Data<T>, index: number): T['TValue'] | null;
    visitMap<T extends Map_>(data: Data<T>, index: number): T['TValue'] | null;
}

/** @ignore */
export class AtVisitor extends Visitor { }

/** @ignore */
function wrapAt<T extends DataType>(fn: (data: Data<T>, _1: any) => any) {
    return (data: Data<T>, _1: any) => data.getValid(_1) ? fn(data, _1) : null;
}

/** @ignore */const epochDaysToMs = (data: Int32Array, index: number) => 86400000 * data[index];
/** @ignore */const epochMillisecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1]) + (data[index] >>> 0);
/** @ignore */const epochMicrosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000) + ((data[index] >>> 0) / 1000);
/** @ignore */const epochNanosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000000) + ((data[index] >>> 0) / 1000000);

/** @ignore */const epochMillisecondsToDate = (epochMs: number) => new Date(epochMs);
/** @ignore */const epochDaysToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochDaysToMs(data, index));
/** @ignore */const epochMillisecondsLongToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochMillisecondsLongToMs(data, index));

/** @ignore */
const atNull = <T extends Null>(_data: Data<T>, _index: number): T['TValue'] => null;
/** @ignore */
const atVariableWidthBytes = (values: Uint8Array, valueOffsets: Int32Array | BigInt64Array, index: number) => {
    if (index + 1 >= valueOffsets.length) {
        return null as any;
    }
    const x = bigIntToNumber(valueOffsets[index]);
    const y = bigIntToNumber(valueOffsets[index + 1]);
    return values.subarray(x, y);
};

/** @ignore */
const atBool = <T extends Bool>({ offset, values }: Data<T>, index: number): T['TValue'] => {
    const idx = offset + index;
    const byte = values[idx >> 3];
    return (byte & 1 << (idx % 8)) !== 0;
};

/** @ignore */
type Numeric1X = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32 | Float32 | Float64;
/** @ignore */
type Numeric2X = Int64 | Uint64;

/** @ignore */
const atDateDay = <T extends DateDay>({ values }: Data<T>, index: number): T['TValue'] => epochDaysToDate(values, index);
/** @ignore */
const atDateMillisecond = <T extends DateMillisecond>({ values }: Data<T>, index: number): T['TValue'] => epochMillisecondsLongToDate(values, index * 2);
/** @ignore */
const atNumeric = <T extends Numeric1X>({ stride, values }: Data<T>, index: number): T['TValue'] => values[stride * index];
/** @ignore */
const atFloat16 = <T extends Float16>({ stride, values }: Data<T>, index: number): T['TValue'] => uint16ToFloat64(values[stride * index]);
/** @ignore */
const atBigInts = <T extends Numeric2X>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const atFixedSizeBinary = <T extends FixedSizeBinary>({ stride, values }: Data<T>, index: number): T['TValue'] => values.subarray(stride * index, stride * (index + 1));

/** @ignore */
const atBinary = <T extends Binary | LargeBinary>({ values, valueOffsets }: Data<T>, index: number): T['TValue'] => atVariableWidthBytes(values, valueOffsets, index);
/** @ignore */
const atUtf8 = <T extends Utf8 | LargeUtf8>({ values, valueOffsets }: Data<T>, index: number): T['TValue'] => {
    const bytes = atVariableWidthBytes(values, valueOffsets, index);
    return bytes !== null ? decodeUtf8(bytes) : null as any;
};

/* istanbul ignore next */
/** @ignore */
const atInt = <T extends Int>({ values }: Data<T>, index: number): T['TValue'] => values[index];

/* istanbul ignore next */
/** @ignore */
const atFloat = <T extends Float>({ type, values }: Data<T>, index: number): T['TValue'] => (
    type.precision !== Precision.HALF ? values[index] : uint16ToFloat64(values[index])
);

/* istanbul ignore next */
/** @ignore */
const atDate = <T extends Date_>(data: Data<T>, index: number): T['TValue'] => (
    data.type.unit === DateUnit.DAY
        ? atDateDay(data as Data<DateDay>, index)
        : atDateMillisecond(data as Data<DateMillisecond>, index)
);

/** @ignore */
const atTimestampSecond = <T extends TimestampSecond>({ values }: Data<T>, index: number): T['TValue'] => 1000 * epochMillisecondsLongToMs(values, index * 2);
/** @ignore */
const atTimestampMillisecond = <T extends TimestampMillisecond>({ values }: Data<T>, index: number): T['TValue'] => epochMillisecondsLongToMs(values, index * 2);
/** @ignore */
const atTimestampMicrosecond = <T extends TimestampMicrosecond>({ values }: Data<T>, index: number): T['TValue'] => epochMicrosecondsLongToMs(values, index * 2);
/** @ignore */
const atTimestampNanosecond = <T extends TimestampNanosecond>({ values }: Data<T>, index: number): T['TValue'] => epochNanosecondsLongToMs(values, index * 2);
/* istanbul ignore next */
/** @ignore */
const atTimestamp = <T extends Timestamp>(data: Data<T>, index: number): T['TValue'] => {
    switch (data.type.unit) {
        case TimeUnit.SECOND: return atTimestampSecond(data as Data<TimestampSecond>, index);
        case TimeUnit.MILLISECOND: return atTimestampMillisecond(data as Data<TimestampMillisecond>, index);
        case TimeUnit.MICROSECOND: return atTimestampMicrosecond(data as Data<TimestampMicrosecond>, index);
        case TimeUnit.NANOSECOND: return atTimestampNanosecond(data as Data<TimestampNanosecond>, index);
    }
};

/** @ignore */
const atTimeSecond = <T extends TimeSecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const atTimeMillisecond = <T extends TimeMillisecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const atTimeMicrosecond = <T extends TimeMicrosecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const atTimeNanosecond = <T extends TimeNanosecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/* istanbul ignore next */
/** @ignore */
const atTime = <T extends Time>(data: Data<T>, index: number): T['TValue'] => {
    switch (data.type.unit) {
        case TimeUnit.SECOND: return atTimeSecond(data as Data<TimeSecond>, index);
        case TimeUnit.MILLISECOND: return atTimeMillisecond(data as Data<TimeMillisecond>, index);
        case TimeUnit.MICROSECOND: return atTimeMicrosecond(data as Data<TimeMicrosecond>, index);
        case TimeUnit.NANOSECOND: return atTimeNanosecond(data as Data<TimeNanosecond>, index);
    }
};

/** @ignore */
const atDecimal = <T extends Decimal>({ values, stride }: Data<T>, index: number): T['TValue'] => BN.decimal(values.subarray(stride * index, stride * (index + 1)));

/** @ignore */
const atList = <T extends List>(data: Data<T>, index: number): T['TValue'] => {
    const { valueOffsets, stride, children } = data;
    const { [index * stride]: begin, [index * stride + 1]: end } = valueOffsets;
    const child: Data<T['valueType']> = children[0];
    const slice = child.slice(begin, end - begin);
    return new Vector([slice]) as T['TValue'];
};

/** @ignore */
const atMap = <T extends Map_>(data: Data<T>, index: number): T['TValue'] => {
    const { valueOffsets, children } = data;
    const { [index]: begin, [index + 1]: end } = valueOffsets;
    const child = children[0] as Data<T['childType']>;
    return new MapRow(child.slice(begin, end - begin));
};

/** @ignore */
const atStruct = <T extends Struct>(data: Data<T>, index: number): T['TValue'] => {
    return new StructRow(data, index) as StructRowProxy<T['TValue']>;
};

/* istanbul ignore next */
/** @ignore */
const atUnion = <
    D extends Data<Union> | Data<DenseUnion> | Data<SparseUnion>
>(data: D, index: number): D['TValue'] => {
    return data.type.mode === UnionMode.Dense ?
        atDenseUnion(data as Data<DenseUnion>, index) :
        atSparseUnion(data as Data<SparseUnion>, index);
};

/** @ignore */
const atDenseUnion = <T extends DenseUnion>(data: Data<T>, index: number): T['TValue'] => {
    const childIndex = data.type.typeIdToChildIndex[data.typeIds[index]];
    const child = data.children[childIndex];
    return instance.visit(child, data.valueOffsets[index]);
};

/** @ignore */
const atSparseUnion = <T extends SparseUnion>(data: Data<T>, index: number): T['TValue'] => {
    const childIndex = data.type.typeIdToChildIndex[data.typeIds[index]];
    const child = data.children[childIndex];
    return instance.visit(child, index);
};

/** @ignore */
const atDictionary = <T extends Dictionary>(data: Data<T>, index: number): T['TValue'] => {
    return data.dictionary?.at(data.values[index]);
};

/* istanbul ignore next */
/** @ignore */
const atInterval = <T extends Interval>(data: Data<T>, index: number): T['TValue'] =>
    (data.type.unit === IntervalUnit.DAY_TIME)
        ? atIntervalDayTime(data as Data<IntervalDayTime>, index)
        : atIntervalYearMonth(data as Data<IntervalYearMonth>, index);

/** @ignore */
const atIntervalDayTime = <T extends IntervalDayTime>({ values }: Data<T>, index: number): T['TValue'] => values.subarray(2 * index, 2 * (index + 1));

/** @ignore */
const atIntervalYearMonth = <T extends IntervalYearMonth>({ values }: Data<T>, index: number): T['TValue'] => {
    const interval = values[index];
    const int32s = new Int32Array(2);
    int32s[0] = Math.trunc(interval / 12); /* years */
    int32s[1] = Math.trunc(interval % 12); /* months */
    return int32s;
};

/** @ignore */
const atDurationSecond = <T extends DurationSecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const atDurationMillisecond = <T extends DurationMillisecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const atDurationMicrosecond = <T extends DurationMicrosecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/** @ignore */
const atDurationNanosecond = <T extends DurationNanosecond>({ values }: Data<T>, index: number): T['TValue'] => values[index];
/* istanbul ignore next */
/** @ignore */
const atDuration = <T extends Duration>(data: Data<T>, index: number): T['TValue'] => {
    switch (data.type.unit) {
        case TimeUnit.SECOND: return atDurationSecond(data as Data<DurationSecond>, index);
        case TimeUnit.MILLISECOND: return atDurationMillisecond(data as Data<DurationMillisecond>, index);
        case TimeUnit.MICROSECOND: return atDurationMicrosecond(data as Data<DurationMicrosecond>, index);
        case TimeUnit.NANOSECOND: return atDurationNanosecond(data as Data<DurationNanosecond>, index);
    }
};

/** @ignore */
const atFixedSizeList = <T extends FixedSizeList>(data: Data<T>, index: number): T['TValue'] => {
    const { stride, children } = data;
    const child: Data<T['valueType']> = children[0];
    const slice = child.slice(index * stride, stride);
    return new Vector([slice]);
};

AtVisitor.prototype.visitNull = wrapAt(atNull);
AtVisitor.prototype.visitBool = wrapAt(atBool);
AtVisitor.prototype.visitInt = wrapAt(atInt);
AtVisitor.prototype.visitInt8 = wrapAt(atNumeric);
AtVisitor.prototype.visitInt16 = wrapAt(atNumeric);
AtVisitor.prototype.visitInt32 = wrapAt(atNumeric);
AtVisitor.prototype.visitInt64 = wrapAt(atBigInts);
AtVisitor.prototype.visitUint8 = wrapAt(atNumeric);
AtVisitor.prototype.visitUint16 = wrapAt(atNumeric);
AtVisitor.prototype.visitUint32 = wrapAt(atNumeric);
AtVisitor.prototype.visitUint64 = wrapAt(atBigInts);
AtVisitor.prototype.visitFloat = wrapAt(atFloat);
AtVisitor.prototype.visitFloat16 = wrapAt(atFloat16);
AtVisitor.prototype.visitFloat32 = wrapAt(atNumeric);
AtVisitor.prototype.visitFloat64 = wrapAt(atNumeric);
AtVisitor.prototype.visitUtf8 = wrapAt(atUtf8);
AtVisitor.prototype.visitLargeUtf8 = wrapAt(atUtf8);
AtVisitor.prototype.visitBinary = wrapAt(atBinary);
AtVisitor.prototype.visitLargeBinary = wrapAt(atBinary);
AtVisitor.prototype.visitFixedSizeBinary = wrapAt(atFixedSizeBinary);
AtVisitor.prototype.visitDate = wrapAt(atDate);
AtVisitor.prototype.visitDateDay = wrapAt(atDateDay);
AtVisitor.prototype.visitDateMillisecond = wrapAt(atDateMillisecond);
AtVisitor.prototype.visitTimestamp = wrapAt(atTimestamp);
AtVisitor.prototype.visitTimestampSecond = wrapAt(atTimestampSecond);
AtVisitor.prototype.visitTimestampMillisecond = wrapAt(atTimestampMillisecond);
AtVisitor.prototype.visitTimestampMicrosecond = wrapAt(atTimestampMicrosecond);
AtVisitor.prototype.visitTimestampNanosecond = wrapAt(atTimestampNanosecond);
AtVisitor.prototype.visitTime = wrapAt(atTime);
AtVisitor.prototype.visitTimeSecond = wrapAt(atTimeSecond);
AtVisitor.prototype.visitTimeMillisecond = wrapAt(atTimeMillisecond);
AtVisitor.prototype.visitTimeMicrosecond = wrapAt(atTimeMicrosecond);
AtVisitor.prototype.visitTimeNanosecond = wrapAt(atTimeNanosecond);
AtVisitor.prototype.visitDecimal = wrapAt(atDecimal);
AtVisitor.prototype.visitList = wrapAt(atList);
AtVisitor.prototype.visitStruct = wrapAt(atStruct);
AtVisitor.prototype.visitUnion = wrapAt(atUnion);
AtVisitor.prototype.visitDenseUnion = wrapAt(atDenseUnion);
AtVisitor.prototype.visitSparseUnion = wrapAt(atSparseUnion);
AtVisitor.prototype.visitDictionary = wrapAt(atDictionary);
AtVisitor.prototype.visitInterval = wrapAt(atInterval);
AtVisitor.prototype.visitIntervalDayTime = wrapAt(atIntervalDayTime);
AtVisitor.prototype.visitIntervalYearMonth = wrapAt(atIntervalYearMonth);
AtVisitor.prototype.visitDuration = wrapAt(atDuration);
AtVisitor.prototype.visitDurationSecond = wrapAt(atDurationSecond);
AtVisitor.prototype.visitDurationMillisecond = wrapAt(atDurationMillisecond);
AtVisitor.prototype.visitDurationMicrosecond = wrapAt(atDurationMicrosecond);
AtVisitor.prototype.visitDurationNanosecond = wrapAt(atDurationNanosecond);
AtVisitor.prototype.visitFixedSizeList = wrapAt(atFixedSizeList);
AtVisitor.prototype.visitMap = wrapAt(atMap);

/** @ignore */
export const instance = new AtVisitor();
