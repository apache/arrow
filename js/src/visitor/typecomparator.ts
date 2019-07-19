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
import { VectorType } from '../interfaces';
import { Schema, Field } from '../schema';
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
export interface TypeComparator extends Visitor {
    visit<T extends DataType>(type: T, other?: DataType | null): other is T;
    visitMany<T extends DataType>(nodes: T[], others?: DataType[] | null): boolean[];
    getVisitFn<T extends DataType>(node: VectorType<T> |  Data<T> | T): (other?: DataType | null) => other is T;
    visitNull                 <T extends Null>                 (type: T, other?: DataType | null): other is T;
    visitBool                 <T extends Bool>                 (type: T, other?: DataType | null): other is T;
    visitInt                  <T extends Int>                  (type: T, other?: DataType | null): other is T;
    visitInt8                 <T extends Int8>                 (type: T, other?: DataType | null): other is T;
    visitInt16                <T extends Int16>                (type: T, other?: DataType | null): other is T;
    visitInt32                <T extends Int32>                (type: T, other?: DataType | null): other is T;
    visitInt64                <T extends Int64>                (type: T, other?: DataType | null): other is T;
    visitUint8                <T extends Uint8>                (type: T, other?: DataType | null): other is T;
    visitUint16               <T extends Uint16>               (type: T, other?: DataType | null): other is T;
    visitUint32               <T extends Uint32>               (type: T, other?: DataType | null): other is T;
    visitUint64               <T extends Uint64>               (type: T, other?: DataType | null): other is T;
    visitFloat                <T extends Float>                (type: T, other?: DataType | null): other is T;
    visitFloat16              <T extends Float16>              (type: T, other?: DataType | null): other is T;
    visitFloat32              <T extends Float32>              (type: T, other?: DataType | null): other is T;
    visitFloat64              <T extends Float64>              (type: T, other?: DataType | null): other is T;
    visitUtf8                 <T extends Utf8>                 (type: T, other?: DataType | null): other is T;
    visitBinary               <T extends Binary>               (type: T, other?: DataType | null): other is T;
    visitFixedSizeBinary      <T extends FixedSizeBinary>      (type: T, other?: DataType | null): other is T;
    visitDate                 <T extends Date_>                (type: T, other?: DataType | null): other is T;
    visitDateDay              <T extends DateDay>              (type: T, other?: DataType | null): other is T;
    visitDateMillisecond      <T extends DateMillisecond>      (type: T, other?: DataType | null): other is T;
    visitTimestamp            <T extends Timestamp>            (type: T, other?: DataType | null): other is T;
    visitTimestampSecond      <T extends TimestampSecond>      (type: T, other?: DataType | null): other is T;
    visitTimestampMillisecond <T extends TimestampMillisecond> (type: T, other?: DataType | null): other is T;
    visitTimestampMicrosecond <T extends TimestampMicrosecond> (type: T, other?: DataType | null): other is T;
    visitTimestampNanosecond  <T extends TimestampNanosecond>  (type: T, other?: DataType | null): other is T;
    visitTime                 <T extends Time>                 (type: T, other?: DataType | null): other is T;
    visitTimeSecond           <T extends TimeSecond>           (type: T, other?: DataType | null): other is T;
    visitTimeMillisecond      <T extends TimeMillisecond>      (type: T, other?: DataType | null): other is T;
    visitTimeMicrosecond      <T extends TimeMicrosecond>      (type: T, other?: DataType | null): other is T;
    visitTimeNanosecond       <T extends TimeNanosecond>       (type: T, other?: DataType | null): other is T;
    visitDecimal              <T extends Decimal>              (type: T, other?: DataType | null): other is T;
    visitList                 <T extends List>                 (type: T, other?: DataType | null): other is T;
    visitStruct               <T extends Struct>               (type: T, other?: DataType | null): other is T;
    visitUnion                <T extends Union>                (type: T, other?: DataType | null): other is T;
    visitDenseUnion           <T extends DenseUnion>           (type: T, other?: DataType | null): other is T;
    visitSparseUnion          <T extends SparseUnion>          (type: T, other?: DataType | null): other is T;
    visitDictionary           <T extends Dictionary>           (type: T, other?: DataType | null): other is T;
    visitInterval             <T extends Interval>             (type: T, other?: DataType | null): other is T;
    visitIntervalDayTime      <T extends IntervalDayTime>      (type: T, other?: DataType | null): other is T;
    visitIntervalYearMonth    <T extends IntervalYearMonth>    (type: T, other?: DataType | null): other is T;
    visitFixedSizeList        <T extends FixedSizeList>        (type: T, other?: DataType | null): other is T;
    visitMap                  <T extends Map_>                 (type: T, other?: DataType | null): other is T;
}

/** @ignore */
export class TypeComparator extends Visitor {
    compareSchemas<T extends { [key: string]: DataType }>(schema: Schema<T>, other?: Schema | null): other is Schema<T> {
        return (schema === other) || (
            other instanceof schema.constructor &&
            instance.compareFields(schema.fields, other.fields)
        );
    }
    compareFields<T extends { [key: string]: DataType }>(fields: Field<T[keyof T]>[], others?: Field[] | null): others is Field<T[keyof T]>[] {
        return (fields === others) || (
            Array.isArray(fields) &&
            Array.isArray(others) &&
            fields.length === others.length &&
            fields.every((f, i) => instance.compareField(f, others[i]))
        );
    }
    compareField<T extends DataType = any>(field: Field<T>, other?: Field | null): other is Field<T> {
        return (field === other) || (
            other instanceof field.constructor &&
            field.name === other.name &&
            field.nullable === other.nullable &&
            instance.visit(field.type, other.type)
        );
    }
}

function compareConstructor<T extends DataType>(type: T, other?: DataType | null): other is T {
    return other instanceof type.constructor;
}

function compareAny<T extends DataType>(type: T, other?: DataType | null): other is T {
    return (type === other) || compareConstructor(type, other);
}

function compareInt<T extends Int>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.bitWidth === other.bitWidth &&
        type.isSigned === other.isSigned
    );
}

function compareFloat<T extends Float>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.precision === other.precision
    );
}

function compareFixedSizeBinary<T extends FixedSizeBinary>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.byteWidth === other.byteWidth
    );
}

function compareDate<T extends Date_>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.unit === other.unit
    );
}

function compareTimestamp<T extends Timestamp>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.unit === other.unit &&
        type.timezone === other.timezone
    );
}

function compareTime<T extends Time>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.unit === other.unit &&
        type.bitWidth === other.bitWidth
    );
}

function compareList<T extends List>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.children.length === other.children.length &&
        instance.compareFields(type.children, other.children)
    );
}

function compareStruct<T extends Struct>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.children.length === other.children.length &&
        instance.compareFields(type.children, other.children)
    );
}

function compareUnion<T extends Union>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.mode === other.mode &&
        type.typeIds.every((x, i) => x === other.typeIds[i]) &&
        instance.compareFields(type.children, other.children)
    );
}

function compareDictionary<T extends Dictionary>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.id === other.id &&
        type.isOrdered === other.isOrdered &&
        instance.visit(<any> type.indices, other.indices) &&
        instance.visit(type.dictionary, other.dictionary)
    );
}

function compareInterval<T extends Interval>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.unit === other.unit
    );
}

function compareFixedSizeList<T extends FixedSizeList>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.listSize === other.listSize &&
        type.children.length === other.children.length &&
        instance.compareFields(type.children, other.children)
    );
}

function compareMap<T extends Map_>(type: T, other?: DataType | null): other is T {
    return (type === other) || (
        compareConstructor(type, other) &&
        type.keysSorted === other.keysSorted &&
        type.children.length === other.children.length &&
        instance.compareFields(type.children, other.children)
    );
}

TypeComparator.prototype.visitNull                 =             compareAny;
TypeComparator.prototype.visitBool                 =             compareAny;
TypeComparator.prototype.visitInt                  =             compareInt;
TypeComparator.prototype.visitInt8                 =             compareInt;
TypeComparator.prototype.visitInt16                =             compareInt;
TypeComparator.prototype.visitInt32                =             compareInt;
TypeComparator.prototype.visitInt64                =             compareInt;
TypeComparator.prototype.visitUint8                =             compareInt;
TypeComparator.prototype.visitUint16               =             compareInt;
TypeComparator.prototype.visitUint32               =             compareInt;
TypeComparator.prototype.visitUint64               =             compareInt;
TypeComparator.prototype.visitFloat                =           compareFloat;
TypeComparator.prototype.visitFloat16              =           compareFloat;
TypeComparator.prototype.visitFloat32              =           compareFloat;
TypeComparator.prototype.visitFloat64              =           compareFloat;
TypeComparator.prototype.visitUtf8                 =             compareAny;
TypeComparator.prototype.visitBinary               =             compareAny;
TypeComparator.prototype.visitFixedSizeBinary      = compareFixedSizeBinary;
TypeComparator.prototype.visitDate                 =            compareDate;
TypeComparator.prototype.visitDateDay              =            compareDate;
TypeComparator.prototype.visitDateMillisecond      =            compareDate;
TypeComparator.prototype.visitTimestamp            =       compareTimestamp;
TypeComparator.prototype.visitTimestampSecond      =       compareTimestamp;
TypeComparator.prototype.visitTimestampMillisecond =       compareTimestamp;
TypeComparator.prototype.visitTimestampMicrosecond =       compareTimestamp;
TypeComparator.prototype.visitTimestampNanosecond  =       compareTimestamp;
TypeComparator.prototype.visitTime                 =            compareTime;
TypeComparator.prototype.visitTimeSecond           =            compareTime;
TypeComparator.prototype.visitTimeMillisecond      =            compareTime;
TypeComparator.prototype.visitTimeMicrosecond      =            compareTime;
TypeComparator.prototype.visitTimeNanosecond       =            compareTime;
TypeComparator.prototype.visitDecimal              =             compareAny;
TypeComparator.prototype.visitList                 =            compareList;
TypeComparator.prototype.visitStruct               =          compareStruct;
TypeComparator.prototype.visitUnion                =           compareUnion;
TypeComparator.prototype.visitDenseUnion           =           compareUnion;
TypeComparator.prototype.visitSparseUnion          =           compareUnion;
TypeComparator.prototype.visitDictionary           =      compareDictionary;
TypeComparator.prototype.visitInterval             =        compareInterval;
TypeComparator.prototype.visitIntervalDayTime      =        compareInterval;
TypeComparator.prototype.visitIntervalYearMonth    =        compareInterval;
TypeComparator.prototype.visitFixedSizeList        =   compareFixedSizeList;
TypeComparator.prototype.visitMap                  =             compareMap;

/** @ignore */
export const instance = new TypeComparator();
