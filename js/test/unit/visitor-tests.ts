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

import { Visitor } from '../Arrow';
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
} from '../Arrow';

class BasicVisitor extends Visitor {
    public type: DataType | undefined;
    public visitNull                 <T extends Null>                 (type: T) { return (this.type = type); }
    public visitBool                 <T extends Bool>                 (type: T) { return (this.type = type); }
    public visitInt                  <T extends Int>                  (type: T) { return (this.type = type); }
    public visitFloat                <T extends Float>                (type: T) { return (this.type = type); }
    public visitUtf8                 <T extends Utf8>                 (type: T) { return (this.type = type); }
    public visitBinary               <T extends Binary>               (type: T) { return (this.type = type); }
    public visitFixedSizeBinary      <T extends FixedSizeBinary>      (type: T) { return (this.type = type); }
    public visitDate                 <T extends Date_>                (type: T) { return (this.type = type); }
    public visitTimestamp            <T extends Timestamp>            (type: T) { return (this.type = type); }
    public visitTime                 <T extends Time>                 (type: T) { return (this.type = type); }
    public visitDecimal              <T extends Decimal>              (type: T) { return (this.type = type); }
    public visitList                 <T extends List>                 (type: T) { return (this.type = type); }
    public visitStruct               <T extends Struct>               (type: T) { return (this.type = type); }
    public visitUnion                <T extends Union>                (type: T) { return (this.type = type); }
    public visitDictionary           <T extends Dictionary>           (type: T) { return (this.type = type); }
    public visitInterval             <T extends Interval>             (type: T) { return (this.type = type); }
    public visitFixedSizeList        <T extends FixedSizeList>        (type: T) { return (this.type = type); }
    public visitMap                  <T extends Map_>                 (type: T) { return (this.type = type); }
}

class FeatureVisitor extends Visitor {
    public type: DataType | undefined;
    public visitNull                 <T extends Null>                 (type: T) { return (this.type = type); }
    public visitBool                 <T extends Bool>                 (type: T) { return (this.type = type); }
    public visitInt8                 <T extends Int8>                 (type: T) { return (this.type = type); }
    public visitInt16                <T extends Int16>                (type: T) { return (this.type = type); }
    public visitInt32                <T extends Int32>                (type: T) { return (this.type = type); }
    public visitInt64                <T extends Int64>                (type: T) { return (this.type = type); }
    public visitUint8                <T extends Uint8>                (type: T) { return (this.type = type); }
    public visitUint16               <T extends Uint16>               (type: T) { return (this.type = type); }
    public visitUint32               <T extends Uint32>               (type: T) { return (this.type = type); }
    public visitUint64               <T extends Uint64>               (type: T) { return (this.type = type); }
    public visitFloat16              <T extends Float16>              (type: T) { return (this.type = type); }
    public visitFloat32              <T extends Float32>              (type: T) { return (this.type = type); }
    public visitFloat64              <T extends Float64>              (type: T) { return (this.type = type); }
    public visitUtf8                 <T extends Utf8>                 (type: T) { return (this.type = type); }
    public visitBinary               <T extends Binary>               (type: T) { return (this.type = type); }
    public visitFixedSizeBinary      <T extends FixedSizeBinary>      (type: T) { return (this.type = type); }
    public visitDateDay              <T extends DateDay>              (type: T) { return (this.type = type); }
    public visitDateMillisecond      <T extends DateMillisecond>      (type: T) { return (this.type = type); }
    public visitTimestampSecond      <T extends TimestampSecond>      (type: T) { return (this.type = type); }
    public visitTimestampMillisecond <T extends TimestampMillisecond> (type: T) { return (this.type = type); }
    public visitTimestampMicrosecond <T extends TimestampMicrosecond> (type: T) { return (this.type = type); }
    public visitTimestampNanosecond  <T extends TimestampNanosecond>  (type: T) { return (this.type = type); }
    public visitTimeSecond           <T extends TimeSecond>           (type: T) { return (this.type = type); }
    public visitTimeMillisecond      <T extends TimeMillisecond>      (type: T) { return (this.type = type); }
    public visitTimeMicrosecond      <T extends TimeMicrosecond>      (type: T) { return (this.type = type); }
    public visitTimeNanosecond       <T extends TimeNanosecond>       (type: T) { return (this.type = type); }
    public visitDecimal              <T extends Decimal>              (type: T) { return (this.type = type); }
    public visitList                 <T extends List>                 (type: T) { return (this.type = type); }
    public visitStruct               <T extends Struct>               (type: T) { return (this.type = type); }
    public visitDenseUnion           <T extends DenseUnion>           (type: T) { return (this.type = type); }
    public visitSparseUnion          <T extends SparseUnion>          (type: T) { return (this.type = type); }
    public visitDictionary           <T extends Dictionary>           (type: T) { return (this.type = type); }
    public visitIntervalDayTime      <T extends IntervalDayTime>      (type: T) { return (this.type = type); }
    public visitIntervalYearMonth    <T extends IntervalYearMonth>    (type: T) { return (this.type = type); }
    public visitFixedSizeList        <T extends FixedSizeList>        (type: T) { return (this.type = type); }
    public visitMap                  <T extends Map_>                 (type: T) { return (this.type = type); }
}

describe('Visitor', () => {

    describe('uses the base methods when no feature methods are implemented', () => {
        test(`visits Null types`, () => validateBasicVisitor(new Null()));
        test(`visits Bool types`, () => validateBasicVisitor(new Bool()));
        test(`visits Int types`, () => validateBasicVisitor(new Int(true, 32)));
        test(`visits Float types`, () => validateBasicVisitor(new Float(0)));
        test(`visits Utf8 types`, () => validateBasicVisitor(new Utf8()));
        test(`visits Binary types`, () => validateBasicVisitor(new Binary()));
        test(`visits FixedSizeBinary types`, () => validateBasicVisitor(new FixedSizeBinary(128)));
        test(`visits Date types`, () => validateBasicVisitor(new Date_(0)));
        test(`visits Timestamp types`, () => validateBasicVisitor(new Timestamp(0, 'UTC')));
        test(`visits Time types`, () => validateBasicVisitor(new Time(0, 64)));
        test(`visits Decimal types`, () => validateBasicVisitor(new Decimal(2, 9)));
        test(`visits List types`, () => validateBasicVisitor(new List(null as any)));
        test(`visits Struct types`, () => validateBasicVisitor(new Struct([] as any[])));
        test(`visits Union types`, () => validateBasicVisitor(new Union(0, [] as any[], [] as any[])));
        test(`visits Dictionary types`, () => validateBasicVisitor(new Dictionary(null as any, null as any)));
        test(`visits Interval types`, () => validateBasicVisitor(new Interval(0)));
        test(`visits FixedSizeList types`, () => validateBasicVisitor(new FixedSizeList(2, null as any)));
        test(`visits Map types`, () => validateBasicVisitor(new Map_([] as any[])));
        function validateBasicVisitor<T extends DataType>(type: T) {
            const visitor = new BasicVisitor();
            const result = visitor.visit(type);
            expect(result).toBe(type);
            expect(visitor.type).toBe(type);
        }
    });

    describe(`uses the feature methods instead of the base methods when they're implemented`, () => {

        test(`visits Null types`, () => validateFeatureVisitor(new Null()));
        test(`visits Bool types`, () => validateFeatureVisitor(new Bool()));
        test(`visits Int8 types`, () => validateFeatureVisitor(new Int8()));
        test(`visits Int16 types`, () => validateFeatureVisitor(new Int16()));
        test(`visits Int32 types`, () => validateFeatureVisitor(new Int32()));
        test(`visits Int64 types`, () => validateFeatureVisitor(new Int64()));
        test(`visits Uint8 types`, () => validateFeatureVisitor(new Uint8()));
        test(`visits Uint16 types`, () => validateFeatureVisitor(new Uint16()));
        test(`visits Uint32 types`, () => validateFeatureVisitor(new Uint32()));
        test(`visits Uint64 types`, () => validateFeatureVisitor(new Uint64()));
        test(`visits Float16 types`, () => validateFeatureVisitor(new Float16()));
        test(`visits Float32 types`, () => validateFeatureVisitor(new Float32()));
        test(`visits Float64 types`, () => validateFeatureVisitor(new Float64()));
        test(`visits Utf8 types`, () => validateFeatureVisitor(new Utf8()));
        test(`visits Binary types`, () => validateFeatureVisitor(new Binary()));
        test(`visits FixedSizeBinary types`, () => validateFeatureVisitor(new FixedSizeBinary(128)));
        test(`visits DateDay types`, () => validateFeatureVisitor(new DateDay()));
        test(`visits DateMillisecond types`, () => validateFeatureVisitor(new DateMillisecond()));
        test(`visits TimestampSecond types`, () => validateFeatureVisitor(new TimestampSecond()));
        test(`visits TimestampMillisecond types`, () => validateFeatureVisitor(new TimestampMillisecond()));
        test(`visits TimestampMicrosecond types`, () => validateFeatureVisitor(new TimestampMicrosecond()));
        test(`visits TimestampNanosecond types`, () => validateFeatureVisitor(new TimestampNanosecond()));
        test(`visits TimeSecond types`, () => validateFeatureVisitor(new TimeSecond()));
        test(`visits TimeMillisecond types`, () => validateFeatureVisitor(new TimeMillisecond()));
        test(`visits TimeMicrosecond types`, () => validateFeatureVisitor(new TimeMicrosecond()));
        test(`visits TimeNanosecond types`, () => validateFeatureVisitor(new TimeNanosecond()));
        test(`visits Decimal types`, () => validateFeatureVisitor(new Decimal(2, 9)));
        test(`visits List types`, () => validateFeatureVisitor(new List(null as any)));
        test(`visits Struct types`, () => validateFeatureVisitor(new Struct([] as any[])));
        test(`visits DenseUnion types`, () => validateFeatureVisitor(new DenseUnion([] as any[], [] as any[])));
        test(`visits SparseUnion types`, () => validateFeatureVisitor(new SparseUnion([] as any[], [] as any[])));
        test(`visits Dictionary types`, () => validateFeatureVisitor(new Dictionary(null as any, null as any)));
        test(`visits IntervalDayTime types`, () => validateFeatureVisitor(new IntervalDayTime()));
        test(`visits IntervalYearMonth types`, () => validateFeatureVisitor(new IntervalYearMonth()));
        test(`visits FixedSizeList types`, () => validateFeatureVisitor(new FixedSizeList(2, null as any)));
        test(`visits Map types`, () => validateFeatureVisitor(new Map_([] as any[])));

        function validateFeatureVisitor<T extends DataType>(type: T) {
            const visitor = new FeatureVisitor();
            const result = visitor.visit(type);
            expect(result).toBe(type);
            expect(visitor.type).toBe(type);
        }
    });
});
