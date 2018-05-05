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

import { Vector } from './vector';
import { Type, DataType, Dictionary } from './type';
import { Utf8, Binary, Decimal, FixedSizeBinary } from './type';
import { List, FixedSizeList, Union, Map_, Struct } from './type';
import { Bool, Null, Int, Float, Date_, Time, Interval, Timestamp } from './type';

export interface VisitorNode {
    acceptTypeVisitor(visitor: TypeVisitor): any;
    acceptVectorVisitor(visitor: VectorVisitor): any;
}

export abstract class TypeVisitor {
    visit(type: Partial<VisitorNode>): any {
        return type.acceptTypeVisitor && type.acceptTypeVisitor(this) || null;
    }
    visitMany(types: Partial<VisitorNode>[]): any[] {
        return types.map((type) => this.visit(type));
    }
    abstract visitNull?(type: Null): any;
    abstract visitBool?(type: Bool): any;
    abstract visitInt?(type: Int): any;
    abstract visitFloat?(type: Float): any;
    abstract visitUtf8?(type: Utf8): any;
    abstract visitBinary?(type: Binary): any;
    abstract visitFixedSizeBinary?(type: FixedSizeBinary): any;
    abstract visitDate?(type: Date_): any;
    abstract visitTimestamp?(type: Timestamp): any;
    abstract visitTime?(type: Time): any;
    abstract visitDecimal?(type: Decimal): any;
    abstract visitList?(type: List): any;
    abstract visitStruct?(type: Struct): any;
    abstract visitUnion?(type: Union<any>): any;
    abstract visitDictionary?(type: Dictionary): any;
    abstract visitInterval?(type: Interval): any;
    abstract visitFixedSizeList?(type: FixedSizeList): any;
    abstract visitMap?(type: Map_): any;

    static visitTypeInline<T extends DataType>(visitor: TypeVisitor, type: T): any {
        switch (type.TType) {
            case Type.Null:            return visitor.visitNull            && visitor.visitNull(type            as any as Null);
            case Type.Int:             return visitor.visitInt             && visitor.visitInt(type             as any as Int);
            case Type.Float:           return visitor.visitFloat           && visitor.visitFloat(type           as any as Float);
            case Type.Binary:          return visitor.visitBinary          && visitor.visitBinary(type          as any as Binary);
            case Type.Utf8:            return visitor.visitUtf8            && visitor.visitUtf8(type            as any as Utf8);
            case Type.Bool:            return visitor.visitBool            && visitor.visitBool(type            as any as Bool);
            case Type.Decimal:         return visitor.visitDecimal         && visitor.visitDecimal(type         as any as Decimal);
            case Type.Date:            return visitor.visitDate            && visitor.visitDate(type            as any as Date_);
            case Type.Time:            return visitor.visitTime            && visitor.visitTime(type            as any as Time);
            case Type.Timestamp:       return visitor.visitTimestamp       && visitor.visitTimestamp(type       as any as Timestamp);
            case Type.Interval:        return visitor.visitInterval        && visitor.visitInterval(type        as any as Interval);
            case Type.List:            return visitor.visitList            && visitor.visitList(type            as any as List<T>);
            case Type.Struct:          return visitor.visitStruct          && visitor.visitStruct(type          as any as Struct);
            case Type.Union:           return visitor.visitUnion           && visitor.visitUnion(type           as any as Union);
            case Type.FixedSizeBinary: return visitor.visitFixedSizeBinary && visitor.visitFixedSizeBinary(type as any as FixedSizeBinary);
            case Type.FixedSizeList:   return visitor.visitFixedSizeList   && visitor.visitFixedSizeList(type   as any as FixedSizeList);
            case Type.Map:             return visitor.visitMap             && visitor.visitMap(type             as any as Map_);
            case Type.Dictionary:      return visitor.visitDictionary      && visitor.visitDictionary(type      as any as Dictionary);
            default: return null;
        }
    }
}

export abstract class VectorVisitor {
    visit(vector: Partial<VisitorNode>): any {
        return vector.acceptVectorVisitor && vector.acceptVectorVisitor(this) || null;
    }
    visitMany(vectors: Partial<VisitorNode>[]): any[] {
        return vectors.map((vector) => this.visit(vector));
    }
    abstract visitNull?(vector: Vector<Null>): any;
    abstract visitBool?(vector: Vector<Bool>): any;
    abstract visitInt?(vector: Vector<Int>): any;
    abstract visitFloat?(vector: Vector<Float>): any;
    abstract visitUtf8?(vector: Vector<Utf8>): any;
    abstract visitBinary?(vector: Vector<Binary>): any;
    abstract visitFixedSizeBinary?(vector: Vector<FixedSizeBinary>): any;
    abstract visitDate?(vector: Vector<Date_>): any;
    abstract visitTimestamp?(vector: Vector<Timestamp>): any;
    abstract visitTime?(vector: Vector<Time>): any;
    abstract visitDecimal?(vector: Vector<Decimal>): any;
    abstract visitList?(vector: Vector<List>): any;
    abstract visitStruct?(vector: Vector<Struct>): any;
    abstract visitUnion?(vector: Vector<Union<any>>): any;
    abstract visitDictionary?(vector: Vector<Dictionary>): any;
    abstract visitInterval?(vector: Vector<Interval>): any;
    abstract visitFixedSizeList?(vector: Vector<FixedSizeList>): any;
    abstract visitMap?(vector: Vector<Map_>): any;

    static visitTypeInline<T extends DataType>(visitor: VectorVisitor, type: T, vector: Vector<T>): any {
        switch (type.TType) {
            case Type.Null:            return visitor.visitNull            && visitor.visitNull(vector            as any as Vector<Null>);
            case Type.Int:             return visitor.visitInt             && visitor.visitInt(vector             as any as Vector<Int>);
            case Type.Float:           return visitor.visitFloat           && visitor.visitFloat(vector           as any as Vector<Float>);
            case Type.Binary:          return visitor.visitBinary          && visitor.visitBinary(vector          as any as Vector<Binary>);
            case Type.Utf8:            return visitor.visitUtf8            && visitor.visitUtf8(vector            as any as Vector<Utf8>);
            case Type.Bool:            return visitor.visitBool            && visitor.visitBool(vector            as any as Vector<Bool>);
            case Type.Decimal:         return visitor.visitDecimal         && visitor.visitDecimal(vector         as any as Vector<Decimal>);
            case Type.Date:            return visitor.visitDate            && visitor.visitDate(vector            as any as Vector<Date_>);
            case Type.Time:            return visitor.visitTime            && visitor.visitTime(vector            as any as Vector<Time>);
            case Type.Timestamp:       return visitor.visitTimestamp       && visitor.visitTimestamp(vector       as any as Vector<Timestamp>);
            case Type.Interval:        return visitor.visitInterval        && visitor.visitInterval(vector        as any as Vector<Interval>);
            case Type.List:            return visitor.visitList            && visitor.visitList(vector            as any as Vector<List<T>>);
            case Type.Struct:          return visitor.visitStruct          && visitor.visitStruct(vector          as any as Vector<Struct>);
            case Type.Union:           return visitor.visitUnion           && visitor.visitUnion(vector           as any as Vector<Union>);
            case Type.FixedSizeBinary: return visitor.visitFixedSizeBinary && visitor.visitFixedSizeBinary(vector as any as Vector<FixedSizeBinary>);
            case Type.FixedSizeList:   return visitor.visitFixedSizeList   && visitor.visitFixedSizeList(vector   as any as Vector<FixedSizeList>);
            case Type.Map:             return visitor.visitMap             && visitor.visitMap(vector             as any as Vector<Map_>);
            case Type.Dictionary:      return visitor.visitDictionary      && visitor.visitDictionary(vector      as any as Vector<Dictionary>);
            default: return null;
        }
    }
}
