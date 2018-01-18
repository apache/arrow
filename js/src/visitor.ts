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
    // acceptMessageVisitor(visitor: MessageVisitor): any;
}

export abstract class TypeVisitor {
    visit(node: Partial<VisitorNode>): any {
        return node.acceptTypeVisitor!(this);
    }
    visitMany(nodes: Partial<VisitorNode>[]): any[] {
        return nodes.map((node) => this.visit(node));
    }
    abstract visitNull(node: Null): any;
    abstract visitBool(node: Bool): any;
    abstract visitInt(node: Int): any;
    abstract visitFloat(node: Float): any;
    abstract visitUtf8(node: Utf8): any;
    abstract visitBinary(node: Binary): any;
    abstract visitFixedSizeBinary(node: FixedSizeBinary): any;
    abstract visitDate(node: Date_): any;
    abstract visitTimestamp(node: Timestamp): any;
    abstract visitTime(node: Time): any;
    abstract visitDecimal(node: Decimal): any;
    abstract visitList(node: List): any;
    abstract visitStruct(node: Struct): any;
    abstract visitUnion(node: Union<any>): any;
    abstract visitDictionary(node: Dictionary): any;
    abstract visitInterval(node: Interval): any;
    abstract visitFixedSizeList(node: FixedSizeList): any;
    abstract visitMap(node: Map_): any;

    static visitTypeInline<T extends DataType>(visitor: TypeVisitor, type: T): any {
        switch (type.TType) {
            case Type.Null:            return visitor.visitNull(type            as any as Null);
            case Type.Int:             return visitor.visitInt(type             as any as Int);
            case Type.Float:           return visitor.visitFloat(type           as any as Float);
            case Type.Binary:          return visitor.visitBinary(type          as any as Binary);
            case Type.Utf8:            return visitor.visitUtf8(type            as any as Utf8);
            case Type.Bool:            return visitor.visitBool(type            as any as Bool);
            case Type.Decimal:         return visitor.visitDecimal(type         as any as Decimal);
            case Type.Date:            return visitor.visitDate(type            as any as Date_);
            case Type.Time:            return visitor.visitTime(type            as any as Time);
            case Type.Timestamp:       return visitor.visitTimestamp(type       as any as Timestamp);
            case Type.Interval:        return visitor.visitInterval(type        as any as Interval);
            case Type.List:            return visitor.visitList(type            as any as List<T>);
            case Type.Struct:          return visitor.visitStruct(type          as any as Struct);
            case Type.Union:           return visitor.visitUnion(type           as any as Union);
            case Type.FixedSizeBinary: return visitor.visitFixedSizeBinary(type as any as FixedSizeBinary);
            case Type.FixedSizeList:   return visitor.visitFixedSizeList(type   as any as FixedSizeList);
            case Type.Map:             return visitor.visitMap(type             as any as Map_);
            case Type.Dictionary:      return visitor.visitDictionary(type      as any as Dictionary);
            default: return null;
        }
    }
}

export abstract class VectorVisitor {
    visit(node: Partial<VisitorNode>): any {
        return node.acceptVectorVisitor!(this);
    }
    visitMany(nodes: Partial<VisitorNode>[]): any[] {
        return nodes.map((node) => this.visit(node));
    }
    abstract visitNullVector(node: Vector<Null>): any;
    abstract visitBoolVector(node: Vector<Bool>): any;
    abstract visitIntVector(node: Vector<Int>): any;
    abstract visitFloatVector(node: Vector<Float>): any;
    abstract visitUtf8Vector(node: Vector<Utf8>): any;
    abstract visitBinaryVector(node: Vector<Binary>): any;
    abstract visitFixedSizeBinaryVector(node: Vector<FixedSizeBinary>): any;
    abstract visitDateVector(node: Vector<Date_>): any;
    abstract visitTimestampVector(node: Vector<Timestamp>): any;
    abstract visitTimeVector(node: Vector<Time>): any;
    abstract visitDecimalVector(node: Vector<Decimal>): any;
    abstract visitListVector(node: Vector<List>): any;
    abstract visitStructVector(node: Vector<Struct>): any;
    abstract visitUnionVector(node: Vector<Union<any>>): any;
    abstract visitDictionaryVector(node: Vector<Dictionary>): any;
    abstract visitIntervalVector(node: Vector<Interval>): any;
    abstract visitFixedSizeListVector(node: Vector<FixedSizeList>): any;
    abstract visitMapVector(node: Vector<Map_>): any;

    static visitTypeInline<T extends DataType>(visitor: VectorVisitor, type: T, vector: Vector<T>): any {
        switch (type.TType) {
            case Type.Null:            return visitor.visitNullVector(vector            as any as Vector<Null>);
            case Type.Int:             return visitor.visitIntVector(vector             as any as Vector<Int>);
            case Type.Float:           return visitor.visitFloatVector(vector           as any as Vector<Float>);
            case Type.Binary:          return visitor.visitBinaryVector(vector          as any as Vector<Binary>);
            case Type.Utf8:            return visitor.visitUtf8Vector(vector            as any as Vector<Utf8>);
            case Type.Bool:            return visitor.visitBoolVector(vector            as any as Vector<Bool>);
            case Type.Decimal:         return visitor.visitDecimalVector(vector         as any as Vector<Decimal>);
            case Type.Date:            return visitor.visitDateVector(vector            as any as Vector<Date_>);
            case Type.Time:            return visitor.visitTimeVector(vector            as any as Vector<Time>);
            case Type.Timestamp:       return visitor.visitTimestampVector(vector       as any as Vector<Timestamp>);
            case Type.Interval:        return visitor.visitIntervalVector(vector        as any as Vector<Interval>);
            case Type.List:            return visitor.visitListVector(vector            as any as Vector<List<T>>);
            case Type.Struct:          return visitor.visitStructVector(vector          as any as Vector<Struct>);
            case Type.Union:           return visitor.visitUnionVector(vector           as any as Vector<Union>);
            case Type.FixedSizeBinary: return visitor.visitFixedSizeBinaryVector(vector as any as Vector<FixedSizeBinary>);
            case Type.FixedSizeList:   return visitor.visitFixedSizeListVector(vector   as any as Vector<FixedSizeList>);
            case Type.Map:             return visitor.visitMapVector(vector             as any as Vector<Map_>);
            case Type.Dictionary:      return visitor.visitDictionaryVector(vector      as any as Vector<Dictionary>);
            default: return null;
        }
    }
}

// import { Footer, Block } from './ipc/message';
// import { Field, FieldNode, Buffer } from './ipc/message';
// import { Message, Schema, RecordBatch, DictionaryBatch } from './ipc/message';

// export abstract class MessageVisitor {
//     visit(node: VisitorNode): any {
//         return node.acceptMessageVisitor(this);
//     }
//     visitMany(nodes: VisitorNode[]): any[] {
//         return nodes.map((node) => this.visit(node));
//     }
//     abstract visitFooter(node: Footer): any;
//     abstract visitBlock(node: Block): any;
//     abstract visitMessage(node: Message): any;
//     abstract visitSchema(node: Schema): any;
//     abstract visitField<T extends DataType>(node: Field<T>): any;
//     abstract visitBuffer(node: Buffer): any;
//     abstract visitFieldNode(node: FieldNode): any;
//     abstract visitDataType<T extends Type>(node: DataType<T>): any;
//     abstract visitDictionary(node: Dictionary): any;
//     abstract visitRecordBatch(node: RecordBatch): any;
//     abstract visitDictionaryBatch(node: DictionaryBatch): any;
// }
