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

/* tslint:disable:class-name */

import { align } from '../util/layout';
import * as Schema_ from './fb/Schema';
import * as Message_ from './fb/Message';
import { flatbuffers } from 'flatbuffers';
import Long = flatbuffers.Long;
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
import TimeUnit = Schema_.org.apache.arrow.flatbuf.TimeUnit;
import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
import UnionMode = Schema_.org.apache.arrow.flatbuf.UnionMode;
import Endianness = Schema_.org.apache.arrow.flatbuf.Endianness;
import IntervalUnit = Schema_.org.apache.arrow.flatbuf.IntervalUnit;
import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;
import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;

export type IntBitWidth = 8 | 16 | 32 | 64;
export type TimeBitWidth = IntBitWidth | 128;

export interface VisitorNode {
    accept(visitor: Visitor): any;
}

export abstract class Visitor<T = any> {
    visit(node: VisitorNode): T {
        return node.accept(this);
    }
    visitMany(nodes: VisitorNode[]): T[] {
        return nodes.map((node) => this.visit(node));
    }
    abstract visitFooter(node: Footer): any;
    abstract visitBlock(node: Block): any;
    abstract visitMessage(node: Message): any;
    abstract visitSchema(node: Schema): any;
    abstract visitField(node: Field): any;
    abstract visitBuffer(node: Buffer): any;
    abstract visitFieldNode(node: FieldNode): any;
    abstract visitRecordBatch(node: RecordBatch): any;
    abstract visitDictionaryBatch(node: DictionaryBatch): any;
    abstract visitDictionaryEncoding(node: DictionaryEncoding): any;
    abstract visitNullFieldType(node: Null): any;
    abstract visitIntFieldType(node: Int): any;
    abstract visitFloatingPointFieldType(node: FloatingPoint): any;
    abstract visitBinaryFieldType(node: Binary): any;
    abstract visitBoolFieldType(node: Bool): any;
    abstract visitUtf8FieldType(node: Utf8): any;
    abstract visitDecimalFieldType(node: Decimal): any;
    abstract visitDateFieldType(node: Date): any;
    abstract visitTimeFieldType(node: Time): any;
    abstract visitTimestampFieldType(node: Timestamp): any;
    abstract visitIntervalFieldType(node: Interval): any;
    abstract visitListFieldType(node: List): any;
    abstract visitStructFieldType(node: Struct): any;
    abstract visitUnionFieldType(node: Union): any;
    abstract visitFixedSizeBinaryFieldType(node: FixedSizeBinary): any;
    abstract visitFixedSizeListFieldType(node: FixedSizeList): any;
    abstract visitMapFieldType(node: Map_): any;
}

export class Footer implements VisitorNode {
    constructor(public dictionaryBatches: Block[], public recordBatches: Block[], public schema: Schema) {}
    accept(visitor: Visitor): any {
        return visitor.visitFooter(this);
    }
}

export class Block implements VisitorNode {
    constructor(public metaDataLength: number, public bodyLength: Long, public offset: Long) {}
    accept(visitor: Visitor): any {
        return visitor.visitBlock(this);
    }
}

export class Message implements VisitorNode {
    constructor(public version: MetadataVersion, public bodyLength: Long, public headerType: MessageHeader) {}
    isSchema(): this is Schema { return this.headerType === MessageHeader.Schema; }
    isRecordBatch(): this is RecordBatch { return this.headerType === MessageHeader.RecordBatch; }
    isDictionaryBatch(): this is DictionaryBatch { return this.headerType === MessageHeader.DictionaryBatch; }
    accept(visitor: Visitor): any {
        visitor.visitMessage(this);
    }
}

export class Schema extends Message {
    public dictionaries: Map<string, Field>;
    constructor(version: MetadataVersion, public fields: Field[], public customMetadata?: Map<string, string>, public endianness = Endianness.Little) {
        super(version, Long.ZERO, MessageHeader.Schema);
        const dictionaries = [] as Field[];
        for (let f: Field, i = -1, n = fields.length; ++i < n;) {
            if ((f = fields[i])) {
                f.dictionary && dictionaries.push(f);
                dictionaries.push(...f.dictionaries);
            }
        }
        this.dictionaries = new Map<string, Field>(dictionaries.map<[string, Field]>((f) => [
            f.dictionary!.dictionaryId.toFloat64().toString(), f
        ]));
    }
    accept(visitor: Visitor): any {
        return visitor.visitSchema(this);
    }
}

export class RecordBatch extends Message {
    constructor(version: MetadataVersion, public length: Long, public fieldNodes: FieldNode[], public buffers: Buffer[]) {
        super(version, new Long(buffers.reduce((s, b) => align(s + b.length.low + (b.offset.low - s), 8), 0), 0), MessageHeader.RecordBatch);
    }
    accept(visitor: Visitor) {
        return visitor.visitRecordBatch(this);
    }
}

export class DictionaryBatch extends Message {
    constructor(version: MetadataVersion, public dictionary: RecordBatch, public dictionaryId: Long, public isDelta: boolean) {
        super(version, dictionary.bodyLength, MessageHeader.DictionaryBatch);
    }
    get fieldNodes(): FieldNode[] { return this.dictionary.fieldNodes; }
    get buffers(): Buffer[] { return this.dictionary.buffers; }
    accept(visitor: Visitor) {
        return visitor.visitDictionaryBatch(this);
    }
    static atomicDictionaryId = 0;
}

export class Field implements VisitorNode {
    public dictionaries: Field[];
    constructor(public name: string,
                public type: FieldType,
                public typeType: Type,
                public nullable = false,
                public children: Field[] = [],
                public metadata?: Map<string, string> | null,
                public dictionary?: DictionaryEncoding | null) {
        const dictionaries = [] as Field[];
        for (let f: Field, i = -1, n = children.length; ++i < n;) {
            if ((f = children[i])) {
                f.dictionary && dictionaries.push(f);
                dictionaries.push(...f.dictionaries);
            }
        }
        this.dictionaries = dictionaries;
    }
    accept(visitor: Visitor): any {
        return visitor.visitField(this);
    }
    indexField() {
        return !this.dictionary ? this : new Field(
            this.name,
            this.dictionary.indexType, this.dictionary.indexType.type,
            this.nullable, this.children, this.metadata, this.dictionary
        );
    }
    toString() { return `Field name[${this.name}], nullable[${this.nullable}], type[${this.type.toString()}]`; }
}

export class Buffer implements VisitorNode {
    constructor(public offset: Long, public length: Long) {}
    accept(visitor: Visitor) {
        return visitor.visitBuffer(this);
    }
}

export class FieldNode implements VisitorNode {
    constructor(public length: Long, public nullCount: Long) {}
    accept(visitor: Visitor) {
        return visitor.visitFieldNode(this);
    }
}

export class DictionaryEncoding implements VisitorNode {
    public isOrdered: boolean;
    public dictionaryId: Long;
    public indexType: Int;
    constructor(indexType?: Int | null, dictionaryId?: Long | null, isOrdered?: boolean | null) {
        this.isOrdered = isOrdered || false;
        /* a dictionary index defaults to signed 32 bit int if unspecified */
        this.indexType = indexType || new Int(true, 32);
        this.dictionaryId = dictionaryId || new Long(DictionaryBatch.atomicDictionaryId++, 0);
    }
    accept(visitor: Visitor): any {
        return visitor.visitDictionaryEncoding(this);
    }
}

export abstract class FieldType implements VisitorNode {
    constructor(public type: Type) {}
    abstract accept(visitor: Visitor): any;
    isNull(): this is Null { return this.type === Type.Null; }
    isInt(): this is Int { return this.type === Type.Int; }
    isFloatingPoint(): this is FloatingPoint { return this.type === Type.FloatingPoint; }
    isBinary(): this is Binary { return this.type === Type.Binary; }
    isUtf8(): this is Utf8 { return this.type === Type.Utf8; }
    isBool(): this is Bool { return this.type === Type.Bool; }
    isDecimal(): this is Decimal { return this.type === Type.Decimal; }
    isDate(): this is Date { return this.type === Type.Date; }
    isTime(): this is Time { return this.type === Type.Time; }
    isTimestamp(): this is Timestamp { return this.type === Type.Timestamp; }
    isInterval(): this is Interval { return this.type === Type.Interval; }
    isList(): this is List { return this.type === Type.List; }
    isStruct(): this is Struct { return this.type === Type.Struct_; }
    isUnion(): this is Union { return this.type === Type.Union; }
    isFixedSizeBinary(): this is FixedSizeBinary { return this.type === Type.FixedSizeBinary; }
    isFixedSizeList(): this is FixedSizeList { return this.type === Type.FixedSizeList; }
    isMap(): this is Map_ { return this.type === Type.Map; }
}

export class Null extends FieldType {
    toString() { return `Null`; }
    constructor() {
        super(Type.Null);
    }
    accept(visitor: Visitor) {
        return visitor.visitNullFieldType(this);
    }
}

export class Int extends FieldType {
    toString() { return `Int isSigned[${this.isSigned}], bitWidth[${this.bitWidth}]`; }
    constructor(public isSigned: boolean, public bitWidth: IntBitWidth) {
        super(Type.Int);
    }
    accept(visitor: Visitor) {
        return visitor.visitIntFieldType(this);
    }
}

export class FloatingPoint extends FieldType {
    toString() { return `FloatingPoint precision`; }
    constructor(public precision: Precision) {
        super(Type.FloatingPoint);
    }
    accept(visitor: Visitor) {
        return visitor.visitFloatingPointFieldType(this);
    }
}

export class Binary extends FieldType {
    toString() { return `Binary`; }
    constructor() {
        super(Type.Binary);
    }
    accept(visitor: Visitor) {
        return visitor.visitBinaryFieldType(this);
    }
}

export class Utf8 extends FieldType {
    toString() { return `Utf8`; }
    constructor() {
        super(Type.Utf8);
    }
    accept(visitor: Visitor) {
        return visitor.visitUtf8FieldType(this);
    }
}

export class Bool extends FieldType {
    toString() { return `Bool`; }
    constructor() {
        super(Type.Bool);
    }
    accept(visitor: Visitor) {
        return visitor.visitBoolFieldType(this);
    }
}

export class Decimal extends FieldType {
    toString() { return `Decimal scale[${this.scale}], precision[${this.precision}]`; }
    constructor(public scale: number, public precision: number) {
        super(Type.Decimal);
    }
    accept(visitor: Visitor) {
        return visitor.visitDecimalFieldType(this);
    }
}

export class Date extends FieldType {
    toString() { return `Date unit[${this.unit}]`; }
    constructor(public unit: DateUnit) {
        super(Type.Date);
    }
    accept(visitor: Visitor) {
        return visitor.visitDateFieldType(this);
    }
}

export class Time extends FieldType {
    toString() { return `Time unit[${this.unit}], bitWidth[${this.bitWidth}]`; }
    constructor(public unit: TimeUnit, public bitWidth: TimeBitWidth) {
        super(Type.Time);
    }
    accept(visitor: Visitor) {
        return visitor.visitTimeFieldType(this);
    }
}

export class Timestamp extends FieldType {
    toString() { return `Timestamp unit[${this.unit}], timezone[${this.timezone}]`; }
    constructor(public unit: TimeUnit, public timezone?: string | null) {
        super(Type.Timestamp);
    }
    accept(visitor: Visitor) {
        return visitor.visitTimestampFieldType(this);
    }
}

export class Interval extends FieldType {
    toString() { return `Interval unit[${this.unit}]`; }
    constructor(public unit: IntervalUnit) {
        super(Type.Interval);
    }
    accept(visitor: Visitor) {
        return visitor.visitIntervalFieldType(this);
    }
}

export class List extends FieldType {
    toString() { return `List`; }
    constructor() {
        super(Type.List);
    }
    accept(visitor: Visitor) {
        return visitor.visitListFieldType(this);
    }
}

export class Struct extends FieldType {
    toString() { return `Struct`; }
    constructor() {
        super(Type.Struct_);
    }
    accept(visitor: Visitor) {
        return visitor.visitStructFieldType(this);
    }
}

export class Union extends FieldType {
    toString() { return `Union mode[${this.mode}], typeIds[${this.typeIds}]`; }
    constructor(public mode: UnionMode, public typeIds: Type[]) {
        super(Type.Union);
    }
    accept(visitor: Visitor) {
        return visitor.visitUnionFieldType(this);
    }
}

export class FixedSizeBinary extends FieldType {
    toString() { return `FixedSizeBinary byteWidth[${this.byteWidth}]`; }
    constructor(public byteWidth: number) {
        super(Type.FixedSizeBinary);
    }
    accept(visitor: Visitor) {
        return visitor.visitFixedSizeBinaryFieldType(this);
    }
}

export class FixedSizeList extends FieldType {
    toString() { return `FixedSizeList listSize[${this.listSize}]`; }
    constructor(public listSize: number) {
        super(Type.FixedSizeList);
    }
    accept(visitor: Visitor) {
        return visitor.visitFixedSizeListFieldType(this);
    }
}

export class Map_ extends FieldType {
    toString() { return `Map keysSorted[${this.keysSorted}]`; }
    constructor(public keysSorted: boolean) {
        super(Type.Map);
    }
    accept(visitor: Visitor) {
        return visitor.visitMapFieldType(this);
    }
}
