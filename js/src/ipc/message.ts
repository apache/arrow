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

import { align } from '../util/bit';
import * as Schema_ from '../fb/Schema';
import * as Message_ from '../fb/Message';
import { flatbuffers } from 'flatbuffers';
import { DataType, Int, Dictionary } from '../type';
import { MessageVisitor, VisitorNode } from '../visitor';

export import Long = flatbuffers.Long;
export import Endianness = Schema_.org.apache.arrow.flatbuf.Endianness;
export import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;
export import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;

export class Footer implements Partial<VisitorNode> {
    constructor(public dictionaryBatches: Block[], public recordBatches: Block[], public schema: Schema) {}
    acceptMessageVisitor(visitor: MessageVisitor): any {
        return visitor.visitFooter(this);
    }
}

export class Block implements Partial<VisitorNode> {
    public readonly offset: number;
    public readonly offsetLong: Long;
    public readonly bodyLength: number;
    public readonly bodyLengthLong: Long;
    constructor(offset: Long | number, public metaDataLength: number, bodyLength: Long | number) {
        this.offset = (this.offsetLong = typeof offset === 'number' ? new Long(offset, 0) : offset).low;
        this.bodyLength = (this.bodyLengthLong = typeof bodyLength === 'number' ? new Long(bodyLength, 0) : bodyLength).low;
    }
    acceptMessageVisitor(visitor: MessageVisitor): any {
        return visitor.visitBlock(this);
    }
}

export class Message implements Partial<VisitorNode> {
    public readonly bodyLength: number;
    public readonly bodyLengthLong: Long;
    constructor(public version: MetadataVersion, public headerType: MessageHeader, bodyLength: Long | number) {
        this.bodyLength = (this.bodyLengthLong = typeof bodyLength === 'number' ? new Long(bodyLength, 0) : bodyLength).low;
    }
    acceptMessageVisitor(visitor: MessageVisitor): any {
        return visitor.visitMessage(this);
    }
    static isSchema(x: Message): x is Schema { return x.headerType === MessageHeader.Schema; }
    static isRecordBatch(x: Message): x is RecordBatch { return x.headerType === MessageHeader.RecordBatch; }
    static isDictionaryBatch(x: Message): x is DictionaryBatch { return x.headerType === MessageHeader.DictionaryBatch; }
}

export class Schema extends Message {
    public dictionaries: Map<string, Field>;
    constructor(version: MetadataVersion, public fields: Field[], public customMetadata?: Map<string, string>, public endianness = Endianness.Little) {
        super(version, MessageHeader.Schema, Long.ZERO);
        this.dictionaries = fields.reduce(function flattenDictionaryFields(dictionaries, f): Map<string, Field> {
            if (f.dictionary) {
                const id = f.dictionary.id.toString();
                if (dictionaries.has(id)) {
                    dictionaries.set(id, f);
                }
            }
            return (f.type.children || []).reduce(flattenDictionaryFields, dictionaries);
        }, new Map<string, Field>());
    }
}

export class RecordBatch extends Message {
    public readonly length: number;
    public readonly lengthLong: Long;
    constructor(version: MetadataVersion, length: Long | number, public fieldNodes: FieldNode[], public buffers: Buffer[]) {
        super(version, MessageHeader.RecordBatch, buffers.reduce((s, b) => align(s + b.length + (b.offset - s), 8), 0));
        this.length = (this.lengthLong = typeof length === 'number' ? new Long(length, 0) : length).low;
    }
    acceptMessageVisitor(visitor: MessageVisitor): any {
        return visitor.visitRecordBatch(this);
    }
}

export class DictionaryBatch extends Message {
    public readonly dictionaryId: number;
    public readonly dictionaryIdLong: Long;
    constructor(version: MetadataVersion, public dictionary: RecordBatch, dictionaryId: Long | number, public isDelta: boolean) {
        super(version, MessageHeader.DictionaryBatch, dictionary.bodyLength);
        this.dictionaryId = (this.dictionaryIdLong = typeof dictionaryId === 'number' ? new Long(dictionaryId, 0) : dictionaryId).low;
    }
    public get fieldNodes(): FieldNode[] { return this.dictionary.fieldNodes; }
    public get buffers(): Buffer[] { return this.dictionary.buffers; }
    public acceptMessageVisitor(visitor: MessageVisitor): any {
        return visitor.visitDictionaryBatch(this);
    }
    static atomicDictionaryId = 0;
}

export class Field<T extends DataType = DataType> implements Partial<VisitorNode> {
    constructor(public name: string,
                public type: T,
                public nullable = false,
                public metadata?: Map<string, string> | null,
                public dictionary?: Dictionary | null) {
    }
    get typeId(): T['TType'] { return this.type.TType; }
    get [Symbol.toStringTag](): string { return 'Field'; }
    get keys(): Field<T> | Field<Int<any>> {
        return !this.dictionary ? this : new Field<Int<any>>(
            this.name, this.dictionary.indicies.type,
            this.nullable, this.metadata, this.dictionary
        );
    }
    acceptMessageVisitor(visitor: MessageVisitor): any {
        return visitor.visitField(this);
    }
    toString() {
        return this[Symbol.toStringTag] +
            ` name[${this.name}]` +
            `, nullable[${this.nullable}]` +
            `, type[${this.type.toString()}]`;
    }
}

export class Buffer implements Partial<VisitorNode> {
    public readonly offset: number;
    public readonly length: number;
    constructor(offset: Long | number, length: Long | number) {
        this.offset = typeof offset === 'number' ? offset : offset.low;
        this.length = typeof length === 'number' ? length : length.low;
    }
    acceptMessageVisitor(visitor: MessageVisitor): any {
        return visitor.visitBuffer(this);
    }
}

export class FieldNode implements Partial<VisitorNode> {
    public readonly length: number;
    public readonly nullCount: number;
    constructor(length: Long | number, nullCount: Long | number) {
        this.length = typeof length === 'number' ? length : length.low;
        this.nullCount = typeof nullCount === 'number' ? nullCount : nullCount.low;
    }
    acceptMessageVisitor(visitor: MessageVisitor): any {
        return visitor.visitFieldNode(this);
    }
}
