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

import { align } from '../util/bit';
import { Schema, Long, MessageHeader, MetadataVersion } from '../type';

export class Footer {
    constructor(public dictionaryBatches: FileBlock[], public recordBatches: FileBlock[], public schema: Schema) {}
}

export class FileBlock {
    constructor(public metaDataLength: number, public bodyLength: Long, public offset: Long) {}
}

export class Message {
    public bodyLength: number;
    public version: MetadataVersion;
    public headerType: MessageHeader;
    constructor(version: MetadataVersion, bodyLength: Long | number, headerType: MessageHeader) {
        this.version = version;
        this.headerType = headerType;
        this.bodyLength = typeof bodyLength === 'number' ? bodyLength : bodyLength.low;
    }
    static isSchema(m: Message): m is Schema { return m.headerType === MessageHeader.Schema; }
    static isRecordBatch(m: Message): m is RecordBatchMetadata { return m.headerType === MessageHeader.RecordBatch; }
    static isDictionaryBatch(m: Message): m is DictionaryBatch { return m.headerType === MessageHeader.DictionaryBatch; }
}

export class RecordBatchMetadata extends Message {
    public length: number;
    public nodes: FieldMetadata[];
    public buffers: BufferMetadata[];
    constructor(version: MetadataVersion, length: Long | number, nodes: FieldMetadata[], buffers: BufferMetadata[]) {
        super(version, buffers.reduce((s, b) => align(s + b.length + (b.offset - s), 8), 0), MessageHeader.RecordBatch);
        this.nodes = nodes;
        this.buffers = buffers;
        this.length = typeof length === 'number' ? length : length.low;
    }
}

export class DictionaryBatch extends Message {
    public id: number;
    public isDelta: boolean;
    public data: RecordBatchMetadata;
    constructor(version: MetadataVersion, data: RecordBatchMetadata, id: Long | number, isDelta: boolean = false) {
        super(version, data.bodyLength, MessageHeader.DictionaryBatch);
        this.isDelta = isDelta;
        this.data = data;
        this.id = typeof id === 'number' ? id : id.low;
    }
    private static atomicDictionaryId = 0;
    public static getId() { return DictionaryBatch.atomicDictionaryId++; }
    public get nodes(): FieldMetadata[] { return this.data.nodes; }
    public get buffers(): BufferMetadata[] { return this.data.buffers; }
}

export class BufferMetadata {
    public offset: number;
    public length: number;
    constructor(offset: Long | number, length: Long | number) {
        this.offset = typeof offset === 'number' ? offset : offset.low;
        this.length = typeof length === 'number' ? length : length.low;
    }
}

export class FieldMetadata {
    public length: number;
    public nullCount: number;
    constructor(length: Long | number, nullCount: Long | number) {
        this.length = typeof length === 'number' ? length : length.low;
        this.nullCount = typeof nullCount === 'number' ? nullCount : nullCount.low;
    }
}
