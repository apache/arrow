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

import { Vector } from '../../vector';
import { RecordBatch } from '../../recordbatch';
import { TypeVisitor } from '../../visitor';
import { FlatType, NestedType, ListType } from '../../type';
import { Message, FieldMetadata, BufferMetadata } from '../metadata';
import { FlatData, ListData, NestedData, SingleNestedData, DenseUnionData, SparseUnionData, BoolData, FlatListData, DictionaryData } from '../../data';
import {
    Schema, Field,
    Dictionary,
    Null, Int, Float,
    Binary, Bool, Utf8, Decimal,
    Date_, Time, Timestamp, Interval,
    List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
    UnionMode, SparseUnion, DenseUnion, FlatListType, DataType,
} from '../../type';

export function* readRecordBatches(messages: Iterable<{ schema: Schema, message: Message, loader: TypeDataLoader }>) {
    for (const { schema, message, loader } of messages) {
        yield* readRecordBatch(schema, message, loader);
    }
}

export async function* readRecordBatchesAsync(messages: AsyncIterable<{ schema: Schema, message: Message, loader: TypeDataLoader }>) {
    for await (const { schema, message, loader } of messages) {
        yield* readRecordBatch(schema, message, loader);
    }
}

export function* readRecordBatch(schema: Schema, message: Message, loader: TypeDataLoader) {
    if (Message.isRecordBatch(message)) {
        yield new RecordBatch(schema, message.length, loader.visitFields(schema.fields));
    } else if (Message.isDictionaryBatch(message)) {
        const dictionaryId = message.id;
        const dictionaries = loader.dictionaries;
        const dictionaryField = schema.dictionaries.get(dictionaryId)!;
        const dictionaryDataType = (dictionaryField.type as Dictionary).dictionary;
        let dictionaryVector = Vector.create(loader.visit(dictionaryDataType));
        if (message.isDelta && dictionaries.has(dictionaryId)) {
            dictionaryVector = dictionaries.get(dictionaryId)!.concat(dictionaryVector);
        }
        dictionaries.set(dictionaryId, dictionaryVector);
    }
}

export abstract class TypeDataLoader extends TypeVisitor {

    public dictionaries: Map<number, Vector>;
    protected nodes: Iterator<FieldMetadata>;
    protected buffers: Iterator<BufferMetadata>;

    constructor(nodes: Iterator<FieldMetadata>, buffers: Iterator<BufferMetadata>, dictionaries: Map<number, Vector>) {
        super();
        this.nodes = nodes;
        this.buffers = buffers;
        this.dictionaries = dictionaries;
    }

    public visitFields(fields: Field[]) { return fields.map((field) => this.visit(field.type)); }

    public visitNull           (type: Null)            { return this.visitNullType(type);   }
    public visitInt            (type: Int)             { return this.visitFlatType(type);   }
    public visitFloat          (type: Float)           { return this.visitFlatType(type);   }
    public visitBinary         (type: Binary)          { return this.visitFlatList(type);   }
    public visitUtf8           (type: Utf8)            { return this.visitFlatList(type);   }
    public visitBool           (type: Bool)            { return this.visitBoolType(type);   }
    public visitDecimal        (type: Decimal)         { return this.visitFlatType(type);   }
    public visitDate           (type: Date_)           { return this.visitFlatType(type);   }
    public visitTime           (type: Time)            { return this.visitFlatType(type);   }
    public visitTimestamp      (type: Timestamp)       { return this.visitFlatType(type);   }
    public visitInterval       (type: Interval)        { return this.visitFlatType(type);   }
    public visitList           (type: List)            { return this.visitListType(type);   }
    public visitStruct         (type: Struct)          { return this.visitNestedType(type); }
    public visitUnion          (type: Union)           { return this.visitUnionType(type);  }
    public visitFixedSizeBinary(type: FixedSizeBinary) { return this.visitFlatType(type);   }
    public visitFixedSizeList  (type: FixedSizeList)   { return this.visitFixedSizeListType(type); }
    public visitMap            (type: Map_)            { return this.visitNestedType(type); }
    public visitDictionary     (type: Dictionary)      {
        return new DictionaryData(type, this.dictionaries.get(type.id)!, this.visit(type.indicies));
    }
    protected getFieldMetadata() { return this.nodes.next().value; }
    protected getBufferMetadata() { return this.buffers.next().value; }
    protected readNullBitmap<T extends DataType>(type: T, nullCount: number, buffer = this.getBufferMetadata()) {
        return nullCount > 0 && this.readData(type, buffer) || new Uint8Array(0);
    }
    protected abstract readData<T extends DataType>(type: T, buffer?: BufferMetadata): any;
    protected abstract readOffsets<T extends DataType>(type: T, buffer?: BufferMetadata): any;
    protected abstract readTypeIds<T extends DataType>(type: T, buffer?: BufferMetadata): any;
    protected visitNullType(type: Null, { length, nullCount }: FieldMetadata = this.getFieldMetadata()) {
        return new FlatData<any>(type, length, this.readNullBitmap(type, nullCount), new Uint8Array(0), 0, nullCount);
    }
    protected visitFlatType<T extends FlatType>(type: T, { length, nullCount }: FieldMetadata = this.getFieldMetadata()) {
        return new FlatData<T>(type, length, this.readNullBitmap(type, nullCount), this.readData(type), 0, nullCount);
    }
    protected visitBoolType(type: Bool, { length, nullCount }: FieldMetadata = this.getFieldMetadata(), data?: Uint8Array) {
        return new BoolData(type, length, this.readNullBitmap(type, nullCount), data || this.readData(type), 0, nullCount);
    }
    protected visitFlatList<T extends FlatListType>(type: T, { length, nullCount }: FieldMetadata = this.getFieldMetadata()) {
        return new FlatListData<T>(type, length, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readData(type), 0, nullCount);
    }
    protected visitListType<T extends ListType>(type: T, { length, nullCount }: FieldMetadata = this.getFieldMetadata()) {
        return new ListData<T>(type, length, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.visit(type.children![0].type), 0, nullCount);
    }
    protected visitFixedSizeListType<T extends FixedSizeList>(type: T, { length, nullCount }: FieldMetadata = this.getFieldMetadata()) {
        return new SingleNestedData<T>(type, length, this.readNullBitmap(type, nullCount), this.visit(type.children![0].type), 0, nullCount);
    }
    protected visitNestedType<T extends NestedType>(type: T, { length, nullCount }: FieldMetadata = this.getFieldMetadata()) {
        return new NestedData<T>(type, length, this.readNullBitmap(type, nullCount), this.visitFields(type.children), 0, nullCount);
    }
    protected visitUnionType(type: DenseUnion | SparseUnion, { length, nullCount }: FieldMetadata = this.getFieldMetadata()) {
        return type.mode === UnionMode.Sparse ?
            new SparseUnionData(type as SparseUnion, length, this.readNullBitmap(type, nullCount), this.readTypeIds(type), this.visitFields(type.children), 0, nullCount) :
            new DenseUnionData(type as DenseUnion, length, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readTypeIds(type), this.visitFields(type.children), 0, nullCount);
    }
}
