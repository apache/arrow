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

import { Table } from '../table';
import { MAGIC } from './message';
import { Vector } from '../vector';
import { Column } from '../column';
import { Schema, Field } from '../schema';
import { Chunked } from '../vector/chunked';
import { Message } from './metadata/message';
import * as metadata from './metadata/message';
import { DataType, Dictionary } from '../type';
import { FileBlock, Footer } from './metadata/file';
import { MessageHeader, MetadataVersion } from '../enum';
import { WritableSink, AsyncByteQueue } from '../io/stream';
import { VectorAssembler } from '../visitor/vectorassembler';
import { JSONTypeAssembler } from '../visitor/jsontypeassembler';
import { JSONVectorAssembler } from '../visitor/jsonvectorassembler';
import { ArrayBufferViewInput, toUint8Array } from '../util/buffer';
import { RecordBatch, _InternalEmptyPlaceholderRecordBatch } from '../recordbatch';
import { Writable, ReadableInterop, ReadableDOMStreamOptions } from '../io/interfaces';
import { isPromise, isAsyncIterable, isWritableDOMStream, isWritableNodeStream, isIterable } from '../util/compat';

export class RecordBatchWriter<T extends { [key: string]: DataType } = any> extends ReadableInterop<Uint8Array> implements Writable<RecordBatch<T>> {

    /** @nocollapse */
    // @ts-ignore
    public static throughNode(options?: import('stream').DuplexOptions & { autoDestroy: boolean }): import('stream').Duplex {
        throw new Error(`"throughNode" not available in this environment`);
    }
    /** @nocollapse */
    public static throughDOM<T extends { [key: string]: DataType }>(
        // @ts-ignore
        writableStrategy?: QueuingStrategy<RecordBatch<T>> & { autoDestroy: boolean },
        // @ts-ignore
        readableStrategy?: { highWaterMark?: number, size?: any }
    ): { writable: WritableStream<Table<T> | RecordBatch<T>>, readable: ReadableStream<Uint8Array> } {
        throw new Error(`"throughDOM" not available in this environment`);
    }

    constructor(options?: { autoDestroy: boolean }) {
        super();
        this._autoDestroy = options && (typeof options.autoDestroy === 'boolean') ? options.autoDestroy : true;
    }

    protected _position = 0;
    protected _started = false;
    protected _autoDestroy: boolean;
    // @ts-ignore
    protected _sink = new AsyncByteQueue();
    protected _schema: Schema | null = null;
    protected _dictionaryBlocks: FileBlock[] = [];
    protected _recordBatchBlocks: FileBlock[] = [];

    public toString(sync: true): string;
    public toString(sync?: false): Promise<string>;
    public toString(sync: any = false) {
        return this._sink.toString(sync) as Promise<string> | string;
    }
    public toUint8Array(sync: true): Uint8Array;
    public toUint8Array(sync?: false): Promise<Uint8Array>;
    public toUint8Array(sync: any = false) {
        return this._sink.toUint8Array(sync) as Promise<Uint8Array> | Uint8Array;
    }

    public writeAll(input: Table<T> | Iterable<RecordBatch<T>>): this;
    public writeAll(input: AsyncIterable<RecordBatch<T>>): Promise<this>;
    public writeAll(input: PromiseLike<AsyncIterable<RecordBatch<T>>>): Promise<this>;
    public writeAll(input: PromiseLike<Table<T> | Iterable<RecordBatch<T>>>): Promise<this>;
    public writeAll(input: PromiseLike<any> | Table<T> | Iterable<RecordBatch<T>> | AsyncIterable<RecordBatch<T>>) {
        if (isPromise<any>(input)) {
            return input.then((x) => this.writeAll(x));
        } else if (isAsyncIterable<RecordBatch<T>>(input)) {
            return writeAllAsync(this, input);
        }
        return writeAll(this, <any> input);
    }

    public get closed() { return this._sink.closed; }
    public [Symbol.asyncIterator]() { return this._sink[Symbol.asyncIterator](); }
    public toDOMStream(options?: ReadableDOMStreamOptions) { return this._sink.toDOMStream(options); }
    public toNodeStream(options?: import('stream').ReadableOptions) { return this._sink.toNodeStream(options); }

    public close() {
        return this.reset()._sink.close();
    }
    public abort(reason?: any) {
        return this.reset()._sink.abort(reason);
    }
    public finish() {
        this._autoDestroy ? this.close() : this.reset(this._sink, this._schema);
        return this;
    }
    public reset(sink: WritableSink<ArrayBufferViewInput> = this._sink, schema: Schema<T> | null = null) {

        if ((sink === this._sink) || (sink instanceof AsyncByteQueue)) {
            this._sink = sink as AsyncByteQueue;
        } else {
            this._sink = new AsyncByteQueue();
            if (sink && isWritableDOMStream(sink)) {
                this.toDOMStream({ type: 'bytes' }).pipeTo(sink);
            } else if (sink && isWritableNodeStream(sink)) {
                this.toNodeStream({ objectMode: false }).pipe(sink);
            }
        }

        if (this._started && this._schema) {
            this._writeFooter();
        }

        this._started = false;
        this._dictionaryBlocks = [];
        this._recordBatchBlocks = [];

        if (!schema || !(schema.compareTo(this._schema))) {
            if (schema === null) {
                this._position = 0;
                this._schema = null;
            } else {
                this._started = true;
                this._schema = schema;
                this._writeSchema(schema);
            }
        }

        return this;
    }

    public write(payload?: Table<T> | RecordBatch<T> | Iterable<RecordBatch<T>> | null) {

        let schema: Schema<T> | null = null;

        if (!this._sink) {
            throw new Error(`RecordBatchWriter is closed`);
        } else if (payload === null || payload === undefined) {
            return this.finish() && undefined;
        } else if (payload instanceof Table && !(schema = payload.schema)) {
            return this.finish() && undefined;
        } else if (payload instanceof RecordBatch && !(schema = payload.schema)) {
            return this.finish() && undefined;
        }

        if (schema && !schema.compareTo(this._schema)) {
            if (this._started && this._autoDestroy) {
                return this.close();
            }
            this.reset(this._sink, schema);
        }

        if (payload instanceof RecordBatch) {
            if (!(payload instanceof _InternalEmptyPlaceholderRecordBatch)) {
                this._writeRecordBatch(payload);
            }
        } else if (payload instanceof Table) {
            this.writeAll(payload.chunks);
        } else if (isIterable(payload)) {
            this.writeAll(payload);
        }
    }

    protected _writeMessage<T extends MessageHeader>(message: Message<T>, alignment = 8) {

        const a = alignment - 1;
        const buffer = Message.encode(message);
        const flatbufferSize = buffer.byteLength;
        const alignedSize = (flatbufferSize + 4 + a) & ~a;
        const nPaddingBytes = alignedSize - flatbufferSize - 4;

        if (message.headerType === MessageHeader.RecordBatch) {
            this._recordBatchBlocks.push(new FileBlock(alignedSize, message.bodyLength, this._position));
        } else if (message.headerType === MessageHeader.DictionaryBatch) {
            this._dictionaryBlocks.push(new FileBlock(alignedSize, message.bodyLength, this._position));
        }

        // Write the flatbuffer size prefix including padding
        this._write(Int32Array.of(alignedSize - 4));
        // Write the flatbuffer
        if (flatbufferSize > 0) { this._write(buffer); }
        // Write any padding
        return this._writePadding(nPaddingBytes);
    }

    protected _write(chunk: ArrayBufferViewInput) {
        if (this._started) {
            const buffer = toUint8Array(chunk);
            if (buffer && buffer.byteLength > 0) {
                this._sink.write(buffer);
                this._position += buffer.byteLength;
            }
        }
        return this;
    }

    protected _writeSchema(schema: Schema<T>) {
        return this
            ._writeMessage(Message.from(schema))
            ._writeDictionaries(schema.dictionaryFields);
    }

    protected _writeFooter() {
        return this._writePadding(4); // eos bytes
    }

    protected _writeMagic() {
        return this._write(MAGIC);
    }

    protected _writePadding(nBytes: number) {
        return nBytes > 0 ? this._write(new Uint8Array(nBytes)) : this;
    }

    protected _writeRecordBatch(records: RecordBatch<T>) {
        const { byteLength, nodes, bufferRegions, buffers } = VectorAssembler.assemble(records);
        const recordBatch = new metadata.RecordBatch(records.length, nodes, bufferRegions);
        const message = Message.from(recordBatch, byteLength);
        return this
            ._writeMessage(message)
            ._writeBodyBuffers(buffers);
    }

    protected _writeDictionaryBatch(dictionary: Vector, id: number, isDelta = false) {
        const { byteLength, nodes, bufferRegions, buffers } = VectorAssembler.assemble(dictionary);
        const recordBatch = new metadata.RecordBatch(dictionary.length, nodes, bufferRegions);
        const dictionaryBatch = new metadata.DictionaryBatch(recordBatch, id, isDelta);
        const message = Message.from(dictionaryBatch, byteLength);
        return this
            ._writeMessage(message)
            ._writeBodyBuffers(buffers);
    }

    protected _writeBodyBuffers(buffers: ArrayBufferView[]) {
        let buffer: ArrayBufferView;
        let size: number, padding: number;
        for (let i = -1, n = buffers.length; ++i < n;) {
            if ((buffer = buffers[i]) && (size = buffer.byteLength) > 0) {
                this._write(buffer);
                if ((padding = ((size + 7) & ~7) - size) > 0) {
                    this._writePadding(padding);
                }
            }
        }
        return this;
    }

    protected _writeDictionaries(dictionaryFields: Map<number, Field<Dictionary<any, any>>[]>) {
        for (const [id, fields] of dictionaryFields) {
            const vector = fields[0].type.dictionaryVector;
            if (!(vector instanceof Chunked)) {
                this._writeDictionaryBatch(vector, id, false);
            } else {
                const chunks = vector.chunks;
                for (let i = -1, n = chunks.length; ++i < n;) {
                    this._writeDictionaryBatch(chunks[i], id, i > 0);
                }
            }
        }
        return this;
    }
}

/** @ignore */
export class RecordBatchStreamWriter<T extends { [key: string]: DataType } = any> extends RecordBatchWriter<T> {
    public static writeAll<T extends { [key: string]: DataType } = any>(input: Table<T> | Iterable<RecordBatch<T>>, options?: { autoDestroy: true }): RecordBatchStreamWriter<T>;
    public static writeAll<T extends { [key: string]: DataType } = any>(input: AsyncIterable<RecordBatch<T>>, options?: { autoDestroy: true }): Promise<RecordBatchStreamWriter<T>>;
    public static writeAll<T extends { [key: string]: DataType } = any>(input: PromiseLike<AsyncIterable<RecordBatch<T>>>, options?: { autoDestroy: true }): Promise<RecordBatchStreamWriter<T>>;
    public static writeAll<T extends { [key: string]: DataType } = any>(input: PromiseLike<Table<T> | Iterable<RecordBatch<T>>>, options?: { autoDestroy: true }): Promise<RecordBatchStreamWriter<T>>;
    /** @nocollapse */
    public static writeAll<T extends { [key: string]: DataType } = any>(input: any, options?: { autoDestroy: true }) {
        const writer = new RecordBatchStreamWriter<T>(options);
        if (isPromise<any>(input)) {
            return input.then((x) => writer.writeAll(x));
        } else if (isAsyncIterable<RecordBatch<T>>(input)) {
            return writeAllAsync(writer, input);
        }
        return writeAll(writer, input);
    }
}

/** @ignore */
export class RecordBatchFileWriter<T extends { [key: string]: DataType } = any> extends RecordBatchWriter<T> {
    public static writeAll<T extends { [key: string]: DataType } = any>(input: Table<T> | Iterable<RecordBatch<T>>): RecordBatchFileWriter<T>;
    public static writeAll<T extends { [key: string]: DataType } = any>(input: AsyncIterable<RecordBatch<T>>): Promise<RecordBatchFileWriter<T>>;
    public static writeAll<T extends { [key: string]: DataType } = any>(input: PromiseLike<AsyncIterable<RecordBatch<T>>>): Promise<RecordBatchFileWriter<T>>;
    public static writeAll<T extends { [key: string]: DataType } = any>(input: PromiseLike<Table<T> | Iterable<RecordBatch<T>>>): Promise<RecordBatchFileWriter<T>>;
    /** @nocollapse */
    public static writeAll<T extends { [key: string]: DataType } = any>(input: any) {
        const writer = new RecordBatchFileWriter<T>();
        if (isPromise<any>(input)) {
            return input.then((x) => writer.writeAll(x));
        } else if (isAsyncIterable<RecordBatch<T>>(input)) {
            return writeAllAsync(writer, input);
        }
        return writeAll(writer, input);
    }

    constructor() {
        super();
        this._autoDestroy = true;
    }

    protected _writeSchema(schema: Schema<T>) {
        return this
            ._writeMagic()._writePadding(2)
            ._writeDictionaries(schema.dictionaryFields);
    }

    protected _writeFooter() {
        const buffer = Footer.encode(new Footer(
            this._schema!, MetadataVersion.V4,
            this._recordBatchBlocks, this._dictionaryBlocks
        ));
        return this
            ._write(buffer) // Write the flatbuffer
            ._write(Int32Array.of(buffer.byteLength)) // then the footer size suffix
            ._writeMagic(); // then the magic suffix
    }
}

/** @ignore */
export class RecordBatchJSONWriter<T extends { [key: string]: DataType } = any> extends RecordBatchWriter<T> {

    public static writeAll<T extends { [key: string]: DataType } = any>(this: typeof RecordBatchWriter, input: Table<T> | Iterable<RecordBatch<T>>): RecordBatchJSONWriter<T>;
    // @ts-ignore
    public static writeAll<T extends { [key: string]: DataType } = any>(this: typeof RecordBatchWriter, input: AsyncIterable<RecordBatch<T>>): Promise<RecordBatchJSONWriter<T>>;
    public static writeAll<T extends { [key: string]: DataType } = any>(this: typeof RecordBatchWriter, input: PromiseLike<AsyncIterable<RecordBatch<T>>>): Promise<RecordBatchJSONWriter<T>>;
    public static writeAll<T extends { [key: string]: DataType } = any>(this: typeof RecordBatchWriter, input: PromiseLike<Table<T> | Iterable<RecordBatch<T>>>): Promise<RecordBatchJSONWriter<T>>;
    /** @nocollapse */
    public static writeAll<T extends { [key: string]: DataType } = any>(this: typeof RecordBatchWriter, input: any) {
        return new RecordBatchJSONWriter<T>().writeAll(input as any);
    }

    constructor() {
        super();
        this._autoDestroy = true;
    }

    protected _writeMessage() { return this; }
    protected _writeSchema(schema: Schema<T>) {
        return this._write(`{\n  "schema": ${
            JSON.stringify({ fields: schema.fields.map(fieldToJSON) }, null, 2)
        }`)._writeDictionaries(schema.dictionaryFields);
    }
    protected _writeDictionaries(dictionaryFields: Map<number, Field<Dictionary<any, any>>[]>) {
        this._write(`,\n  "dictionaries": [\n`);
        super._writeDictionaries(dictionaryFields);
        return this._write(`\n  ]`);
    }
    protected _writeDictionaryBatch(dictionary: Vector, id: number, isDelta = false) {
        this._write(this._dictionaryBlocks.length === 0 ? `    ` : `,\n    `);
        this._write(`${dictionaryBatchToJSON(this._schema!, dictionary, id, isDelta)}`);
        this._dictionaryBlocks.push(new FileBlock(0, 0, 0));
        return this;
    }
    protected _writeRecordBatch(records: RecordBatch<T>) {
        this._write(this._recordBatchBlocks.length === 0
            ? `,\n  "batches": [\n    `
            : `,\n    `);
        this._write(`${recordBatchToJSON(records)}`);
        this._recordBatchBlocks.push(new FileBlock(0, 0, 0));
        return this;
    }
    public close() {
        if (this._recordBatchBlocks.length > 0) {
            this._write(`\n  ]`);
        }
        if (this._schema) {
            this._write(`\n}`);
        }
        return super.close();
    }
}

/** @ignore */
function writeAll<T extends { [key: string]: DataType } = any>(writer: RecordBatchWriter<T>, input: Table<T> | Iterable<RecordBatch<T>>) {
    let chunks = input as Iterable<RecordBatch<T>>;
    if (input instanceof Table) {
        chunks = input.chunks;
        writer.reset(undefined, input.schema);
    }
    for (const batch of chunks) {
        writer.write(batch);
    }
    return writer.finish();
}

/** @ignore */
async function writeAllAsync<T extends { [key: string]: DataType } = any>(writer: RecordBatchWriter<T>, batches: AsyncIterable<RecordBatch<T>>) {
    for await (const batch of batches) {
        writer.write(batch);
    }
    return writer.finish();
}

/** @ignore */
function fieldToJSON({ name, type, nullable }: Field): object {
    const assembler = new JSONTypeAssembler();
    return {
        'name': name, 'nullable': nullable,
        'type': assembler.visit(type),
        'children': (type.children || []).map(fieldToJSON),
        'dictionary': !DataType.isDictionary(type) ? undefined : {
            'id': type.id,
            'isOrdered': type.isOrdered,
            'indexType': assembler.visit(type.indices)
        }
    };
}

/** @ignore */
function dictionaryBatchToJSON(schema: Schema, dictionary: Vector, id: number, isDelta = false) {
    const f = schema.dictionaryFields.get(id)![0];
    const field = new Field(f.name, f.type.dictionary, f.nullable, f.metadata);
    const columns = JSONVectorAssembler.assemble(new Column(field, [dictionary]));
    return JSON.stringify({
        'id': id,
        'isDelta': isDelta,
        'data': {
            'count': dictionary.length,
            'columns': columns
        }
    }, null, 2);
}

/** @ignore */
function recordBatchToJSON(records: RecordBatch) {
    return JSON.stringify({
        'count': records.length,
        'columns': JSONVectorAssembler.assemble(records)
    }, null, 2);
}
