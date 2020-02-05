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

import '../../jest-extensions';

import {
    Table,
    RecordBatchWriter,
    RecordBatchFileWriter,
    RecordBatchJSONWriter,
    RecordBatchStreamWriter,
} from '../../Arrow';

import * as fs from 'fs';
import { fs as memfs } from 'memfs';
import { Readable, PassThrough } from 'stream';

/* tslint:disable */
const randomatic = require('randomatic');

export abstract class ArrowIOTestHelper {

    constructor(public table: Table) {}

    public static file(table: Table) { return new ArrowFileIOTestHelper(table); }
    public static json(table: Table) { return new ArrowJsonIOTestHelper(table); }
    public static stream(table: Table) { return new ArrowStreamIOTestHelper(table); }

    protected abstract writer(table: Table): RecordBatchWriter;
    protected async filepath(table: Table): Promise<fs.PathLike> {
        const path = `/${randomatic('a0', 20)}.arrow`;
        const data = await this.writer(table).toUint8Array();
        await memfs.promises.writeFile(path, data);
        return path;
    }

    buffer(testFn: (buffer: Uint8Array) => void | Promise<void>) {
        return async () => {
            expect.hasAssertions();
            await testFn(await this.writer(this.table).toUint8Array());
        };
    }
    iterable(testFn: (iterable: Iterable<Uint8Array>) => void | Promise<void>) {
        return async () => {
            expect.hasAssertions();
            await testFn(chunkedIterable(await this.writer(this.table).toUint8Array()));
        };
    }
    asyncIterable(testFn: (asyncIterable: AsyncIterable<Uint8Array>) => void | Promise<void>) {
        return async () => {
            expect.hasAssertions();
            await testFn(asyncChunkedIterable(await this.writer(this.table).toUint8Array()));
        };
    }
    fsFileHandle(testFn: (handle: fs.promises.FileHandle) => void | Promise<void>) {
        return async () => {
            expect.hasAssertions();
            const path = await this.filepath(this.table);
            await testFn(<any> await memfs.promises.open(path, 'r'));
            await memfs.promises.unlink(path);
        };
    }
    fsReadableStream(testFn: (stream: fs.ReadStream) => void | Promise<void>) {
        return async () => {
            expect.hasAssertions();
            const path = await this.filepath(this.table);
            await testFn(<any> memfs.createReadStream(path));
            await memfs.promises.unlink(path);
        };
    }
    nodeReadableStream(testFn: (stream: NodeJS.ReadableStream) => void | Promise<void>) {
        return async () => {
            expect.hasAssertions();
            const sink = new PassThrough();
            sink.end(await this.writer(this.table).toUint8Array());
            await testFn(sink);
        };
    }
    whatwgReadableStream(testFn: (stream: ReadableStream) => void | Promise<void>) {
        return async () => {
            expect.hasAssertions();
            const path = await this.filepath(this.table);
            await testFn(nodeToDOMStream(memfs.createReadStream(path)));
            await memfs.promises.unlink(path);
        };
    }
    whatwgReadableByteStream(testFn: (stream: ReadableStream) => void | Promise<void>) {
        return async () => {
            expect.hasAssertions();
            const path = await this.filepath(this.table);
            await testFn(nodeToDOMStream(memfs.createReadStream(path), { type: 'bytes' }));
            await memfs.promises.unlink(path);
        };
    }
}

class ArrowFileIOTestHelper extends ArrowIOTestHelper {
    constructor(table: Table) { super(table); }
    protected writer(table: Table) {
        return RecordBatchFileWriter.writeAll(table);
    }
}

class ArrowJsonIOTestHelper extends ArrowIOTestHelper {
    constructor(table: Table) { super(table); }
    protected writer(table: Table) {
        return RecordBatchJSONWriter.writeAll(table);
    }
}

class ArrowStreamIOTestHelper extends ArrowIOTestHelper {
    constructor(table: Table) { super(table); }
    protected writer(table: Table) {
        return RecordBatchStreamWriter.writeAll(table);
    }
}

export function* chunkedIterable(buffer: Uint8Array) {
    let offset = 0, size = 0;
    while (offset < buffer.byteLength) {
        size = yield buffer.subarray(offset, offset +=
            (isNaN(+size) ? buffer.byteLength - offset : size));
    }
}

export async function* asyncChunkedIterable(buffer: Uint8Array) {
    let offset = 0, size = 0;
    while (offset < buffer.byteLength) {
        size = yield buffer.subarray(offset, offset +=
            (isNaN(+size) ? buffer.byteLength - offset : size));
    }
}

export async function concatBuffersAsync(iterator: AsyncIterable<Uint8Array> | ReadableStream) {
    if (iterator instanceof ReadableStream) {
        iterator = readableDOMStreamToAsyncIterator(iterator);
    }
    let chunks = [], total = 0;
    for await (const chunk of iterator) {
        chunks.push(chunk);
        total += chunk.byteLength;
    }
    return chunks.reduce((x, buffer) => {
        x.buffer.set(buffer, x.offset);
        x.offset += buffer.byteLength;
        return x;
    }, { offset: 0, buffer: new Uint8Array(total) }).buffer;
}

export async function* readableDOMStreamToAsyncIterator<T>(stream: ReadableStream<T>) {
    // Get a lock on the stream
    const reader = stream.getReader();
    try {
        while (true) {
            // Read from the stream
            const { done, value } = await reader.read();
            // Exit if we're done
            if (done) { break; }
            // Else yield the chunk
            yield value as T;
        }
    } catch (e) {
        throw e;
    } finally {
        try { stream.locked && reader.releaseLock(); } catch (e) {}
    }
}

export function nodeToDOMStream<T = any>(stream: NodeJS.ReadableStream, opts: any = {}) {
    stream = new Readable((stream as any)._readableState).wrap(stream);
    return new ReadableStream<T>({
        ...opts,
        start(controller) {
            stream.pause();
            stream.on('data', (chunk) => {
                controller.enqueue(chunk);
                stream.pause();
            });
            stream.on('end', () => controller.close());
            stream.on('error', e => controller.error(e));
        },
        pull() { stream.resume(); },
        cancel(reason) {
            stream.pause();
            if (typeof (stream as any).cancel === 'function') {
                return (stream as any).cancel(reason);
            } else if (typeof (stream as any).destroy === 'function') {
                return (stream as any).destroy(reason);
            }
        }
    });
}
