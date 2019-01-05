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

import { flatbuffers } from 'flatbuffers';
import { encodeUtf8 } from '../util/utf8';
import ByteBuffer = flatbuffers.ByteBuffer;
import { ArrayBufferViewConstructor } from '../interfaces';
import { isPromise, isIterable, isAsyncIterable, isIteratorResult } from './compat';

/** @ignore */
const SharedArrayBuf = (typeof SharedArrayBuffer !== 'undefined' ? SharedArrayBuffer : ArrayBuffer);

/** @ignore */
function collapseContiguousByteRanges(chunks: Uint8Array[]) {
    for (let x, y, i = 0; ++i < chunks.length;) {
        x = chunks[i - 1];
        y = chunks[i - 0];
        // continue x and y don't share the same underlying ArrayBuffer
        if (!x || !y || x.buffer !== y.buffer) { continue; }
        const { byteOffset: xOffset, byteLength: xLen } = x;
        const { byteOffset: yOffset, byteLength: yLen } = y;
        // continue if the byte ranges of x and y aren't contiguous
        if ((xOffset + xLen) < yOffset || (yOffset + yLen) < xOffset) { continue; }
        chunks.splice(--i, 2, new Uint8Array(x.buffer, xOffset, yOffset - xOffset + yLen));
    }
    return chunks;
}

/** @ignore */
export function memcpy<TTarget extends ArrayBufferView, TSource extends ArrayBufferView>(target: TTarget, source: TSource, targetByteOffset = 0, sourceByteLength = source.byteLength) {
    const targetByteLength = target.byteLength;
    const dst = new Uint8Array(target.buffer, target.byteOffset, targetByteLength);
    const src = new Uint8Array(source.buffer, source.byteOffset, Math.min(sourceByteLength, targetByteLength));
    dst.set(src, targetByteOffset);
    return target;
}

/** @ignore */
export function joinUint8Arrays(chunks: Uint8Array[], size?: number | null): [Uint8Array, Uint8Array[]] {
    // collapse chunks that share the same underlying ArrayBuffer and whose byte ranges overlap,
    // to avoid unnecessarily copying the bytes to do this buffer join. This is a common case during
    // streaming, where we may be reading partial byte ranges out of the same underlying ArrayBuffer
    chunks = collapseContiguousByteRanges(chunks);
    let offset = 0, index = -1, numChunks = chunks.length;
    let chunksLen = chunks.reduce((x, y) => x + y.byteLength, 0);
    let source: Uint8Array, sliced: Uint8Array, buffer: Uint8Array | void;
    let length = Math.min(chunksLen, typeof size === 'number' ? size : Infinity);
    while (++index < numChunks) {
        source = chunks[index];
        sliced = source.subarray(0, Math.min(source.length, length - offset));
        if (length <= (offset + sliced.length)) {
            if (sliced.length < source.length) {
                chunks[index] = source.subarray(sliced.length);
            } else if (sliced.length === source.length) { index++; }
            buffer ? memcpy(buffer, sliced, offset) : (buffer = sliced);
            break;
        }
        (buffer || (buffer = new Uint8Array(length))).set(sliced, offset);
        offset += sliced.length;
    }
    return [buffer || new Uint8Array(0), chunks.slice(index)];
}

/** @ignore */
export type ArrayBufferViewInput = ArrayBufferView | ArrayBufferLike | ArrayBufferView | Iterable<number> | ArrayLike<number> | ByteBuffer | string | null | undefined  |
                    IteratorResult<ArrayBufferView | ArrayBufferLike | ArrayBufferView | Iterable<number> | ArrayLike<number> | ByteBuffer | string | null | undefined> |
          ReadableStreamReadResult<ArrayBufferView | ArrayBufferLike | ArrayBufferView | Iterable<number> | ArrayLike<number> | ByteBuffer | string | null | undefined> ;

/** @ignore */
export function toArrayBufferView<T extends ArrayBufferView>(ArrayBufferViewCtor: ArrayBufferViewConstructor<T>, input: ArrayBufferViewInput): T {

    let value: any = isIteratorResult(input) ? input.value : input;

    if (!value) { return new ArrayBufferViewCtor(0); }
    if (typeof value === 'string') { value = encodeUtf8(value); }
    if (value instanceof ArrayBufferViewCtor) {
        return value.constructor === ArrayBufferViewCtor ? value :
            // Node's `Buffer` class passes the `instanceof Uint8Array` check, but we need
            // a real Uint8Array, since Buffer#slice isn't the same as Uint8Array#slice :/
            new ArrayBufferViewCtor(value.buffer, value.byteOffset, value.byteLength / ArrayBufferViewCtor.BYTES_PER_ELEMENT);
    }
    if (value instanceof ArrayBuffer) { return new ArrayBufferViewCtor(value); }
    if (value instanceof SharedArrayBuf) { return new ArrayBufferViewCtor(value); }
    if (value instanceof ByteBuffer) { return toArrayBufferView(ArrayBufferViewCtor, value.bytes()); }
    return !ArrayBuffer.isView(value) ? ArrayBufferViewCtor.from(value) : value.byteLength <= 0 ? new ArrayBufferViewCtor(0)
        : new ArrayBufferViewCtor(value.buffer, value.byteOffset, value.byteLength / ArrayBufferViewCtor.BYTES_PER_ELEMENT);
}

/** @ignore */ export const toInt8Array = (input: ArrayBufferViewInput) => toArrayBufferView(Int8Array, input);
/** @ignore */ export const toInt16Array = (input: ArrayBufferViewInput) => toArrayBufferView(Int16Array, input);
/** @ignore */ export const toInt32Array = (input: ArrayBufferViewInput) => toArrayBufferView(Int32Array, input);
/** @ignore */ export const toUint8Array = (input: ArrayBufferViewInput) => toArrayBufferView(Uint8Array, input);
/** @ignore */ export const toUint16Array = (input: ArrayBufferViewInput) => toArrayBufferView(Uint16Array, input);
/** @ignore */ export const toUint32Array = (input: ArrayBufferViewInput) => toArrayBufferView(Uint32Array, input);
/** @ignore */ export const toFloat32Array = (input: ArrayBufferViewInput) => toArrayBufferView(Float32Array, input);
/** @ignore */ export const toFloat64Array = (input: ArrayBufferViewInput) => toArrayBufferView(Float64Array, input);
/** @ignore */ export const toUint8ClampedArray = (input: ArrayBufferViewInput) => toArrayBufferView(Uint8ClampedArray, input);

/** @ignore */
type ArrayBufferViewIteratorInput = Iterable<ArrayBufferViewInput> | ArrayBufferViewInput;

/** @ignore */
const pump = <T extends Iterator<any> | AsyncIterator<any>>(iterator: T) => { iterator.next(); return iterator; };

/** @ignore */
export function* toArrayBufferViewIterator<T extends ArrayBufferView>(ArrayCtor: ArrayBufferViewConstructor<T>, source: ArrayBufferViewIteratorInput) {

    const wrap = function*<T>(x: T) { yield x; };
    const buffers: Iterable<ArrayBufferViewInput> =
                   (typeof source === 'string') ? wrap(source)
                 : (ArrayBuffer.isView(source)) ? wrap(source)
              : (source instanceof ArrayBuffer) ? wrap(source)
           : (source instanceof SharedArrayBuf) ? wrap(source)
    : !isIterable<ArrayBufferViewInput>(source) ? wrap(source) : source;

    yield* pump((function* (it) {
        let r: IteratorResult<any> = <any> null;
        do {
            r = it.next(yield toArrayBufferView(ArrayCtor, r));
        } while (!r.done);
    })(buffers[Symbol.iterator]()));
}

/** @ignore */ export const toInt8ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Int8Array, input);
/** @ignore */ export const toInt16ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Int16Array, input);
/** @ignore */ export const toInt32ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Int32Array, input);
/** @ignore */ export const toUint8ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Uint8Array, input);
/** @ignore */ export const toUint16ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Uint16Array, input);
/** @ignore */ export const toUint32ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Uint32Array, input);
/** @ignore */ export const toFloat32ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Float32Array, input);
/** @ignore */ export const toFloat64ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Float64Array, input);
/** @ignore */ export const toUint8ClampedArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Uint8ClampedArray, input);

/** @ignore */
type ArrayBufferViewAsyncIteratorInput = AsyncIterable<ArrayBufferViewInput> | Iterable<ArrayBufferViewInput> | PromiseLike<ArrayBufferViewInput> | ArrayBufferViewInput;

/** @ignore */
export async function* toArrayBufferViewAsyncIterator<T extends ArrayBufferView>(ArrayCtor: ArrayBufferViewConstructor<T>, source: ArrayBufferViewAsyncIteratorInput): AsyncIterableIterator<T> {

    // if a Promise, unwrap the Promise and iterate the resolved value
    if (isPromise<ArrayBufferViewInput>(source)) {
        return yield* toArrayBufferViewAsyncIterator(ArrayCtor, await source);
    }

    const wrap = async function*<T>(x: T) { yield await x; };
    const emit = async function* <T extends Iterable<any>>(source: T) {
        yield* pump((function*(it: Iterator<any>) {
            let r: IteratorResult<any> = <any> null;
            do {
                r = it.next(yield r && r.value);
            } while (!r.done);
        })(source[Symbol.iterator]()));
    };

    const buffers: AsyncIterable<ArrayBufferViewInput> =
                        (typeof source === 'string') ? wrap(source) // if string, wrap in an AsyncIterableIterator
                      : (ArrayBuffer.isView(source)) ? wrap(source) // if TypedArray, wrap in an AsyncIterableIterator
                   : (source instanceof ArrayBuffer) ? wrap(source) // if ArrayBuffer, wrap in an AsyncIterableIterator
                : (source instanceof SharedArrayBuf) ? wrap(source) // if SharedArrayBuffer, wrap in an AsyncIterableIterator
          : isIterable<ArrayBufferViewInput>(source) ? emit(source) // If Iterable, wrap in an AsyncIterableIterator and compose the `next` values
    : !isAsyncIterable<ArrayBufferViewInput>(source) ? wrap(source) // If not an AsyncIterable, treat as a sentinel and wrap in an AsyncIterableIterator
                                                     : source; // otherwise if AsyncIterable, use it

    yield* pump((async function* (it) {
        let r: IteratorResult<any> = <any> null;
        do {
            r = await it.next(yield toArrayBufferView(ArrayCtor, r));
        } while (!r.done);
    })(buffers[Symbol.asyncIterator]()));
}

/** @ignore */ export const toInt8ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Int8Array, input);
/** @ignore */ export const toInt16ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Int16Array, input);
/** @ignore */ export const toInt32ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Int32Array, input);
/** @ignore */ export const toUint8ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Uint8Array, input);
/** @ignore */ export const toUint16ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Uint16Array, input);
/** @ignore */ export const toUint32ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Uint32Array, input);
/** @ignore */ export const toFloat32ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Float32Array, input);
/** @ignore */ export const toFloat64ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Float64Array, input);
/** @ignore */ export const toUint8ClampedArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Uint8ClampedArray, input);

/** @ignore */
export function rebaseValueOffsets(offset: number, length: number, valueOffsets: Int32Array) {
    // If we have a non-zero offset, create a new offsets array with the values
    // shifted by the start offset, such that the new start offset is 0
    if (offset !== 0) {
        valueOffsets = valueOffsets.slice(0, length + 1);
        for (let i = -1; ++i <= length;) {
            valueOffsets[i] += offset;
        }
    }
    return valueOffsets;
}
