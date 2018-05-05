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

import { align } from './bit';
import { TextEncoder } from 'text-encoding-utf-8';
import { TypedArrayConstructor, TypedArray } from '../type';

export type NullableLayout = { nullCount: number, validity: Uint8Array };
export type BufferLayout<TArray = ArrayLike<number>> = { data: TArray };
export type DictionaryLayout<TArray = ArrayLike<number>> = { data: TArray, keys: number[] };
export type VariableWidthLayout<TArray = ArrayLike<number>> = { data: TArray, offsets: number[] };
export type VariableWidthDictionaryLayout<TArray = ArrayLike<number>> = { data: TArray, keys: number[], offsets: number[] };

export type values<T, TNull> = ArrayLike<T | TNull | null | undefined>;
export type BufferValueWriter<T> = (src: ArrayLike<T>, dst: number[], index: number) => boolean | void;
export type BufferWriter<T, TNull> = (values: values<T, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout;
export type BufferLayoutWriter<T, TNull> = (write: BufferValueWriter<T>, values: values<T, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout;

const writeNumeric64Value = writeFixedWidthValue.bind(null, 64);
const writeNumeric128Value = writeFixedWidthValue.bind(null, 128);
const utf8Encoder = new TextEncoder() as { encode: (s: string) => Uint8Array };

const stride1Encode = writeValidityLayout.bind(null, writeFixedWidthLayoutWithStride.bind(null, 1));
const stride1FixedWidth = writeFixedWidthLayout.bind(null, writeValidityLayout.bind(null, stride1Encode));
const stride2FixedWidth = writeFixedWidthLayout.bind(null, writeValidityLayout.bind(null, writeFixedWidthLayoutWithStride.bind(null, 2)));
const stride4FixedWidth = writeFixedWidthLayout.bind(null, writeValidityLayout.bind(null, writeFixedWidthLayoutWithStride.bind(null, 4)));

export const writeBools = writeTypedLayout.bind(null, stride1FixedWidth.bind(null, writeBooleanValue), Uint8Array)                                                as <TNull>(values: values<boolean | number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Uint8Array>;
export const writeInt8s = writeTypedLayout.bind(null, stride1FixedWidth.bind(null, writeNumericValue), Int8Array)                                                 as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Int8Array>;
export const writeInt16s = writeTypedLayout.bind(null, stride1FixedWidth.bind(null, writeNumericValue), Int16Array)                                               as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Int16Array>;
export const writeInt32s = writeTypedLayout.bind(null, stride1FixedWidth.bind(null, writeNumericValue), Int32Array)                                               as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Int32Array>;
export const writeInt64s = writeTypedLayout.bind(null, stride2FixedWidth.bind(null, writeNumeric64Value), Int32Array)                                             as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Int32Array>;
export const writeUint8s = writeTypedLayout.bind(null, stride1FixedWidth.bind(null, writeNumericValue), Uint8Array)                                               as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Uint8Array>;
export const writeUint16s = writeTypedLayout.bind(null, stride1FixedWidth.bind(null, writeNumericValue), Uint16Array)                                             as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Uint16Array>;
export const writeUint32s = writeTypedLayout.bind(null, stride1FixedWidth.bind(null, writeNumericValue), Uint32Array)                                             as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Uint32Array>;
export const writeUint64s = writeTypedLayout.bind(null, stride2FixedWidth.bind(null, writeNumeric64Value), Uint32Array)                                           as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Uint32Array>;
export const writeDecimals = writeTypedLayout.bind(null, stride4FixedWidth.bind(null, writeNumeric128Value), Uint32Array)                                         as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Uint32Array>;
export const writeFloat32s = writeTypedLayout.bind(null, stride1FixedWidth.bind(null, writeNumericValue), Float32Array)                                           as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Float32Array>;
export const writeFloat64s = writeTypedLayout.bind(null, stride1FixedWidth.bind(null, writeNumericValue), Float64Array)                                           as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => BufferLayout<Float64Array>;
export const writeVariableWidth = writeVariableWidthLayout.bind(null, stride1Encode)                                                                              as <T, TNull>(writeValue: BufferValueWriter<T>, values: values<T, TNull>, nulls?: ArrayLike<TNull>) => VariableWidthLayout<Uint8Array>;
export const writeBinary = writeTypedLayout.bind(null, writeVariableWidth.bind(null, writeBinaryValue))                                                           as <TNull>(values: values<Iterable<number>, TNull>, nulls?: ArrayLike<TNull>) => VariableWidthLayout<Uint8Array>;
export const writeUtf8s = writeTypedLayout.bind(null, writeVariableWidth.bind(null, writeUtf8Value), Uint8Array)                                                  as <TNull>(values: values<string, TNull>, nulls?: ArrayLike<TNull>) => VariableWidthLayout<Uint8Array>;
export const writeDictionaryEncoded = writeDictionaryLayout.bind(null, stride1Encode)                                                                             as <T, TNull>(writeValue: BufferValueWriter<T>, values: values<T, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Uint8Array>;
export const writeDictionaryEncodedBools = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride1FixedWidth, writeBooleanValue), Uint8Array)        as <TNull>(values: values<boolean | number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Uint8Array>;
export const writeDictionaryEncodedInt8s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride1FixedWidth, writeNumericValue), Int8Array)         as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Int8Array>;
export const writeDictionaryEncodedInt16s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride1FixedWidth, writeNumericValue), Int16Array)       as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Int16Array>;
export const writeDictionaryEncodedInt32s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride1FixedWidth, writeNumericValue), Int32Array)       as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Int32Array>;
export const writeDictionaryEncodedInt64s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride2FixedWidth, writeNumeric64Value), Int32Array)     as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Int32Array>;
export const writeDictionaryEncodedUint8s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride1FixedWidth, writeNumericValue), Uint8Array)       as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Uint8Array>;
export const writeDictionaryEncodedUint16s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride1FixedWidth, writeNumericValue), Uint16Array)     as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Uint16Array>;
export const writeDictionaryEncodedUint32s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride1FixedWidth, writeNumericValue), Uint32Array)     as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Uint32Array>;
export const writeDictionaryEncodedUint64s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride2FixedWidth, writeNumeric64Value), Uint32Array)   as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Uint32Array>;
export const writeDictionaryEncodedDecimals = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride4FixedWidth, writeNumeric128Value), Uint32Array) as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Uint32Array>;
export const writeDictionaryEncodedFloat32s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride1FixedWidth, writeNumericValue), Float32Array)   as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Float32Array>;
export const writeDictionaryEncodedFloat64s = writeTypedLayout.bind(null, writeDictionaryLayout.bind(null, stride1FixedWidth, writeNumericValue), Float64Array)   as <TNull>(values: values<number, TNull>, nulls?: ArrayLike<TNull>) => DictionaryLayout<Float64Array>;
export const writeDictionaryEncodedVariableWidth = writeDictionaryLayout.bind(null, writeVariableWidth)                                                           as <T, TNull>(writeValue: BufferValueWriter<T>, values: values<T, TNull>, nulls?: ArrayLike<TNull>) => VariableWidthDictionaryLayout<Uint8Array>;
export const writeDictionaryEncodedBinary = writeTypedLayout.bind(null, writeDictionaryEncodedVariableWidth.bind(null, writeBinaryValue))                         as <TNull>(values: values<Iterable<number>, TNull>, nulls?: ArrayLike<TNull>) => VariableWidthDictionaryLayout<Uint8Array>;
export const writeDictionaryEncodedUtf8s = writeTypedLayout.bind(null, writeDictionaryEncodedVariableWidth.bind(null, writeUtf8Value), Uint8Array)                as <TNull>(values: values<string, TNull>, nulls?: ArrayLike<TNull>) => VariableWidthDictionaryLayout<Uint8Array>;

function writeFixedWidthLayoutWithStride<T, TNull>(
    stride: number,
    writeValue: BufferValueWriter<T>,
    values: values<T, TNull>
) {
    let index = -stride;
    const data = [] as number[];
    const length = values.length;
    while ((index += stride) < length) {
        writeValue(values as ArrayLike<T>, data, index);
    }
    return { data: data as ArrayLike<number> };
}

function writeFixedWidthLayout<T, TNull>(
    writeLayout: BufferLayoutWriter<T, TNull>,
    writeValue: BufferValueWriter<T>,
    values: values<T, TNull>,
    nulls?: ArrayLike<TNull>
) {
    return writeLayout(writeValue, values, nulls);
}

function writeValidityLayout<T, TNull>(
    writeLayout: BufferLayoutWriter<T, TNull>,
    writeValue: BufferValueWriter<T>,
    values: values<T, TNull>,
    nulls?: ArrayLike<TNull>
) {
    let nullCount = 0;
    let nullsLength = nulls && nulls.length || 0;
    let validity = new Uint8Array(align(values.length >>> 3, 8)).fill(255);
    return {
        ...writeLayout(writeValueOrValidity, values),
        nullCount, validity: (nullCount > 0 && validity) || new Uint8Array(0)
    } as BufferLayout & NullableLayout;
    function writeValueOrValidity(src: ArrayLike<T>, dst: number[], index: number) {
        writeValue(src, dst, index);
        let i = -1, x = src[index] as T | TNull;
        let isNull = x === null || x === undefined;
        while (!isNull && ++i < nullsLength) {
            isNull = x === nulls![i];
        }
        if (isNull) {
            nullCount++;
            validity[index >> 3] &= ~(1 << (index % 8));
        }
    }
}

function writeVariableWidthLayout<T, TNull>(
    writeLayout: BufferLayoutWriter<T, TNull>,
    writeValue: BufferValueWriter<T>,
    values: values<T, TNull>,
    nulls?: ArrayLike<TNull>
) {
    let offsets = [0], offsetsIndex = 0;
    return { ...writeLayout(writeValueAndOffset, values, nulls), offsets } as VariableWidthLayout;
    function writeValueAndOffset(src: ArrayLike<T>, dst: number[], index: number) {
        if (!writeValue(src, dst, index)) {
            offsets[++offsetsIndex] = dst.length;
        }
    }
}

function writeDictionaryLayout<T, TNull>(
    writeLayout: BufferLayoutWriter<T, TNull>,
    writeValue: BufferValueWriter<T>,
    values: values<T, TNull>,
    nulls?: ArrayLike<TNull>
) {
    let keys = [] as number[], keysIndex = 0, keysMap = Object.create(null);
    return { ...writeLayout(writeKeysOrValues, values, nulls), keys };
    function writeKeysOrValues(src: ArrayLike<T>, dst: number[], index: number) {
        const x: any = src[index];
        if (x in keysMap) {
            return (keys[index] = keysMap[x]) || true;
        } else if (!writeValue(src, dst, index)) {
            keys[index] = keysMap[x] = keysIndex++;
        }
    }
}

function writeTypedLayout<T, TNull, TArray extends TypedArray>(
    writeBuffers: BufferWriter<T, TNull>,
    ArrayBufferView: TypedArrayConstructor<TArray>,
    values: values<T, TNull>,
    nulls?: ArrayLike<TNull>
) {
    const result = writeBuffers(values, nulls);
    result.data = new ArrayBufferView(result.data);
    return result as BufferLayout<TArray>;
}

function writeBooleanValue(src: ArrayLike<boolean>, dst: number[], index: number) {
    if (src[index]) {
        let i = index >>> 3;
        let b = dst[i] || 0;
        dst[i] = b | 1 << (index % 8);
    }
}

function writeNumericValue(src: ArrayLike<number>, dst: number[], index: number) {
    dst[index] = +src[index];
}

function writeFixedWidthValue(bitWidth: number, src: ArrayLike<number>, dst: number[], index: number) {
    const bytesLen = bitWidth / 32;
    for (let i = -1; ++i < bytesLen;) {
        dst[index + i] = src[index + i];
    }
}

function writeUtf8Value(src: ArrayLike<string>, dst: number[], index: number) {
    dst.push(...utf8Encoder.encode(src[index]));
}

function writeBinaryValue(src: ArrayLike<Iterable<number>>, dst: number[], index: number) {
    dst.push(...src[index]);
}
