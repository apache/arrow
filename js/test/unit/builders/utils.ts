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

import 'web-streams-polyfill';

import { from, fromDOMStream, toArray } from 'ix/Ix.asynciterable';
import { fromNodeStream } from 'ix/asynciterable/fromnodestream';
import 'ix/Ix.node';

import '../../jest-extensions.js';
import { randomString } from '../../random-string.js';

import { Builder, makeBuilder, builderThroughIterable, DataType, util, Vector } from 'apache-arrow';

const rand = Math.random.bind(Math);
const randnulls = <T, TNull = null>(values: T[], n: TNull = <any>null) => values.map((x) => Math.random() > 0.25 ? x : n) as (T | TNull)[];

export const randomBytes = (length: number) => fillRandom(Uint8Array, length);

export const stringsNoNulls = (length = 20) => Array.from({ length }, (_) => randomString(1 + (Math.trunc(Math.random() * 19))));

export const boolsNoNulls = (length = 20) => Array.from({ length }, () => rand() > 0.5);

export const dateNoNulls = (length = 20, now = Math.trunc(Date.now() / 86400000)) =>
    Array.from({ length }, (_) => (Math.trunc(now + (rand() * 100000 * (rand() > 0.5 ? -1 : 1)))) * 86400000);
export const int8sNoNulls = (length = 20) => Array.from(new Int8Array(randomBytes(length * Int8Array.BYTES_PER_ELEMENT).buffer));
export const int16sNoNulls = (length = 20) => Array.from(new Int16Array(randomBytes(length * Int16Array.BYTES_PER_ELEMENT).buffer));
export const int32sNoNulls = (length = 20) => Array.from(new Int32Array(randomBytes(length * Int32Array.BYTES_PER_ELEMENT).buffer));
export const int64sNoNulls = (length = 20) => Array.from(new BigInt64Array(randomBytes(length * BigInt64Array.BYTES_PER_ELEMENT).buffer));

export const uint8sNoNulls = (length = 20) => Array.from(new Uint8Array(randomBytes(length * Uint8Array.BYTES_PER_ELEMENT).buffer));
export const uint16sNoNulls = (length = 20) => Array.from(new Uint16Array(randomBytes(length * Uint16Array.BYTES_PER_ELEMENT).buffer));
export const uint32sNoNulls = (length = 20) => Array.from(new Uint32Array(randomBytes(length * Uint32Array.BYTES_PER_ELEMENT).buffer));
export const uint64sNoNulls = (length = 20) => Array.from(new BigUint64Array(randomBytes(length * BigUint64Array.BYTES_PER_ELEMENT).buffer));
export const float16sNoNulls = (length = 20) => Array.from(new Uint16Array(randomBytes(length * Uint16Array.BYTES_PER_ELEMENT).buffer)).map(x => util.uint16ToFloat64(x));
export const float32sNoNulls = (length = 20) => Array.from(new Float32Array(randomBytes(length * Float32Array.BYTES_PER_ELEMENT).buffer));
export const float64sNoNulls = (length = 20) => Array.from(new Float64Array(randomBytes(length * Float64Array.BYTES_PER_ELEMENT).buffer));

export const stringsWithNAs = (length = 20) => randnulls(stringsNoNulls(length), 'n/a');
export const stringsWithNulls = (length = 20) => randnulls(stringsNoNulls(length), null);
export const stringsWithEmpties = (length = 20) => randnulls(stringsNoNulls(length), '\0');

export const boolsWithNulls = (length = 20) => randnulls(boolsNoNulls(length), null);
export const dateWithNulls = (length = 20) => randnulls(dateNoNulls(length), null);
export const int8sWithNulls = (length = 20) => randnulls(int8sNoNulls(length), null);
export const int16sWithNulls = (length = 20) => randnulls(int16sNoNulls(length), null);
export const int32sWithNulls = (length = 20) => randnulls(int32sNoNulls(length), null);
export const int64sWithNulls = (length = 20) => randnulls(int64sNoNulls(length), null);
export const uint8sWithNulls = (length = 20) => randnulls(uint8sNoNulls(length), null);
export const uint16sWithNulls = (length = 20) => randnulls(uint16sNoNulls(length), null);
export const uint32sWithNulls = (length = 20) => randnulls(uint32sNoNulls(length), null);
export const uint64sWithNulls = (length = 20) => randnulls(uint64sNoNulls(length), null);
export const float16sWithNulls = (length = 20) => randnulls(float16sNoNulls(length), null);
export const float32sWithNulls = (length = 20) => randnulls(float32sNoNulls(length), null);
export const float64sWithNulls = (length = 20) => randnulls(float64sNoNulls(length), null);

export const int8sWithMaxInts = (length = 20) => randnulls(int8sNoNulls(length), 0x7FFFFFFF);
export const int16sWithMaxInts = (length = 20) => randnulls(int16sNoNulls(length), 0x7FFFFFFF);
export const int32sWithMaxInts = (length = 20) => randnulls(int32sNoNulls(length), 0x7FFFFFFF);
export const int64sWithMaxInts = (length = 20) => randnulls(int64sNoNulls(length), 9223372034707292159n);
export const uint8sWithMaxInts = (length = 20) => randnulls(uint8sNoNulls(length), 0x7FFFFFFF);
export const uint16sWithMaxInts = (length = 20) => randnulls(uint16sNoNulls(length), 0x7FFFFFFF);
export const uint32sWithMaxInts = (length = 20) => randnulls(uint32sNoNulls(length), 0x7FFFFFFF);
export const uint64sWithMaxInts = (length = 20) => randnulls(uint64sNoNulls(length), 9223372034707292159n);
export const float16sWithNaNs = (length = 20) => randnulls(float16sNoNulls(length), Number.NaN);
export const float32sWithNaNs = (length = 20) => randnulls(float32sNoNulls(length), Number.NaN);
export const float64sWithNaNs = (length = 20) => randnulls(float64sNoNulls(length), Number.NaN);

export const duplicateItems = (n: number, xs: (any | null)[]) => {
    const out = new Array<string | null>(n);
    for (let i = -1, k = xs.length; ++i < n;) {
        out[i] = xs[Math.trunc(Math.random() * k)];
    }
    return out;
};

export function encodeAll<T extends DataType>(typeFactory: () => T) {
    return async function encodeAll<TNull = any>(values: (T['TValue'] | TNull)[], nullValues?: TNull[]) {
        const type = typeFactory();
        const builder = makeBuilder({ type, nullValues });
        for (const x of values) builder.append.bind(builder)(x);
        return builder.finish().toVector();
    };
}

export function encodeEach<T extends DataType>(typeFactory: () => T, chunkLen?: number) {
    return async function encodeEach<TNull = any>(vals: (T['TValue'] | TNull)[], nullValues?: TNull[]) {
        const type = typeFactory();
        const opts = { type, nullValues, highWaterMark: chunkLen };
        const chunks = [...builderThroughIterable(opts)(vals)];
        return chunks.reduce((a, b) => a.concat(b)) as Vector<T>;
    };
}

export function encodeEachDOM<T extends DataType>(typeFactory: () => T, chunkLen?: number) {
    return async function encodeEachDOM<TNull = any>(vals: (T['TValue'] | TNull)[], nullValues?: TNull[]) {
        const type = typeFactory();
        const strategy = { highWaterMark: chunkLen };
        const source = from(vals).toDOMStream();
        const builder = Builder.throughDOM({ type, nullValues, readableStrategy: strategy, writableStrategy: strategy });
        const chunks = await fromDOMStream(source.pipeThrough(builder)).pipe(toArray);
        return chunks.reduce((a, b) => a.concat(b)) as Vector<T>;
    };
}

export function encodeEachNode<T extends DataType>(typeFactory: () => T, chunkLen?: number) {
    return async function encodeEachNode<TNull = any>(vals: (T['TValue'] | TNull)[], nullValues?: TNull[]) {
        const type = typeFactory();
        const vals_ = vals.map((x) => x === null ? undefined : x);
        const source = from(vals_).toNodeStream({ objectMode: true });
        const nulls_ = nullValues ? nullValues.map((x) => x === null ? undefined : x) : nullValues;
        const builder = Builder.throughNode({ type, nullValues: nulls_, highWaterMark: chunkLen });
        const chunks: any[] = await fromNodeStream(source.pipe(builder), chunkLen).pipe(toArray);
        return chunks.reduce((a, b) => a.concat(b)) as Vector<T>;
    };
}

const isInt64Null = (nulls: Map<any, any>, x: any) => {
    if (ArrayBuffer.isView(x)) {
        const bn = util.BN.new<Int32Array>(x as Int32Array);
        return nulls.has((<any>bn)[Symbol.toPrimitive]('default'));
    }
    return false;
};

export function validateVector<T extends DataType>(vals: (T['TValue'] | null)[], vec: Vector, nullVals: any[]) {
    let i = 0, x: T['TValue'] | null, y: T['TValue'] | null;
    const nulls = nullVals.reduce((m, x) => m.set(x, x), new Map());
    try {
        for (x of vec) {
            if (nulls.has(y = vals[i])) {
                expect(x).toBeNull();
            } else if (isInt64Null(nulls, y)) {
                expect(x).toBeNull();
            } else {
                expect(x).toArrowCompare(y);
            }
            i++;
        }
    } catch (e: any) {
        // Uncomment these two lines to catch and debug the value retrieval that failed
        // debugger;
        // vec.get(i);
        throw new Error([
            `${(vec as any).VectorName}[${i}]: ${e?.stack || e}`,
            `nulls: [${nullVals.join(', ')}]`,
            `values: [${vals.join(', ')}]`,
        ].join('\n'));
    }
}

function fillRandom<T extends TypedArrayConstructor>(ArrayType: T, length: number) {
    const BPE = ArrayType.BYTES_PER_ELEMENT;
    const array = new ArrayType(length);
    const max = (2 ** (8 * BPE)) - 1;
    for (let i = -1; ++i < length; array[i] = rand() * max * (rand() > 0.5 ? -1 : 1)) { }
    return array as InstanceType<T>;
}

type TypedArrayConstructor =
    (typeof Int8Array) |
    (typeof Int16Array) |
    (typeof Int32Array) |
    (typeof Uint8Array) |
    (typeof Uint16Array) |
    (typeof Uint32Array) |
    (typeof Float32Array) |
    (typeof Float64Array);
