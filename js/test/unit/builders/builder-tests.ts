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

import '../../jest-extensions.js';
import { from, fromDOMStream, toArray } from 'ix/asynciterable';
import { fromNodeStream } from 'ix/asynciterable/fromnodestream';

import { validateVector } from './utils.js';
import * as generate from '../../generate-test-data.js';

import { Type, DataType, util, Builder, makeBuilder, builderThroughIterable } from 'apache-arrow';

const testDOMStreams = process.env.TEST_DOM_STREAMS === 'true';
const testNodeStreams = process.env.TEST_NODE_STREAMS === 'true';

describe('Generated Test Data', () => {
    describe('NullBuilder', () => { validateBuilder(generate.null_); });
    describe('BoolBuilder', () => { validateBuilder(generate.bool); });
    describe('Int8Builder', () => { validateBuilder(generate.int8); });
    describe('Int16Builder', () => { validateBuilder(generate.int16); });
    describe('Int32Builder', () => { validateBuilder(generate.int32); });
    describe('Int64Builder', () => { validateBuilder(generate.int64); });
    describe('Uint8Builder', () => { validateBuilder(generate.uint8); });
    describe('Uint16Builder', () => { validateBuilder(generate.uint16); });
    describe('Uint32Builder', () => { validateBuilder(generate.uint32); });
    describe('Uint64Builder', () => { validateBuilder(generate.uint64); });
    describe('Float16Builder', () => { validateBuilder(generate.float16); });
    describe('Float32Builder', () => { validateBuilder(generate.float32); });
    describe('Float64Builder', () => { validateBuilder(generate.float64); });
    describe('Utf8Builder', () => { validateBuilder(generate.utf8); });
    describe('BinaryBuilder', () => { validateBuilder(generate.binary); });
    describe('FixedSizeBinaryBuilder', () => { validateBuilder(generate.fixedSizeBinary); });
    describe('DateDayBuilder', () => { validateBuilder(generate.dateDay); });
    describe('DateMillisecondBuilder', () => { validateBuilder(generate.dateMillisecond); });
    describe('TimestampSecondBuilder', () => { validateBuilder(generate.timestampSecond); });
    describe('TimestampMillisecondBuilder', () => { validateBuilder(generate.timestampMillisecond); });
    describe('TimestampMicrosecondBuilder', () => { validateBuilder(generate.timestampMicrosecond); });
    describe('TimestampNanosecondBuilder', () => { validateBuilder(generate.timestampNanosecond); });
    describe('TimeSecondBuilder', () => { validateBuilder(generate.timeSecond); });
    describe('TimeMillisecondBuilder', () => { validateBuilder(generate.timeMillisecond); });
    describe('TimeMicrosecondBuilder', () => { validateBuilder(generate.timeMicrosecond); });
    describe('TimeNanosecondBuilder', () => { validateBuilder(generate.timeNanosecond); });
    describe('DecimalBuilder', () => { validateBuilder(generate.decimal); });
    describe('ListBuilder', () => { validateBuilder(generate.list); });
    describe('StructBuilder', () => { validateBuilder(generate.struct); });
    describe('DenseUnionBuilder', () => { validateBuilder(generate.denseUnion); });
    describe('SparseUnionBuilder', () => { validateBuilder(generate.sparseUnion); });
    describe('DictionaryBuilder', () => { validateBuilder(generate.dictionary); });
    describe('IntervalDayTimeBuilder', () => { validateBuilder(generate.intervalDayTime); });
    describe('IntervalYearMonthBuilder', () => { validateBuilder(generate.intervalYearMonth); });
    describe('FixedSizeListBuilder', () => { validateBuilder(generate.fixedSizeList); });
    describe('MapBuilder', () => { validateBuilder(generate.map); });
});

function validateBuilder(generate: (length?: number, nullCount?: number, ...args: any[]) => generate.GeneratedVector) {

    const type = generate(0, 0).vector.type;

    for (let i = -1; ++i < 1;) {
        validateBuilderWithNullValues(`no nulls`, [], generate(100, 0));
        validateBuilderWithNullValues(`with nulls`, [null], generate(100));
        validateBuilderWithNullValues(`with nulls (length=518)`, [null], generate(518));
        if (DataType.isUtf8(type)) {
            validateBuilderWithNullValues(`with \\0`, ['\0'], generate(100));
            validateBuilderWithNullValues(`with n/a`, ['n/a'], generate(100));
        } else if (DataType.isFloat(type)) {
            validateBuilderWithNullValues(`with NaNs`, [Number.NaN], generate(100));
        } else if (DataType.isInt(type)) {
            validateBuilderWithNullValues(`with MAX_INT`, [
                type.bitWidth < 64 ? 0x7FFFFFFF : 9223372034707292159n
            ], generate(100));
        }
    }
}

const countQueueingStrategy = { highWaterMark: 10 };
const byteLengthQueueingStrategy = { highWaterMark: 64 };

const iterableBuilderOptions = <T extends DataType = any>({ vector }: generate.GeneratedVector, { type, ...opts }: BuilderOptions<T>) => ({
    ...opts, type,
    valueToChildTypeId: !DataType.isUnion(type) ? undefined : (() => {
        let { typeIds } = vector.data[0];
        let lastChunkLength = 0, chunksLength = 0;
        return (builder: Builder<T>, _value: any, index: number) => {
            if (index === 0) {
                chunksLength += lastChunkLength;
            }
            lastChunkLength = builder.length + 1;
            return typeIds[chunksLength + index];
        };
    })()
});

const domStreamBuilderOptions = <T extends DataType = any>({ vector }: generate.GeneratedVector, { type, queueingStrategy, ...opts }: Partial<BuilderTransformOptions<T>>) => ({
    ...opts, type,
    valueToChildTypeId: !DataType.isUnion(type) ? undefined : (() => {
        let { typeIds } = vector.data[0];
        let lastChunkLength = 0, chunksLength = 0;
        return (builder: Builder<T>, _value: any, index: number) => {
            if (index === 0) {
                chunksLength += lastChunkLength;
            }
            lastChunkLength = builder.length + 1;
            return typeIds[chunksLength + index];
        };
    })(),
    queueingStrategy,
    readableStrategy: queueingStrategy === 'bytes' ? byteLengthQueueingStrategy : countQueueingStrategy,
    writableStrategy: queueingStrategy === 'bytes' ? byteLengthQueueingStrategy : countQueueingStrategy,
});

const nodeStreamBuilderOptions = <T extends DataType = any>({ vector }: generate.GeneratedVector, { type, queueingStrategy, ...opts }: Partial<BuilderDuplexOptions<T>>) => ({
    ...opts, type,
    valueToChildTypeId: !DataType.isUnion(type) ? undefined : (() => {
        let { typeIds } = vector.data[0];
        let lastChunkLength = 0, chunksLength = 0;
        return (builder: Builder<T>, _value: any, index: number) => {
            if (index === 0) {
                chunksLength += lastChunkLength;
            }
            lastChunkLength = builder.length + 1;
            return typeIds[chunksLength + index];
        };
    })(),
    queueingStrategy,
    highWaterMark: queueingStrategy === 'bytes' ? 64 : 10
});

function validateBuilderWithNullValues(suiteName: string, nullValues: any[], generated: generate.GeneratedVector) {

    const type = generated.vector.type;
    const referenceNullValues = nullValues.slice();
    const originalValues = generated.values().slice();
    const typeName = Type[type.typeId].toLowerCase();

    let values: any[];
    const opts: any = { type, nullValues };

    if (DataType.isNull(type) || (nullValues.length === 1 && nullValues[0] === null)) {
        values = originalValues.slice();
    } else if (nullValues.length > 0) {
        values = fillNA(originalValues, nullValues);
    } else {
        values = fillNADefault(originalValues, [originalValues.find((x) => x !== null)]);
    }

    if (DataType.isInt(type) && type.bitWidth === 64 && ArrayBuffer.isView(nullValues[0])) {
        referenceNullValues[0] = util.BN.new<any>(nullValues[0])[Symbol.toPrimitive]('default');
    }

    describe(suiteName, () => {
        it(`encodes ${typeName} single`, () => {
            const opts_ = iterableBuilderOptions(generated, { ...opts });
            const vector = encodeSingle(values.slice(), opts_);
            validateVector(values, vector, referenceNullValues);
        });
        it(`encodes ${typeName} chunks by count`, () => {
            const highWaterMark = Math.max(5, Math.trunc(Math.random() * values.length - 5));
            const opts_ = iterableBuilderOptions(generated, { ...opts, highWaterMark, queueingStrategy: 'count' });
            const vector = encodeChunks(values.slice(), opts_);
            validateVector(values, vector, referenceNullValues);
        });
        it(`encodes ${typeName} chunks by bytes`, () => {
            const highWaterMark = 64;
            const opts_ = iterableBuilderOptions(generated, { ...opts, highWaterMark, queueingStrategy: 'bytes' });
            const vector = encodeChunks(values.slice(), opts_);
            validateVector(values, vector, referenceNullValues);
        });
        if (testDOMStreams) {
            it(`encodes ${typeName} chunks from a DOM stream by count`, async () => {
                const opts_ = domStreamBuilderOptions(generated, { ...opts, queueingStrategy: 'count' });
                const vector = await encodeChunksDOM(values.slice(), opts_);
                validateVector(values, vector, referenceNullValues);
            });
            it(`encodes ${typeName} chunks from a DOM stream by bytes`, async () => {
                const opts_ = domStreamBuilderOptions(generated, { ...opts, queueingStrategy: 'bytes' });
                const vector = await encodeChunksDOM(values.slice(), opts_);
                validateVector(values, vector, referenceNullValues);
            });
        }
        if (testNodeStreams) {
            it(`encodes ${typeName} chunks from a Node stream by count`, async () => {
                const opts_ = nodeStreamBuilderOptions(generated, { ...opts, queueingStrategy: 'count' });
                const vector = await encodeChunksNode(values.slice(), opts_);
                validateVector(values, vector, referenceNullValues);
            });
            it(`encodes ${typeName} chunks from a Node stream by bytes`, async () => {
                const opts_ = nodeStreamBuilderOptions(generated, { ...opts, queueingStrategy: 'bytes' });
                const vector = await encodeChunksNode(values.slice(), opts_);
                validateVector(values, vector, referenceNullValues);
            });
        }
    });
}

function fillNA(values: any[], nulls: any[]): any[] {
    const n = nulls.length - 1;
    return values.map((x) => {
        if (x === null) {
            return nulls[Math.round(n * Math.random())];
        }
        return x;
    });
}

function fillNADefault(values: any[], nulls: any[]): any[] {
    const n = nulls.length - 1;
    return values.map((x) => {
        if (x === null) {
            return nulls[Math.round(n * Math.random())];
        } else if (Array.isArray(x) && x.length > 0) {
            let defaultValue = x.find((y) => y !== null);
            if (defaultValue === undefined) { defaultValue = 0; }
            return fillNADefault(x, [defaultValue]);
        }
        return x;
    });
}

type BuilderOptions<T extends DataType = any, TNull = any> = import('apache-arrow/builder').BuilderOptions<T, TNull>;
type BuilderDuplexOptions<T extends DataType = any, TNull = any> = import('apache-arrow/io/node/builder').BuilderDuplexOptions<T, TNull>;
type BuilderTransformOptions<T extends DataType = any, TNull = any> = import('apache-arrow/io/whatwg/builder').BuilderTransformOptions<T, TNull>;

function encodeSingle<T extends DataType, TNull = any>(values: (T['TValue'] | TNull)[], options: BuilderOptions<T, TNull>) {
    const builder = makeBuilder(options);
    for (const x of values) builder.append(x);
    return builder.finish().toVector();
}

function encodeChunks<T extends DataType, TNull = any>(values: (T['TValue'] | TNull)[], options: BuilderOptions<T, TNull>) {
    return [...builderThroughIterable(options)(values)].reduce((a, b) => a.concat(b));
}

async function encodeChunksDOM<T extends DataType, TNull = any>(values: (T['TValue'] | TNull)[], options: BuilderTransformOptions<T, TNull>) {

    const stream = from(values).toDOMStream()
        .pipeThrough(Builder.throughDOM(options));

    const chunks = await fromDOMStream(stream).pipe(toArray);

    return chunks.reduce((a, b) => a.concat(b));
}

async function encodeChunksNode<T extends DataType, TNull = any>(values: (T['TValue'] | TNull)[], options: BuilderDuplexOptions<T, TNull>) {

    if (options.nullValues) {
        options.nullValues = [...options.nullValues, undefined] as TNull[];
    }

    const stream = from(fillNA(values, [undefined]))
        .toNodeStream({ objectMode: true })
        .pipe(Builder.throughNode(options));

    const chunks: any[] = await fromNodeStream(stream, options.highWaterMark).pipe(toArray);

    return chunks.reduce((a, b) => a.concat(b));
}
