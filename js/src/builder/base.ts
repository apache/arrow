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

import { Data, Buffers } from '../data';
import { createIsValidFunction } from './valid';
import { VectorType as BufferType } from '../enum';
import { BufferBuilder, BitmaskBuilder, DataBufferBuilder, BinaryBufferBuilder, OffsetsBufferBuilder } from './buffer';
import { DataType, strideForType, Float, Int, Decimal, Date_, Time, Timestamp, Interval, FixedSizeBinary, Struct, Map_, Union, Utf8, Binary } from '../type';

export interface BuilderOptions<T extends DataType = any, TNull = any> {
    type: T;
    nullValues?: TNull[] | ReadonlyArray<TNull> | null;
    children?: { [key: string]: BuilderOptions; } | BuilderOptions[];
}

/** @ignore */
export interface IterableBuilderOptions<T extends DataType = any, TNull = any> extends BuilderOptions<T, TNull> {
    highWaterMark?: number;
    queueingStrategy?: 'bytes' | 'count';
    dictionaryHashFunction?: (value: any) => string | number;
    valueToChildTypeId?: (builder: Builder<T, TNull>, value: any, offset: number) => number;
}

export abstract class Builder<T extends DataType = any, TNull = any> {

    /** @nocollapse */
    // @ts-ignore
    public static throughNode<T extends DataType = any, TNull = any>(options: import('../io/node/builder').BuilderDuplexOptions<T, TNull>): import('stream').Duplex {
        throw new Error(`"throughNode" not available in this environment`);
    }
    /** @nocollapse */
    // @ts-ignore
    public static throughDOM<T extends DataType = any, TNull = any>(options: import('../io/whatwg/builder').BuilderTransformOptions<T, TNull>): import('../io/whatwg/builder').BuilderTransform<T, TNull> {
        throw new Error(`"throughDOM" not available in this environment`);
    }

    /** @nocollapse */
    public static throughIterable<T extends DataType = any, TNull = any>(options: IterableBuilderOptions<T, TNull>) { return throughIterable(options); }
    /** @nocollapse */
    public static throughAsyncIterable<T extends DataType = any, TNull = any>(options: IterableBuilderOptions<T, TNull>) { return throughAsyncIterable(options); }

    constructor({ 'type': type, 'nullValues': nulls }: BuilderOptions<T, TNull>) {
        this._type = type;
        this.children = [];
        this.stride = strideForType(type);
        this._nulls = new BitmaskBuilder();
        if (nulls && nulls.length > 0) {
            this._isValid = createIsValidFunction(nulls);
        }
    }

    public readonly stride: number;
    public readonly children: Builder[];

    public get length() { return this._nulls.length; }
    public get ArrayType() { return this._type.ArrayType; }
    public get nullCount() { return this._nulls.numInvalid; }
    public get numChildren() { return this.children.length; }

    public get byteLength(): number {
        let size = 0;
        this._nulls && (size += this._nulls.byteLength);
        this._offsets && (size += this._offsets.byteLength);
        this._values && (size += this._values.byteLength);
        this._typeIds && (size += this._typeIds.byteLength);
        return this.children.reduce((size, child) => size + child.byteLength, size);
    }

    public get reservedLength(): number {
        let size = 0;
        this._nulls && (size += this._nulls.reservedLength);
        this._offsets && (size += this._offsets.reservedLength);
        this._values && (size += this._values.reservedLength);
        this._typeIds && (size += this._typeIds.reservedLength);
        return this.children.reduce((size, child) => size + child.reservedLength, size);
    }

    public get reservedByteLength(): number {
        let size = 0;
        this._nulls && (size += this._nulls.reservedByteLength);
        this._offsets && (size += this._offsets.reservedByteLength);
        this._values && (size += this._values.reservedByteLength);
        this._typeIds && (size += this._typeIds.reservedByteLength);
        return this.children.reduce((size, child) => size + child.reservedByteLength, size);
    }

    protected _type: T;
    public get type() { return this._type; }

    protected _finished = false;
    public get finished() { return this._finished; }

    // @ts-ignore
    protected _offsets: DataBufferBuilder<Int32Array>;
    public get valueOffsets() { return this._offsets.buffer; }

    // @ts-ignore
    protected _values: BufferBuilder<T['TArray'], any>;
    public get values() { return this._values.buffer; }

    protected _nulls: BitmaskBuilder;
    public get nullBitmap() { return this._nulls.buffer; }

    // @ts-ignore
    protected _typeIds: DataBufferBuilder<Int8Array>;
    public get typeIds() { return this._typeIds.buffer; }

    // @ts-ignore
    protected _isValid: (value: T['TValue'] | TNull) => boolean;
    // @ts-ignore
    protected _setValue: (inst: Builder<T>, index: number, value: T['TValue']) => void;

    public append(value: T['TValue'] | TNull) { return this.set(this.length, value); }

    /**
     * Validates whether a value is valid (true), or null (false)
     * @param value The value to compare against null the value representations
     */
    // @ts-ignore
    public isValid(value: T['TValue'] | TNull): boolean { return this._isValid(value); }

    public set(index: number, value: T['TValue'] | TNull) {
        if (this.setValid(index, this.isValid(value))) {
            this.setValue(index, value);
        }
        return this;
    }
    // @ts-ignore
    public setValue(index: number, value: T['TValue']) { this._setValue(this, index, value); }
    public setValid(index: number, valid: boolean) {
        const bit = +valid as 0 | 1;
        this._nulls.set(index, bit);
        return valid;
    }

    // @ts-ignore
    public addChild(child: Builder, name = `${this.numChildren}`) {
        throw new Error(`Cannot append children to non-nested type "${this._type}"`);
    }

    public getChildAt<R extends DataType = any>(index: number): Builder<R> | null {
        return this.children[index];
    }

    public flush() {

        const buffers: any = [];
        const values =  this._values;
        const offsets =  this._offsets;
        const typeIds =  this._typeIds;
        const { length, nullCount } = this;

        if (typeIds) { /* Unions */
            buffers[BufferType.TYPE] = typeIds.flush(length);
            // DenseUnions
            offsets && (buffers[BufferType.OFFSET] = offsets.flush(length));
        } else if (offsets) { /* Variable-width primitives (Binary, Utf8) and Lists */
            // Binary, Utf8
            values && (buffers[BufferType.DATA] = values.flush(offsets.last()));
            buffers[BufferType.OFFSET] = offsets.flush(length);
        } else if (values) { /* Fixed-width primitives (Int, Float, Decimal, Time, Timestamp, and Interval) */
            buffers[BufferType.DATA] = values.flush(length);
        }

        nullCount > 0 && (buffers[BufferType.VALIDITY] = this._nulls.flush(length));

        const data = Data.new<T>(
            this._type, 0, length, nullCount, buffers as Buffers<T>,
            this.children.map((child) => child.flush())) as Data<T>;

        this.clear();

        return data;
    }

    public finish() {
        this._finished = true;
        this.children.forEach((child) => child.finish());
        return this;
    }

    public clear() {
        this._nulls && (this._nulls.clear());
        this._values && (this._values.clear());
        this._offsets && (this._offsets.clear());
        this._typeIds && (this._typeIds.clear());
        this.children.forEach((child) => child.clear());
        return this;
    }
}

(Builder.prototype as any)._isValid = () => true;

export abstract class FixedWidthBuilder<T extends Int | Float | FixedSizeBinary | Date_ | Timestamp | Time | Decimal | Interval = any, TNull = any> extends Builder<T, TNull> {
    constructor(opts: BuilderOptions<T, TNull>) {
        super(opts);
        const stride = strideForType(this._type);
        this._values = new DataBufferBuilder(new this.ArrayType(0), stride);
    }
    public setValue(index: number, value: T['TValue']) {
        const values = this._values;
        values.reserve(index - values.length + 1);
        return super.setValue(index, value);
    }
}

export abstract class VariableWidthBuilder<T extends Binary | Utf8, TNull = any> extends Builder<T, TNull> {
    protected _offsets: OffsetsBufferBuilder;
    protected _buffers: Map<number, Uint8Array> | undefined;
    constructor(opts: BuilderOptions<T, TNull>) {
        super(opts);
        this._offsets = new OffsetsBufferBuilder();
        this._values = new BinaryBufferBuilder() as BufferBuilder<T['TArray']>;
    }
    public setValue(index: number, value: Uint8Array | string) {
        (this._offsets.set(index, value.length));
        (this._buffers || (this._buffers = new Map())).set(index, value);
    }
    public setValid(index: number, isValid: boolean) {
        if (!super.setValid(index, isValid)) {
            this._offsets.append(0);
            return false;
        }
        return true;
    }
    public clear() {
        this._buffers = undefined;
        return super.clear();
    }
    public flush() {
        const buffers = this._buffers;
        if (buffers && buffers.size > 0) {
            this._values.reserve(this._offsets.last());
            for (let [i, x] of buffers) { super.setValue(i, x); }
        }
        return super.flush();
    }
}

export abstract class NestedBuilder<T extends Struct | Map_ | Union = any, TNull = any> extends Builder<T, TNull> {
    constructor(options: BuilderOptions<T, TNull>) {
        super(options);
    }
}

type ThroughIterable<T extends DataType = any, TNull = any> = (source: Iterable<T['TValue'] | TNull>) => IterableIterator<Data<T>>;

function throughIterable<T extends DataType = any, TNull = any>(options: IterableBuilderOptions<T, TNull>): ThroughIterable<T, TNull> {
    const { ['queueingStrategy']: queueingStrategy = 'count' } = options;
    const { ['highWaterMark']: highWaterMark = queueingStrategy !== 'bytes' ? 1000 : 2 ** 14 } = options;
    const sizeProperty: 'length' | 'byteLength' = queueingStrategy !== 'bytes' ? 'length' : 'byteLength';
    return function*(source: Iterable<T['TValue'] | TNull>) {
        const builder = Builder.new(options);
        for (const value of source) {
            if (builder.append(value)[sizeProperty] >= highWaterMark) {
                yield builder.flush();
            }
        }
        if (builder.finish().length > 0) yield builder.flush();
    }
}

type ThroughAsyncIterable<T extends DataType = any, TNull = any> = (source: Iterable<T['TValue'] | TNull> | AsyncIterable<T['TValue'] | TNull>) => AsyncIterableIterator<Data<T>>;

function throughAsyncIterable<T extends DataType = any, TNull = any>(options: IterableBuilderOptions<T, TNull>): ThroughAsyncIterable<T, TNull> {
    const { ['queueingStrategy']: queueingStrategy = 'count' } = options;
    const { ['highWaterMark']: highWaterMark = queueingStrategy !== 'bytes' ? 1000 : 2 ** 14 } = options;
    const sizeProperty: 'length' | 'byteLength' = queueingStrategy !== 'bytes' ? 'length' : 'byteLength';
    return async function* (source: Iterable<T['TValue'] | TNull> | AsyncIterable<T['TValue'] | TNull>) {
        const builder = Builder.new(options);
        for await (const value of source) {
            if (builder.append(value)[sizeProperty] >= highWaterMark) {
                yield builder.flush();
            }
        }
        if (builder.finish().length > 0) yield builder.flush();
    }
}
