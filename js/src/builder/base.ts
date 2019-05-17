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

import { setBool } from '../util/bit';
import { memcpy } from '../util/buffer';
import { Data, Buffers } from '../data';
import { valueToString } from '../util/pretty';
import { BigIntAvailable } from '../util/compat';
import { TypedArray, TypedArrayConstructor } from '../interfaces';
import {
    DataType, strideForType,
    Utf8, Binary, Decimal, FixedSizeBinary, List, FixedSizeList, Map_, Struct,
    Float, Int, Date_, Interval, Time, Timestamp, Union, DenseUnion, SparseUnion,
} from '../type';

export interface BuilderOptions<T extends DataType = any, TNull = any> {
    type: T;
    nullValues?: TNull[];
}

export class Builder<T extends DataType = any, TNull = any> {

    /** @nocollapse */
    public static throughNode<T extends DataType = any, TNull = any>(
        // @ts-ignore
        options: import('stream').DuplexOptions & BuilderOptions<T, TNull>
    ): import('stream').Duplex {
        throw new Error(`"throughNode" not available in this environment`);
    }
    /** @nocollapse */
    public static throughDOM<T extends DataType = any, TNull = any>(
        // @ts-ignore
        writableStrategy: QueuingStrategy<T['TValue'] | TNull> & BuilderOptions<T, TNull>,
        // @ts-ignore
        readableStrategy?: { highWaterMark?: number, size?: any }
    ): { writable: WritableStream<T['TValue'] | TNull>, readable: ReadableStream<Data<T>> } {
        throw new Error(`"throughDOM" not available in this environment`);
    }

    public length = 0;
    public nullCount = 0;

    public readonly offset = 0;
    public readonly stride: number;
    public readonly children: Builder[];
    public readonly nullValues: ReadonlyArray<TNull>;

    // @ts-ignore
    public valueOffsets: Int32Array;
    // @ts-ignore
    public values: T['TArray'];
    // @ts-ignore
    public nullBitmap: Uint8Array;
    // @ts-ignore
    public typeIds: Int8Array;

    constructor(options: BuilderOptions<T, TNull>) {
        const type = options['type'];
        const nullValues = options['nullValues'];
        this.stride = strideForType(this._type = type);
        this.children = (type.children || []).map((f) => new Builder(f.type));
        this.nullValues = Object.freeze(nullValues || []) as ReadonlyArray<TNull>;
        this.nullBitmap = new Uint8Array(0);
        if (this.nullValues.length) {
            this._isValid = compileIsValid<T, TNull>(this.nullValues);
            this.children.forEach((child: any /* <-- any so we can assign to `nullValues` */) => {
                child._isValid = this._isValid;
                child.nullValues = this.nullValues;
                child.nullBitmap = new Uint8Array(0);
            });
        }
    }

    protected _type: T;
    public get type() { return this._type; }

    protected _finished: boolean = false;
    public get finished() { return this._finished; }

    protected _bytesUsed = 0;
    public get bytesUsed() { return this._bytesUsed; }

    protected _bytesReserved = 0;
    public get bytesReserved() { return this._bytesReserved; }

    // @ts-ignore
    protected _isValid: (value: T['TValue'] | TNull) => boolean;
    // @ts-ignore
    protected _setValue: (inst: Builder<T>, index: number, value: T['TValue']) => void;

    public get ArrayType() { return this._type.ArrayType; }

    public *writeAll(source: Iterable<any>, chunkLength = Infinity) {
        for (const value of source) {
            if (this.write(value).length >= chunkLength) {
                yield this.flush();
            }
        }
        if (this.finish().length > 0) yield this.flush();
    }

    public async *writeAllAsync(source: Iterable<any> | AsyncIterable<any>, chunkLength = Infinity) {
        for await (const value of source) {
            if (this.write(value).length >= chunkLength) {
                yield this.flush();
            }
        }
        if (this.finish().length > 0) yield this.flush();
    }

    /**
     * Validates whether a value is valid (true), or null (false)
     * @param value The value to compare against null the value representations
     */
    public isValid(value: T['TValue'] | TNull): boolean {
        return this._isValid(value);
    }

    public write(value: T['TValue'] | TNull, ..._: any[]): this;
    public write(value: T['TValue'] | TNull) {
        const offset = this.length;
        if (this.writeValid(this.isValid(value), offset)) {
            this.writeValue(value, offset);
        }
        return this._updateBytesUsed(offset, this.length = offset + 1);
    }

    /** @ignore */
    public writeValue(value: T['TValue'], offset: number, ..._: any[]): void;
    public writeValue(value: T['TValue'], offset: number): void {
        this._setValue(this, offset, value);
    }

    /** @ignore */
    public writeValid(isValid: boolean, offset: number): boolean {
        isValid || ++this.nullCount;
        setBool(this._getNullBitmap(offset), offset, isValid);
        return isValid;
    }

    // @ts-ignore
    protected _updateBytesUsed(offset: number, length: number) {
        offset % 512 || (this._bytesUsed += 64);
        return this;
    }

    public flush() {

        const { length, nullCount } = this;
        let { valueOffsets, values, nullBitmap, typeIds } = this;

        if (valueOffsets) {
            valueOffsets = sliceOrExtendArray(valueOffsets, roundLengthToMultipleOf64Bytes(length, 4));
            values && (values = sliceOrExtendArray(values, roundLengthToMultipleOf64Bytes(valueOffsets[length], values.BYTES_PER_ELEMENT)));
        } else if (values) {
            values = sliceOrExtendArray(values, roundLengthToMultipleOf64Bytes(length * this.stride, values.BYTES_PER_ELEMENT));
        }

        nullBitmap && (nullBitmap = nullCount === 0 ? new Uint8Array(0)
            : sliceOrExtendArray(nullBitmap, roundLengthToMultipleOf64Bytes(length >> 3, 1) || 64));

        typeIds && (typeIds = sliceOrExtendArray(typeIds, roundLengthToMultipleOf64Bytes(length, 1)));

        const data = Data.new<T>(
            this._type, 0, length, nullCount, [
            valueOffsets, values, nullBitmap, typeIds] as Buffers<T>,
            this.children.map((child) => child.flush())) as Data<T>;

        this.reset();

        return data;
    }

    public finish() {
        this._finished = true;
        this.children.forEach((child) => child.finish());
        return this;
    }

    public reset() {
        this.length = 0;
        this.nullCount = 0;
        this._bytesUsed = 0;
        this._bytesReserved = 0;
        this.children.forEach((child) => child.reset());
        this.values && (this.values = this.values.subarray(0, 0));
        this.typeIds && (this.typeIds = this.typeIds.subarray(0, 0));
        this.nullBitmap && (this.nullBitmap = this.nullBitmap.subarray(0, 0));
        this.valueOffsets && (this.valueOffsets = this.valueOffsets.subarray(0, 0));
        return this;
    }

    protected _getNullBitmap(length: number) {
        let buf = this.nullBitmap;
        if ((length >> 3) >= buf.length) {
            length = roundLengthToMultipleOf64Bytes(length, 1) || 32;
            this.nullBitmap = buf = memcpy(new Uint8Array(length * 2), buf);
        }
        return buf;
    }
    protected _getValueOffsets(length: number) {
        let buf = this.valueOffsets;
        if (length >= buf.length - 1) {
            length = roundLengthToMultipleOf64Bytes(length, 4) || 8;
            this.valueOffsets = buf = memcpy(new Int32Array(length * 2), buf);
        }
        return buf;
    }
    protected _getValues(length: number) {
        let { stride, values: buf } = this;
        if ((length * stride) >= buf.length) {
            let { ArrayType } = this, BPE = ArrayType.BYTES_PER_ELEMENT;
            length = roundLengthToMultipleOf64Bytes(length, BPE) || (32 / BPE);
            this.values = buf = memcpy(new ArrayType(length * stride * 2), buf);
        }
        return buf;
    }
    protected _getValuesBitmap(length: number) {
        let buf = this.values;
        if ((length >> 3) >= buf.length) {
            length = roundLengthToMultipleOf64Bytes(length, 1) || 32;
            this.values = buf = memcpy(new Uint8Array(length * 2), buf);
        }
        return buf;
    }
    protected _getTypeIds(length: number) {
        let buf = this.typeIds;
        if (length >= buf.length) {
            length = roundLengthToMultipleOf64Bytes(length, 1) || 32;
            this.typeIds = buf = memcpy(new Int8Array(length * 2), buf);
        }
        return buf;
    }
}

(Builder.prototype as any)._isValid = compileIsValid<any, any>([null, undefined]);

export abstract class FlatBuilder<T extends Int | Float | FixedSizeBinary | Date_ | Timestamp | Time | Decimal | Interval = any, TNull = any> extends Builder<T, TNull> {
    public readonly BYTES_PER_ELEMENT: number;
    constructor(options: BuilderOptions<T, TNull>) {
        super(options);
        this.values = new this.ArrayType(0);
        this.BYTES_PER_ELEMENT = this.stride * this.ArrayType.BYTES_PER_ELEMENT;
    }
    public get bytesReserved() {
        return this.values.byteLength + this.nullBitmap.byteLength;
    }
    public writeValue(value: T['TValue'], offset: number) {
        this._getValues(offset);
        return super.writeValue(value, offset);
    }
    protected _updateBytesUsed(offset: number, length: number) {
        this._bytesUsed += this.BYTES_PER_ELEMENT;
        return super._updateBytesUsed(offset, length);
    }
}

export abstract class FlatListBuilder<T extends Utf8 | Binary = any, TNull = any> extends Builder<T, TNull> {
    protected _values?: Map<number, undefined | Uint8Array>;
    constructor(options: BuilderOptions<T, TNull>) {
        super(options);
        this.valueOffsets = new Int32Array(0);
    }
    public get bytesReserved() {
        return this.valueOffsets.byteLength + this.nullBitmap.byteLength +
            roundLengthToMultipleOf64Bytes(this.valueOffsets[this.length], 1);
    }
    public writeValid(isValid: boolean, offset: number) {
        if (!super.writeValid(isValid, offset)) {
            const valueOffsets = this._getValueOffsets(offset);
            valueOffsets[offset + 1] = valueOffsets[offset];
        }
        return isValid;
    }
    public writeValue(value: Uint8Array | string, offset: number) {
        const valueOffsets = this._getValueOffsets(offset);
        valueOffsets[offset + 1] = valueOffsets[offset] + value.length;
        (this._values || (this._values = new Map())).set(offset, value);
        this._bytesUsed += value.length;
        this._bytesReserved += value.length;
    }
    protected _updateBytesUsed(offset: number, length: number) {
        this._bytesUsed += 4;
        return super._updateBytesUsed(offset, length);
    }
    public flush() {
        this.values = new Uint8Array(roundLengthToMultipleOf64Bytes(this.valueOffsets[this.length], 1));
        this._values && ((xs, n) => {
            let i = -1, x: Uint8Array | undefined;
            while (++i < n) {
                if ((x = xs.get(i)) !== undefined) {
                    super.writeValue(x, i);
                }
            }
        })(this._values, this.length);
        this._values = undefined;
        return super.flush();
    }
}

export abstract class NestedBuilder<T extends List | FixedSizeList | Map_ | Struct | Union | DenseUnion | SparseUnion, TNull = any> extends Builder<T, TNull> {
    public get bytesUsed() {
        return this.children.reduce((acc, { bytesUsed }) => acc + bytesUsed, this._bytesUsed);
    }
    public get bytesReserved() {
        return this.children.reduce((acc, { bytesReserved }) => acc + bytesReserved, this.nullBitmap.byteLength);
    }
    public getChildAt<R extends DataType = any>(index: number): Builder<R> | null {
        return this.children[index];
    }
}

/** @ignore */
function roundLengthToMultipleOf64Bytes(len: number, BYTES_PER_ELEMENT: number) {
    return ((((len * BYTES_PER_ELEMENT) + 63) & ~63)) / BYTES_PER_ELEMENT;
}

/** @ignore */
function sliceOrExtendArray<T extends TypedArray>(array: T, alignedLength = 0) {
    return array.length >= alignedLength ? array.subarray(0, alignedLength) as T
        : memcpy(new (array.constructor as TypedArrayConstructor<T>)(alignedLength), array, 0) as T;
}

/** @ignore */
function valueToCase(x: any) {
    if (typeof x !== 'bigint') {
        return valueToString(x);
    } else if (BigIntAvailable) {
        return `${valueToString(x)}n`;
    }
    return `"${valueToString(x)}"`;
}

/**
 * Dynamically compile the null values into an `isValid()` function whose
 * implementation is a switch statement. Microbenchmarks in v8 indicate
 * this approach is 25% faster than using an ES6 Map.
 * @ignore
 * @param nullValues 
 */
function compileIsValid<T extends DataType = any, TNull = any>(nullValues?: ReadonlyArray<TNull>) {

    if (!nullValues || nullValues.length <= 0) {
        return function isValid(_value: any) { return true; };
    }

    let fnBody = '';
    let noNaNs = nullValues.filter((x) => x === x);

    if (noNaNs.length > 0) {
        fnBody = `
    switch (x) {${noNaNs.map((x) => `
        case ${valueToCase(x)}:`).join('')}
            return false;
    }`;
    }

    // NaN doesn't equal anything including itself, so it doesn't work as a
    // switch case. Instead we must explicitly check for NaN before the switch.
    if (nullValues.length !== noNaNs.length) {
        fnBody = `if (x !== x) return false;\n${fnBody}`;
    }

    return new Function(`x`, `${fnBody}\nreturn true;`) as (value: T['TValue'] | TNull) => boolean;
}
