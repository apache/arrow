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

import { Data } from '../data';
import { View } from '../vector';
import { getBool, iterateBits } from '../util/bit';
import { Bool, Float16, Date_, Interval, Null } from '../type';
import { DataType, FlatType, PrimitiveType, IterableArrayLike } from '../type';

export class FlatView<T extends FlatType> implements View<T> {
    public readonly length: number;
    public readonly values: T['TArray'];
    constructor(data: Data<T>) {
        this.length = data.length;
        this.values = data.values;
    }
    public isValid(): boolean {
        return true;
    }
    public get(index: number): T['TValue'] {
        return this.values[index];
    }
    public toArray(): IterableArrayLike<T['TValue']> {
        return this.values;
    }
    public [Symbol.iterator](): IterableIterator<T['TValue']> {
        return this.values[Symbol.iterator]() as IterableIterator<T['TValue']>;
    }
}

export class NullView implements View<Null> {
    public readonly length: number;
    constructor(data: Data<Null>) {
        this.length = data.length;
    }
    public isValid(): boolean {
        return true;
    }
    public get() { return null; }
    public toArray(): IterableArrayLike<null> {
        return [...this];
    }
    public *[Symbol.iterator](): IterableIterator<null> {
        for (let index = -1, length = this.length; ++index < length;) {
            yield null;
        }
    }
}

export class BoolView extends FlatView<Bool> {
    public toArray() { return [...this]; }
    public get(index: number): boolean {
        return getBool(null, index, this.values[index >> 3], index % 8);
    }
    public [Symbol.iterator](): IterableIterator<boolean> {
        return iterateBits<boolean>(this.values, 0, this.length, this.values, getBool);
    }
}

export class ValidityView<T extends DataType> implements View<T> {
    protected view: View<T>;
    protected length: number;
    protected nullBitmap: Uint8Array;
    constructor(data: Data<T>, view: View<T>) {
        this.view = view;
        this.length = data.length;
        this.nullBitmap = data.nullBitmap!;
    }
    public toArray(): IterableArrayLike<T['TValue'] | null> { return [...this]; }
    public isValid(index: number): boolean {
        return getBool(null, index, this.nullBitmap[index >> 3], index % 8);
    }
    public get(index: number): T['TValue'] | null {
        return getNullable(this.view, index, this.nullBitmap[index >> 3], index % 8);
    }
    public [Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        return iterateBits<T['TValue'] | null>(this.nullBitmap, 0, this.length, this.view, getNullable);
    }
}

export class PrimitiveView<T extends PrimitiveType> extends FlatView<T> {
    public readonly size: number;
    public readonly ArrayType: T['ArrayType'];
    constructor(data: Data<T>, size?: number) {
        super(data);
        this.size = size || 1;
        this.ArrayType = data.type.ArrayType;
    }
    protected getValue(values: T['TArray'], index: number): T['TValue'] {
        return values[index];
    }
    public get(index: number): T['TValue'] {
        return this.getValue(this.values, this.size * index);
    }
    public toArray(): IterableArrayLike<T['TValue']> {
        return this.size <= 1 ? this.values : new this.ArrayType(this as Iterable<number>);
    }
    public [Symbol.iterator](): IterableIterator<T['TValue']> {
        return this.size <= 1 ? this.values[Symbol.iterator]() : iterateWithStride(this.values, 0, this.length, this.size, this.getValue);
    }
}

export class FixedSizeView<T extends PrimitiveType> extends PrimitiveView<T> {
    constructor(data: Data<T>, size: number) {
        super(data, size);
    }
    protected getValue(values: T['TArray'], index: number): T['TValue'] {
        return values.subarray(index, index + this.size);
    }
}

export class Float16View extends PrimitiveView<Float16> {
    protected getValue(values: Uint16Array, index: number): number {
        return Math.min((values[index] -  32767) / 32767, 1);
    }
}

export class DateDayView extends PrimitiveView<Date_> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number): Date {
        return epochDaysToDate(values, index);
    }
}

export class DateMillisecondView extends FixedSizeView<Date_> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number): Date {
        return epochMillisecondsLongToDate(values, index);
    }
}

export class IntervalYearMonthView extends PrimitiveView<Interval> {
    protected getValue(values: Int32Array, index: number): Int32Array {
        const interval = values[index];
        return new Int32Array([interval / 12, /* years */ interval % 12  /* months */]);
    }
}

export function getNullable<T extends DataType>(view: View<T>, index: number, byte: number, bit: number) {
    return getBool(view, index, byte, bit) ? view.get(index) : null;
}

export function* iterateWithStride<T extends PrimitiveType>(values: T['TArray'], begin: number, length: number, stride: number, get: (values: T['TArray'], index: number) => T['TValue']) {
    for (let index = -1, offset = begin - stride; ++index < length;) {
        yield get(values, offset += stride);
    }
}

export function epochSecondsToMs(data: Int32Array, index: number) { return 1000 * data[index]; }
export function epochDaysToMs(data: Int32Array, index: number) { return 86400000 * data[index]; }
export function epochMillisecondsLongToMs(data: Int32Array, index: number) { return 4294967296 * (data[index + 1]) + data[index]; }
export function epochMicrosecondsLongToMs(data: Int32Array, index: number) { return 4294967296 * (data[index + 1] / 1000) + (data[index] / 1000); }
export function epochNanosecondsLongToMs(data: Int32Array, index: number) { return 4294967296 * (data[index + 1] / 1000000) + (data[index] / 1000000); }

export function epochMillisecondsToDate(epochMs: number) { return new Date(epochMs); }
export function epochDaysToDate(data: Int32Array, index: number) { return epochMillisecondsToDate(epochDaysToMs(data, index)); }
export function epochSecondsToDate(data: Int32Array, index: number) { return epochMillisecondsToDate(epochSecondsToMs(data, index)); }
export function epochNanosecondsLongToDate(data: Int32Array, index: number) { return epochMillisecondsToDate(epochNanosecondsLongToMs(data, index)); }
export function epochMillisecondsLongToDate(data: Int32Array, index: number) { return epochMillisecondsToDate(epochMillisecondsLongToMs(data, index)); }
