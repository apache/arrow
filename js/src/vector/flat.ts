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
import { getBool, setBool, iterateBits } from '../util/bit';
import { Bool, Float16, Date_, Interval, Null, Int32, Timestamp } from '../type';
import { DataType, FlatType, PrimitiveType, IterableArrayLike } from '../type';

export class FlatView<T extends FlatType> implements View<T> {
    public length: number;
    public values: T['TArray'];
    constructor(data: Data<T>) {
        this.length = data.length;
        this.values = data.values;
    }
    public clone(data: Data<T>): this {
        return new (<any> this.constructor)(data) as this;
    }
    public isValid(): boolean {
        return true;
    }
    public get(index: number): T['TValue'] {
        return this.values[index];
    }
    public set(index: number, value: T['TValue']): void {
        return this.values[index] = value;
    }
    public toArray(): IterableArrayLike<T['TValue']> {
        return this.values.subarray(0, this.length);
    }
    public [Symbol.iterator](): IterableIterator<T['TValue']> {
        return this.values.subarray(0, this.length)[Symbol.iterator]() as IterableIterator<T['TValue']>;
    }
}

export class NullView implements View<Null> {
    public length: number;
    constructor(data: Data<Null>) {
        this.length = data.length;
    }
    public clone(data: Data<Null>): this {
        return new (<any> this.constructor)(data) as this;
    }
    public isValid(): boolean {
        return true;
    }
    public set(): void {}
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
    protected offset: number;
    constructor(data: Data<Bool>) {
        super(data);
        this.offset = data.offset;
    }
    public toArray() { return [...this]; }
    public get(index: number): boolean {
        const boolBitIndex = this.offset + index;
        return getBool(null, index, this.values[boolBitIndex >> 3], boolBitIndex % 8);
    }
    public set(index: number, value: boolean): void {
        setBool(this.values, this.offset + index, value);
    }
    public [Symbol.iterator](): IterableIterator<boolean> {
        return iterateBits<boolean>(this.values, this.offset, this.length, this.values, getBool);
    }
}

export class ValidityView<T extends DataType> implements View<T> {
    protected view: View<T>;
    protected length: number;
    protected offset: number;
    protected nullBitmap: Uint8Array;
    constructor(data: Data<T>, view: View<T>) {
        this.view = view;
        this.length = data.length;
        this.offset = data.offset;
        this.nullBitmap = data.nullBitmap!;
    }
    public clone(data: Data<T>): this {
        return new ValidityView(data, this.view.clone(data)) as this;
    }
    public toArray(): IterableArrayLike<T['TValue'] | null> {
        return [...this];
    }
    public isValid(index: number): boolean {
        const nullBitIndex = this.offset + index;
        return getBool(null, index, this.nullBitmap[nullBitIndex >> 3], nullBitIndex % 8);
    }
    public get(index: number): T['TValue'] | null {
        const nullBitIndex = this.offset + index;
        return this.getNullable(this.view, index, this.nullBitmap[nullBitIndex >> 3], nullBitIndex % 8);
    }
    public set(index: number, value: T['TValue'] | null): void {
        if (setBool(this.nullBitmap, this.offset + index, value != null)) {
            this.view.set(index, value);
        }
    }
    public [Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        return iterateBits<T['TValue'] | null>(this.nullBitmap, this.offset, this.length, this.view, this.getNullable);
    }
    protected getNullable(view: View<T>, index: number, byte: number, bit: number) {
        return getBool(view, index, byte, bit) ? view.get(index) : null;
    }
}

export class PrimitiveView<T extends PrimitiveType> extends FlatView<T> {
    public size: number;
    public ArrayType: T['ArrayType'];
    constructor(data: Data<T>, size?: number) {
        super(data);
        this.size = size || 1;
        this.ArrayType = data.type.ArrayType;
    }
    public clone(data: Data<T>): this {
        return new (<any> this.constructor)(data, this.size) as this;
    }
    protected getValue(values: T['TArray'], index: number, size: number): T['TValue'] {
        return values[index * size];
    }
    protected setValue(values: T['TArray'], index: number, size: number, value: T['TValue']): void {
        values[index * size] = value;
    }
    public get(index: number): T['TValue'] {
        return this.getValue(this.values, index, this.size);
    }
    public set(index: number, value: T['TValue']): void {
        return this.setValue(this.values, index, this.size, value);
    }
    public toArray(): IterableArrayLike<T['TValue']> {
        return this.size > 1 ?
            new this.ArrayType(this) :
            this.values.subarray(0, this.length);
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const get = this.getValue;
        const { size, values, length } = this;
        for (let index = -1; ++index < length;) {
            yield get(values, index, size);
        }
    }
}

export class FixedSizeView<T extends PrimitiveType> extends PrimitiveView<T> {
    public toArray(): IterableArrayLike<T['TValue']> {
        return this.values;
    }
    protected getValue(values: T['TArray'], index: number, size: number): T['TValue'] {
        return values.subarray(index * size, index * size + size);
    }
    protected setValue(values: T['TArray'], index: number, size: number, value: T['TValue']): void {
        values.set((value as T['TArray']).subarray(0, size), index * size);
    }
}

export class Float16View extends PrimitiveView<Float16> {
    public toArray() { return new Float32Array(this); }
    protected getValue(values: Uint16Array, index: number, size: number): number {
        return (values[index * size] - 32767) / 32767;
    }
    protected setValue(values: Uint16Array, index: number, size: number, value: number): void {
        values[index * size] = (value * 32767) + 32767;
    }
}

export class DateDayView extends PrimitiveView<Date_> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): Date {
        return epochDaysToDate(values, index * size);
    }
    protected setValue(values: Int32Array, index: number, size: number, value: Date): void {
        values[index * size] = value.valueOf() / 86400000;
    }
}

export class DateMillisecondView extends FixedSizeView<Date_> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): Date {
        return epochMillisecondsLongToDate(values, index * size);
    }
    protected setValue(values: Int32Array, index: number, size: number, value: Date): void {
        const epochMs = value.valueOf();
        values[index * size] = (epochMs % 4294967296) | 0;
        values[index * size + size] = (epochMs / 4294967296) | 0;
    }
}

export class TimestampDayView extends PrimitiveView<Timestamp> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): number {
        return epochDaysToMs(values, index * size);
    }
    protected setValue(values: Int32Array, index: number, size: number, epochMs: number): void {
        values[index * size] = (epochMs / 86400000) | 0;
    }
}

export class TimestampSecondView extends PrimitiveView<Timestamp> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): number {
        return epochSecondsToMs(values, index * size);
    }
    protected setValue(values: Int32Array, index: number, size: number, epochMs: number): void {
        values[index * size] = (epochMs / 1000) | 0;
    }
}

export class TimestampMillisecondView extends PrimitiveView<Timestamp> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): number {
        return epochMillisecondsLongToMs(values, index * size);
    }
    protected setValue(values: Int32Array, index: number, size: number, epochMs: number): void {
        values[index * size] = (epochMs % 4294967296) | 0;
        values[index * size + size] = (epochMs / 4294967296) | 0;
    }
}

export class TimestampMicrosecondView extends PrimitiveView<Timestamp> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): number {
        return epochMicrosecondsLongToMs(values, index * size);
    }
    protected setValue(values: Int32Array, index: number, size: number, epochMs: number): void {
        values[index * size] = ((epochMs / 1000) % 4294967296) | 0;
        values[index * size + size] = ((epochMs / 1000) / 4294967296) | 0;
    }
}

export class TimestampNanosecondView extends PrimitiveView<Timestamp> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): number {
        return epochNanosecondsLongToMs(values, index * size);
    }
    protected setValue(values: Int32Array, index: number, size: number, epochMs: number): void {
        values[index * size] = ((epochMs / 1000000) % 4294967296) | 0;
        values[index * size + size] = ((epochMs / 1000000) / 4294967296) | 0;
    }
}

export class IntervalYearMonthView extends PrimitiveView<Interval> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): Int32Array {
        const interval = values[index * size];
        return new Int32Array([interval / 12, /* years */ interval % 12  /* months */]);
    }
    protected setValue(values: Int32Array, index: number, size: number, value: Int32Array): void {
        values[index * size] = (value[0] * 12) + (value[1] % 12);
    }
}

export class IntervalYearView extends PrimitiveView<Int32> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): number {
        return values[index * size] / 12;
    }
    protected setValue(values: Int32Array, index: number, size: number, value: number): void {
        values[index * size] = (value * 12) + (values[index * size] % 12);
    }
}

export class IntervalMonthView extends PrimitiveView<Int32> {
    public toArray() { return [...this]; }
    protected getValue(values: Int32Array, index: number, size: number): number {
        return values[index * size] % 12;
    }
    protected setValue(values: Int32Array, index: number, size: number, value: number): void {
        values[index * size] = (values[index * size] * 12) + (value % 12);
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
