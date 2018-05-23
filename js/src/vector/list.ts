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
import { View, Vector, createVector } from '../vector';
import { TextEncoder, TextDecoder } from 'text-encoding-utf-8';
import { List, Binary, Utf8, FixedSizeList, FlatListType } from '../type';
import { ListType, SingleNestedType, DataType, IterableArrayLike } from '../type';

export const encodeUtf8 = ((encoder) =>
    encoder.encode.bind(encoder) as (input?: string) => Uint8Array
)(new TextEncoder('utf-8'));

export const decodeUtf8 = ((decoder) =>
    decoder.decode.bind(decoder) as (input?: ArrayBufferLike | ArrayBufferView) => string
)(new TextDecoder('utf-8'));

export abstract class ListViewBase<T extends (FlatListType | SingleNestedType)> implements View<T> {
    public length: number;
    public values: T['TArray'];
    public valueOffsets?: Int32Array;
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
    public toArray(): IterableArrayLike<T['TValue']> {
        return [...this];
    }
    public get(index: number): T['TValue'] {
        return this.getList(this.values, index, this.valueOffsets);
    }
    public set(index: number, value: T['TValue']): void {
        return this.setList(this.values, index, value, this.valueOffsets);
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const get = this.getList, length = this.length;
        const values = this.values, valueOffsets = this.valueOffsets;
        for (let index = -1; ++index < length;) {
            yield get(values, index, valueOffsets);
        }
    }
    public indexOf(search: T['TValue']) {
        let index = 0;
        for (let value of this) {
            if (value === search) { return index; }
            ++index;
        }

        return -1;
    }
    protected abstract getList(values: T['TArray'], index: number, valueOffsets?: Int32Array): T['TValue'];
    protected abstract setList(values: T['TArray'], index: number, value: T['TValue'], valueOffsets?: Int32Array): void;
}

export abstract class VariableListViewBase<T extends (ListType | FlatListType)> extends ListViewBase<T> {
    constructor(data: Data<T>) {
        super(data);
        this.length = data.length;
        this.valueOffsets = data.valueOffsets;
    }
}

export class ListView<T extends DataType> extends VariableListViewBase<List<T>> {
    public values: Vector<T>;
    constructor(data: Data<T>) {
        super(data);
        this.values = createVector(data.values);
    }
    public getChildAt<R extends T = T>(index: number): Vector<R> | null {
        return index === 0 ? (this.values as Vector<R>) : null;
    }
    protected getList(values: Vector<T>, index: number, valueOffsets: Int32Array) {
        return values.slice(valueOffsets[index], valueOffsets[index + 1]) as Vector<T>;
    }
    protected setList(values: Vector<T>, index: number, value: Vector<T>, valueOffsets: Int32Array): void {
        let idx = -1;
        let offset = valueOffsets[index];
        let end = Math.min(value.length, valueOffsets[index + 1] - offset);
        while (offset < end) {
            values.set(offset++, value.get(++idx));
        }
    }
}

export class FixedSizeListView<T extends DataType> extends ListViewBase<FixedSizeList<T>> {
    public size: number;
    public values: Vector<T>;
    constructor(data: Data<FixedSizeList<T>>) {
        super(data);
        this.size = data.type.listSize;
        this.values = createVector(data.values);
    }
    public getChildAt<R extends T = T>(index: number): Vector<R> | null {
        return index === 0 ? (this.values as Vector<R>) : null;
    }
    protected getList(values: Vector<T>, index: number) {
        const size = this.size;
        return values.slice(index *= size, index + size) as Vector<T>;
    }
    protected setList(values: Vector<T>, index: number, value: Vector<T>): void {
        let size = this.size;
        for (let idx = -1, offset = index * size; ++idx < size;) {
            values.set(offset + idx, value.get(++idx));
        }
    }
}

export class BinaryView extends VariableListViewBase<Binary> {
    protected getList(values: Uint8Array, index: number, valueOffsets: Int32Array) {
        return values.subarray(valueOffsets[index], valueOffsets[index + 1]);
    }
    protected setList(values: Uint8Array, index: number, value: Uint8Array, valueOffsets: Int32Array): void {
        const offset = valueOffsets[index];
        values.set(value.subarray(0, valueOffsets[index + 1] - offset), offset);
    }
}

export class Utf8View extends VariableListViewBase<Utf8> {
    protected getList(values: Uint8Array, index: number, valueOffsets: Int32Array) {
        return decodeUtf8(values.subarray(valueOffsets[index], valueOffsets[index + 1]));
    }
    protected setList(values: Uint8Array, index: number, value: string, valueOffsets: Int32Array): void {
        const offset = valueOffsets[index];
        values.set(encodeUtf8(value).subarray(0, valueOffsets[index + 1] - offset), offset);
    }
}
