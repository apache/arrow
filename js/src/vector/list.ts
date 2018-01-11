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
import { Vector, View } from '../vector';
import { TextDecoder } from 'text-encoding-utf-8';
import { List, Binary, Utf8, FixedSizeList } from '../type';
import { ListType, DataType, IterableArrayLike } from '../type';

export const decodeUtf8 = ((decoder) =>
    decoder.decode.bind(decoder) as (input?: ArrayBufferLike | ArrayBufferView) => string
)(new TextDecoder('utf-8'));

export abstract class ListViewBase<T extends ListType> implements View<T> {
    protected length: number;
    protected values: T['TArray'];
    protected valueOffsets?: Int32Array;
    constructor(data: Data<T>) {
        this.length = data.length;
        this.values = data.values;
        this.valueOffsets = data.valueOffsets;
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
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const get = this.getList, length = this.length;
        const values = this.values, valueOffsets = this.valueOffsets;
        for (let index = -1; ++index < length;) {
            yield get(values, index, valueOffsets);
        }
    }
    protected abstract getList(values: T['TArray'], index: number, valueOffsets?: Int32Array): T['TValue'];
}

export class ListView<T extends DataType> extends ListViewBase<List<T>> {
    protected getList(values: Vector<T>, index: number, valueOffsets: Int32Array) {
        return values.slice(valueOffsets[index], valueOffsets[index + 1]) as Vector<T>;
    }
}

export class FixedSizeListView<T extends DataType> extends ListViewBase<FixedSizeList<T>> {
    public size: number;
    constructor(data: Data<FixedSizeList<T>>) {
        super(data);
        this.size = data.type.listSize;
    }
    protected getList(values: Vector<T>, index: number) {
        return values.slice(index, index + this.size) as Vector<T>;
    }
}

export class BinaryView extends ListViewBase<Binary> {
    protected getList(values: Uint8Array, index: number, valueOffsets: Int32Array) {
        return values.subarray(valueOffsets[index], valueOffsets[index + 1]);
    }
}

export class Utf8View extends ListViewBase<Utf8> {
    protected getList(values: Uint8Array, index: number, valueOffsets: Int32Array) {
        return decodeUtf8(values.subarray(valueOffsets[index], valueOffsets[index + 1]));
    }
}
