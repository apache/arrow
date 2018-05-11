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
import { View, Vector } from '../vector';
import { NestedView } from './nested';
import { DataType, IterableArrayLike } from '../type';
import { getBool, setBool, iterateBits } from '../util/bit';

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
    public get size(): number {
        return (this.view as any).size || 1;
    }
    public clone(data: Data<T>): this {
        return new ValidityView(data, this.view.clone(data)) as this;
    }
    public toArray(): IterableArrayLike<T['TValue'] | null> {
        return [...this];
    }
    public indexOf(search: T['TValue']) {
        let index = 0;
        for (let value of this) {
            if (value === search) { return index; }
            ++index;
        }

        return -1;
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
    public getChildAt<R extends DataType = DataType>(index: number): Vector<R> | null {
        return (this.view as NestedView<any>).getChildAt<R>(index);
    }
    public [Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        return iterateBits<T['TValue'] | null>(this.nullBitmap, this.offset, this.length, this.view, this.getNullable);
    }
    protected getNullable(view: View<T>, index: number, byte: number, bit: number) {
        return getBool(view, index, byte, bit) ? view.get(index) : null;
    }
}
