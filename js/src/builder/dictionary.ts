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
import { Vector } from '../vector';
import { IntBuilder } from './int';
import { Dictionary, DataType } from '../type';
import { Builder, BuilderOptions } from './base';

type DictionaryHashFunction = (x: any) => string | number;

export interface DictionaryBuilderOptions<T extends DataType = any, TNull = any> extends BuilderOptions<T, TNull> {
    dictionaryHashFunction?: DictionaryHashFunction;
}

export class DictionaryBuilder<T extends Dictionary, TNull = any> extends Builder<T, TNull> {

    protected _hash: DictionaryHashFunction;
    protected _codes = Object.create(null);
    public readonly indices: IntBuilder<T['indices']>;
    public readonly dictionary: Builder<T['dictionary']>;

    constructor(options: DictionaryBuilderOptions<T, TNull>) {
        super(options);
        const { type, nullValues } = options;
        this._hash = options.dictionaryHashFunction || defaultHashFunction;
        this.indices = Builder.new({ type: type.indices, nullValues }) as IntBuilder<T['indices']>;
        this.dictionary = Builder.new({ type: type.dictionary, nullValues: [] }) as Builder<T['dictionary']>;
    }
    public get values() { return this.indices && this.indices.values; }
    public get nullBitmap() { return this.indices && this.indices.nullBitmap; }
    public set values(values: T['TArray']) { this.indices && (this.indices.values = values); }
    public set nullBitmap(nullBitmap: Uint8Array) { this.indices && (this.indices.nullBitmap = nullBitmap); }
    public get bytesUsed() {
        return this.indices.bytesUsed;
    }
    public get bytesReserved() {
        return this.indices.bytesReserved;
    }
    public setHashFunction(hash: DictionaryHashFunction) {
        this._hash = hash;
        return this;
    }
    public reset() {
        this.length = 0;
        this.indices.reset();
        return this;
    }
    public flush() {
        const indices = this.indices;
        const data = indices.flush().clone(this.type);
        this.length = indices.length;
        return data;
    }
    public finish() {
        this.type.dictionaryVector = Vector.new(this.dictionary.finish().flush());
        return super.finish();
    }
    /** @ignore */
    public set(offset: number, value: T['TValue'] | TNull): void {
        super.set(offset, value);
        this.indices.length = this.length;
    }
    public write(value: any) {
        this.indices.length = super.write(value).length;
        return this;
    }
    public writeValid(isValid: boolean, index: number) {
        return this.indices.writeValid(isValid, index);
    }
    // @ts-ignore
    protected _updateBytesUsed(offset: number, length: number) {
        const indices = this.indices;
        indices.length = length;
        indices._updateBytesUsed(offset, length);
        return this;
    }
    public writeValue(value: T['TValue'], index: number) {
        let code: number | void;
        let codes = this._codes;
        let key = this._hash(value);
        if ((code = codes[key]) === undefined) {
            codes[key] = code = this.dictionary.write(value).length - 1;
        }
        return this.indices.writeValue(code, index);
    }
    public *writeAll(source: Iterable<any>, chunkLength = Infinity) {
        const chunks = [] as Data<T>[];
        for (const chunk of super.writeAll(source, chunkLength)) {
            chunks.push(chunk);
        }
        yield* chunks;
    }
    public async *writeAllAsync(source: Iterable<any> | AsyncIterable<any>, chunkLength = Infinity) {
        const chunks = [] as Data<T>[];
        for await (const chunk of super.writeAllAsync(source, chunkLength)) {
            chunks.push(chunk);
        }
        yield* chunks;
    }
}

function defaultHashFunction(val: any) {
    typeof val === 'string' || (val = `${val}`);
    let h = 6, y = 9 * 9, i = val.length;
    while (i > 0) {
        h = Math.imul(h ^ val.charCodeAt(--i), y);
    }
    return (h ^ h >>> 9) as any;
}
