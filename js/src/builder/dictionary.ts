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

import { Vector } from '../vector';
import { IntBuilder } from './int';
import { Dictionary, DataType } from '../type';
import { Builder, BuilderOptions } from '../builder';

type DictionaryHashFunction = (x: any) => string | number;

export interface DictionaryBuilderOptions<T extends DataType = any, TNull = any> extends BuilderOptions<T, TNull> {
    dictionaryHashFunction?: DictionaryHashFunction;
}

/** @ignore */
export class DictionaryBuilder<T extends Dictionary, TNull = any> extends Builder<T, TNull> {

    protected _codes = Object.create(null);
    public readonly indices: IntBuilder<T['indices']>;
    public readonly dictionary: Builder<T['dictionary']>;

    constructor({ 'type': type, 'nullValues': nulls, 'dictionaryHashFunction': hashFn }: DictionaryBuilderOptions<T, TNull>) {
        super({ type });
        this._nulls = <any> null;
        this.indices = Builder.new({ 'type': this.type.indices, 'nullValues': nulls }) as IntBuilder<T['indices']>;
        this.dictionary = Builder.new({ 'type': this.type.dictionary, 'nullValues': null }) as Builder<T['dictionary']>;
        if (typeof hashFn === 'function') {
            this.valueToKey = hashFn;
        }
    }

    public get values() { return this.indices.values; }
    public get nullCount() { return this.indices.nullCount; }
    public get nullBitmap() { return this.indices.nullBitmap; }
    public get byteLength() { return this.indices.byteLength; }
    public get reservedLength() { return this.indices.reservedLength; }
    public get reservedByteLength() { return this.indices.reservedByteLength; }
    public isValid(value: T['TValue'] | TNull) { return this.indices.isValid(value); }
    public setValid(index: number, valid: boolean) {
        const indices = this.indices;
        valid = indices.setValid(index, valid);
        this.length = indices.length;
        return valid;
    }
    public setValue(index: number, value: T['TValue']) {
        let keysToCodesMap = this._codes;
        let key = this.valueToKey(value);
        let idx = keysToCodesMap[key];
        if (idx === undefined) {
            keysToCodesMap[key] = idx = this.dictionary.append(value).length - 1;
        }
        return this.indices.setValue(index, idx);
    }
    public flush() {
        const chunk = this.indices.flush().clone(this.type);
        this.clear();
        return chunk;
    }
    public finish() {
        this.type.dictionaryVector = Vector.new(this.dictionary.finish().flush());
        return super.finish();
    }
    public clear() {
        this.indices.clear();
        return super.clear();
    }
    public valueToKey(val: any): string | number {
        return typeof val === 'string' ? val : `${val}`;
    }
}
