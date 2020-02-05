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
import { BaseVector } from './base';
import { VectorType as V } from '../interfaces';
import { VectorBuilderOptions } from './index';
import { vectorFromValuesWithType } from './index';
import { VectorBuilderOptionsAsync } from './index';
import { DataType, Dictionary, TKeys } from '../type';

/** @ignore */
type FromArgs<T extends DataType = any, TKey extends TKeys = TKeys> = [Vector<T>, TKey, ArrayLike<number> | TKey['TArray']];

/** @ignore */
export class DictionaryVector<T extends DataType = any, TKey extends TKeys = TKeys> extends BaseVector<Dictionary<T, TKey>> {
    public static from<T extends DataType = any, TKey extends TKeys = TKeys>(...args: FromArgs<T, TKey>): V<Dictionary<T, TKey>>;
    public static from<T extends DataType = any, TKey extends TKeys = TKeys>(input: VectorBuilderOptions<Dictionary<T, TKey>>): Vector<Dictionary<T, TKey>>;
    public static from<T extends DataType = any, TKey extends TKeys = TKeys>(input: VectorBuilderOptionsAsync<Dictionary<T, TKey>>): Promise<Vector<Dictionary<T, TKey>>>;
    /** @nocollapse */
    public static from<T extends DataType = any, TKey extends TKeys = TKeys>(...args: any[]) {
        if (args.length === 3) {
            const [values, indices, keys] = args as FromArgs<T, TKey>;
            const type = new Dictionary(values.type, indices, null, null);
            return Vector.new(Data.Dictionary(type, 0, keys.length, 0, null, keys, values));
        }
        return vectorFromValuesWithType(() => args[0].type, args[0]);
    }

    constructor(data: Data<Dictionary<T, TKey>>) {
        super(data);
        this.indices = Vector.new(data.clone(this.type.indices));
    }

    public readonly indices: V<TKey>;

    public get dictionary() { return <Vector<T>> this.data.dictionary; }
    public reverseLookup(value: T) { return this.dictionary.indexOf(value); }
    public getKey(idx: number): TKey['TValue'] | null { return this.indices.get(idx); }
    public getValue(key: number): T['TValue'] | null { return this.dictionary.get(key); }
    public setKey(idx: number, key: TKey['TValue'] | null) { return this.indices.set(idx, key); }
    public setValue(key: number, value: T['TValue'] | null) { return this.dictionary.set(key, value); }
}

(DictionaryVector.prototype as any).indices = null;
