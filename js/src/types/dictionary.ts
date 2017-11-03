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

import { Vector, Column } from './types';
import { VirtualVector } from './vector/virtual';

export interface DictionaryVector<T> extends Vector<T> {
    getValue(key: number): T;
    getKey(index: number): number;
}

export class DictionaryVector<T> extends Vector<T> implements Column<T>, DictionaryVector<T> {
    readonly data: Vector<T>;
    readonly keys: Column<number>;
    constructor(argv: { data: Vector<T>, keys: Vector<number> }) {
        super();
        this.data = argv.data;
        this.keys = argv.keys as Column<number>;
    }
    get name () { return this.keys.name; }
    get type () { return this.keys.type; }
    get length () { return this.keys.length; }
    get metadata () { return this.keys.metadata; }
    get nullable () { return this.keys.nullable; }
    get nullCount () { return this.keys.nullCount; }
    get(index: number) {
        return this.getValue(this.getKey(index)!);
    }
    getKey(index: number) {
        return this.keys.get(index);
    }
    getValue(key: number) {
        return this.data.get(key);
    }
    concat(...vectors: Vector<T>[]): Vector<T> {
        return new VirtualVector(Array, this, ...vectors);
    }
    *[Symbol.iterator]() {
        const { data, keys } = this;
        for (let i = -1, n = keys.length; ++i < n;) {
            yield data.get(keys.get(i)!);
        }
    }
}
