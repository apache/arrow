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

import { Vector } from './vector';
import { VirtualVector } from './virtual';

export class DictionaryVector<T> extends Vector<T> {
    readonly length: number;
    readonly data: Vector<T>;
    readonly keys: Vector<number>;
    constructor(argv: { data: Vector<T>, keys: Vector<number> }) {
        super();
        this.data = argv.data;
        this.keys = argv.keys;
        this.length = this.keys.length;
    }
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
        for (let i = -1, n = this.length; ++i < n;) {
            yield this.get(i);
        }
    }
}
