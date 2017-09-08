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

export class DictionaryVector<T> extends Vector<T> {
    protected data: Vector<T>;
    protected keys: Vector<number>;
    constructor(index: Vector<number>, dictionary: Vector<T>) {
        super();
        this.keys = index;
        this.data = dictionary;
        this.length = index && index.length || 0;
    }
    index(index: number) {
        return this.keys.get(index);
    }
    value(index: number) {
        return this.data.get(index);
    }
    get(index: number) {
        return this.value(this.index(index));
    }
    concat(vector: DictionaryVector<T>) {
        return DictionaryVector.from(this,
            this.length + vector.length,
            this.keys.concat(vector.keys),
            this.data
        );
    }
    *[Symbol.iterator]() {
        let { data } = this;
        for (const loc of this.keys) {
            yield data.get(loc);
        }
    }
}
