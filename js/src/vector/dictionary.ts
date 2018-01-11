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

import { View, Vector } from '../vector';
import { IterableArrayLike, DataType, Int } from '../type';

export class DictionaryView<T extends DataType> implements View<T> {
    public indicies: Vector<Int>;
    public dictionary: Vector<T>;
    constructor(dictionary: Vector<T>, indicies: Vector<Int>) {
        this.indicies = indicies;
        this.dictionary = dictionary;
    }
    public isValid(index: number): boolean {
        return this.dictionary.isValid(index);
    }
    public get(index: number): T['TValue'] {
        return this.dictionary.get(this.indicies.get(index));
    }
    public toArray(): IterableArrayLike<T['TValue']> {
        return [...this];
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const values = this.dictionary, indicies = this.indicies;
        for (let index = -1, n = indicies.length; ++index < n;) {
            yield values.get(indicies.get(index));
        }
    }
}
