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

import { List, Vector } from './types';
import { VirtualVector } from './vector/virtual';

export class ListVector<T, TArray extends List<T>> extends Vector<TArray> {
    readonly offsets: Int32Array;
    readonly values: Vector<T>;
    constructor(argv: { offsets: Int32Array, values: Vector<T> }) {
        super();
        this.values = argv.values;
        this.offsets = argv.offsets;
    }
    get(index: number) {
        return this.values.slice<TArray>(this.offsets[index], this.offsets[index + 1]);
    }
    concat(...vectors: Vector<TArray>[]): Vector<TArray> {
        return new VirtualVector(Array, this, ...vectors);
    }
}