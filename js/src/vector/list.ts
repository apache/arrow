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

import { List } from './types';
import { Vector } from './vector';
import { VirtualVector } from './virtual';

export class BinaryVector extends Vector<Uint8Array> {
    readonly data: Uint8Array;
    readonly offsets: Int32Array;
    constructor(argv: { offsets: Int32Array, data: Uint8Array }) {
        super();
        this.data = argv.data;
        this.offsets = argv.offsets;
    }
    get(index: number) {
        return this.data.subarray(this.offsets[index], this.offsets[index + 1]);
    }
    concat(...vectors: Vector<Uint8Array>[]): Vector<Uint8Array> {
        return new VirtualVector(Array, this, ...vectors);
    }
}

export class ListVector<T> extends Vector<T[]> {
    readonly offsets: Int32Array;
    readonly values: Vector<T>;
    constructor(argv: { offsets: Int32Array, values: Vector<T> }) {
        super();
        this.values = argv.values;
        this.offsets = argv.offsets;
    }
    get(index: number) {
        const { offsets, values } = this;
        const from = offsets[index];
        const xs = new Array(offsets[index + 1] - from);
        for (let i = -1, n = xs.length; ++i < n;) {
            xs[i] = values.get(i + from);
        }
        return xs;
    }
    concat(...vectors: Vector<T[]>[]): Vector<T[]> {
        return new VirtualVector(Array, this, ...vectors);
    }
}

export class FixedSizeListVector<T, TArray extends List<T>> extends Vector<TArray> {
    readonly size: number;
    readonly values: Vector<T>;
    constructor(argv: { size: number, values: Vector<T> }) {
        super();
        this.size = argv.size;
        this.values = argv.values;
    }
    get(index: number) {
        return this.values.slice<TArray>(this.size * index, this.size * (index + 1));
    }
    concat(...vectors: Vector<TArray>[]): Vector<TArray> {
        return new VirtualVector(Array, this, ...vectors);
    }
}
