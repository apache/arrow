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
import { StructVector, StructRow } from './struct';
import { readVectors, readVectorsAsync } from '../reader/arrow';
import { readJSON } from '../reader/json';
import { parse } from 'json-bignum';

function concatVectors(tableVectors: Vector<any>[], batchVectors: Vector<any>[]) {
    return tableVectors.length === 0 ? batchVectors : batchVectors.map((vec, i, _vs, col = tableVectors[i]) =>
        vec && col && col.concat(vec) || col || vec
    ) as Vector<any>[];
}

export class Table<T> extends StructVector<T> {
    static from(buffersOrJSON?: Iterable<Uint8Array | Buffer | string> | object | string) {
        let input: any = buffersOrJSON;
        let columns: Vector<any>[] = [];
        let batches: Iterable<Vector[]>;
        if (typeof input === 'string') {
            try { input = parse(input); }
            catch (e) { input = buffersOrJSON; }
        }
        if (!input || typeof input !== 'object') {
            batches = (typeof input === 'string') ? readVectors([input]) : [];
        } else {
            batches = (typeof input[Symbol.iterator] === 'function') ? readVectors(input) : readJSON(input);
        }
        for (let vectors of batches) {
            columns = concatVectors(columns, vectors);
        }
        return new Table({ columns });
    }
    static async fromAsync(buffers?: AsyncIterable<Uint8Array | Buffer | string>) {
        let columns: Vector<any>[] = [];
        if (buffers) {
            for await (let vectors of readVectorsAsync(buffers)) {
                columns = columns = concatVectors(columns, vectors);
            }
        }
        return new Table({ columns });
    }
    readonly length: number;
    constructor(argv: { columns: Vector<any>[] }) {
        super(argv);
        this.length = Math.max(...this.columns.map((col) => col.length)) | 0;
    }
    get(index: number): TableRow<T> {
        return new TableRow(this, index);
    }
}

export class TableRow<T> extends StructRow<T> {
    toString() {
        return this.toArray().map((x) => JSON.stringify(x)).join(', ');
    }
}
