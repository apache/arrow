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

import { Vector, Struct } from '../types';
import { VirtualVector } from '../vector/virtual';

export class Row<TKey extends string | number> extends Vector {
    readonly row: number;
    readonly table: Struct<TKey>;
    [Symbol.toStringTag]() { return 'Row'; }
    constructor(table: Struct<TKey>, row: number) {
        super();
        this.row = row;
        this.table = table;
    }
    get length() { return this.table.length; }
    get<T = any>(key: TKey) {
        const col = this.table.col(key as TKey);
        return col ? col.get(this.row) as T : null;
    }
    *[Symbol.iterator]() {
        const { row } = this;
        for (const col of this.table.columns) {
            yield col ? col.get(row) : null;
        }
    }
    concat(...rows: Row<TKey>[]): Vector {
        return new VirtualVector(Array, this, ...rows as any[]);
    }
    toArray() { return [...this]; }
    toJSON() { return this.toArray(); }
    toString() { return `Row [${this.length}]`; }
    toObject(): Record<string, any> {
        const { row } = this, map = Object.create(null);
        for (const col of this.table.columns) {
            if (col && col.name) {
                map[col.name] = col.get(row);
            }
        }
        return map;
    }
}
