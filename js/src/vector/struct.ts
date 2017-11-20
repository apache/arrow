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

export class StructVector<T = any> extends Vector<StructRow<T>> {
    readonly length: number;
    readonly columns: Vector[];
    constructor(argv: { columns: Vector[] }) {
        super();
        this.columns = argv.columns || [];
    }
    get(index: number): StructRow<T> {
        return new StructRow(this, index);
    }
    col(name: string) {
        return this.columns.find((col) => col.name === name) || null;
    }
    key(index: number) {
        return this.columns[index] ? this.columns[index].name : null;
    }
    select(...columns: string[]) {
        return new StructVector({ columns: columns.map((name) => this.col(name)!) });
    }
    concat(...structs: Vector<StructRow<T>>[]): Vector<StructRow<T>> {
        return new VirtualVector(Array, this, ...structs as any[]);
    }
    toString(options?: any) {
        const index = typeof options === 'object' ? options && !!options.index
                    : typeof options === 'boolean' ? !!options
                    : false;
        const { length } = this;
        if (length <= 0) { return ''; }
        const rows = new Array(length + 1);
        const maxColumnWidths = [] as number[];
        rows[0] = this.columns.map((_, i) => this.key(i));
        index && rows[0].unshift('Index');
        for (let i = -1, n = rows.length - 1; ++i < n;) {
            rows[i + 1] = [...this.get(i)!];
            index && rows[i + 1].unshift(i);
        }
        // Pass one to convert to strings and count max column widths
        for (let i = -1, n = rows.length; ++i < n;) {
            const row = rows[i];
            for (let j = -1, k = row.length; ++j < k;) {
                const val = row[j] = stringify(row[j]);
                maxColumnWidths[j] = !maxColumnWidths[j]
                    ? val.length
                    : Math.max(maxColumnWidths[j], val.length);
            }
        }
        // Pass two to pad each one to max column width
        for (let i = -1, n = rows.length; ++i < n;) {
            const row = rows[i];
            for (let j = -1, k = row.length; ++j < k;) {
                row[j] = leftPad(row[j], ' ', maxColumnWidths[j]);
            }
            rows[i] = row.join(', ');
        }
        return rows.join('\n');
    }
}

export class StructRow<T = any> extends Vector<T> {
    readonly row: number;
    readonly length: number;
    readonly table: StructVector<T>;
    [Symbol.toStringTag]() { return 'Row'; }
    constructor(table: StructVector<T>, row: number) {
        super();
        this.row = row;
        this.table = table;
        this.length = table.columns.length;
    }
    get(index: number) {
        const col = this.table.columns[index];
        return col ? col.get(this.row) as T : null;
    }
    col(key: string) {
        const col = this.table.col(key);
        return col ? col.get(this.row) as T : null;
    }
    *[Symbol.iterator]() {
        const { row } = this;
        for (const col of this.table.columns) {
            yield col ? col.get(row) : null;
        }
    }
    concat(...rows: Vector<T>[]): Vector<T> {
        return new VirtualVector(Array, this, ...rows as any[]);
    }
    toArray() { return [...this]; }
    toJSON() { return this.toArray(); }
    toString() { return JSON.stringify(this); }
    toObject(): Record<string, T> {
        const { row } = this, map = Object.create(null);
        for (const col of this.table.columns) {
            if (col && col.name) {
                map[col.name] = col.get(row);
            }
        }
        return map;
    }
}

function leftPad(str: string, fill: string, n: number) {
    return (new Array(n + 1).join(fill) + str).slice(-1 * n);
}

function stringify(x: any) {
    return Array.isArray(x) ? JSON.stringify(x) : ArrayBuffer.isView(x) ? `[${x}]` : `${x}`;
}
