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

import { readBuffers } from './reader/arrow';
import { StructVector } from './vector/struct';
import { Vector, sliceToRangeArgs } from './vector/vector';

export class Table implements Iterable<Map<string, any>> {
    public length: number;
    protected _columns: Vector<any>[];
    protected _columnsMap: { [k: string]: Vector<any> };
    static from(...bytes: Array<Uint8Array | Buffer | string>) {
        let columns: Vector<any>[];
        for (let vectors of readBuffers(...bytes)) {
            columns = !columns ? vectors : columns.map((v, i) => v.concat(vectors[i]));
        }
        return new Table(columns);
    }
    static fromStruct(vector: StructVector) {
        return new Table((<any> vector).vectors);
    }
    constructor(columns: Vector<any>[]) {
        this._columns = columns || [];
        this.length = Math.max(...this._columns.map((v) => v.length));
        this._columnsMap = this._columns.reduce((map, vec) => {
            return (map[vec.name] = vec) && map || map;
        }, <any> {});
    }
    *[Symbol.iterator]() {
        for (let cols = this._columns, i = -1, n = this.length; ++i < n;) {
            yield rowAsMap(i, cols);
        }
    }
    *rows(startRow?: number | boolean, endRow?: number | boolean, compact?: boolean) {
        let start = startRow as number, end = endRow as number;
        if (typeof startRow === 'boolean') {
            compact = startRow;
            start = end;
            end = undefined;
        } else if (typeof endRow === 'boolean') {
            compact = endRow;
            end = undefined;
        }
        let rowIndex = -1, { length } = this;
        const [rowOffset, rowsTotal] = sliceToRangeArgs(length, start, end);
        while (++rowIndex < rowsTotal) {
            yield this.getRow((rowIndex + rowOffset) % length, compact);
        }
    }
    *cols(startCol?: number, endCol?: number) {
        for (const column of this._columns.slice(startCol, endCol)) {
            yield column;
        }
    }
    getRow(rowIndex: number, compact?: boolean) {
        return (compact && rowAsArray || rowAsObject)(rowIndex, this._columns);
    }
    getCell(columnName: string, rowIndex: number) {
        return this.getColumn(columnName).get(rowIndex);
    }
    getCellAt(columnIndex: number, rowIndex: number) {
        return this.getColumnAt(columnIndex).get(rowIndex);
    }
    getColumn<T = any>(columnName: string) {
        return this._columnsMap[columnName] as Vector<T>;
    }
    getColumnAt<T = any>(columnIndex: number) {
        return this._columns[columnIndex] as Vector<T>;
    }
    toString({ index = false } = {}) {
        const { length } = this;
        if (length <= 0) { return ''; }
        const maxColumnWidths = [];
        const rows = new Array(length + 1);
        rows[0] = this._columns.map((c) => c.name);
        index && rows[0].unshift('Index');
        for (let i = -1, n = rows.length - 1; ++i < n;) {
            rows[i + 1] = this.getRow(i, true);
            index && rows[i + 1].unshift(i);
        }
        // Pass one to convert to strings and count max column widths
        for (let i = -1, n = rows.length; ++i < n;) {
            const row = rows[i];
            for (let j = -1, k = row.length; ++j < k;) {
                const val = row[j] = `${row[j]}`;
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

Table.prototype.length = 0;

function leftPad(str, fill, n) {
    return (new Array(n + 1).join(fill) + str).slice(-1 * n);
}

function rowAsMap(row: number, columns: Vector<any>[]) {
    return columns.reduce((map, vector) => map.set(vector.name, vector.get(row)), new Map());
}

function rowAsObject(rowIndex: number, columns: Vector<any>[]) {
    return columns.reduce((row, vector) => (row[vector.name] = vector.get(rowIndex)) && row || row, Object.create(null));
}

function rowAsArray(rowIndex: number, columns: Vector<any>[]) {
    return columns.reduce((row, vector, columnIndex) => (row[columnIndex] = vector.get(rowIndex)) && row || row, new Array(columns.length));
}
