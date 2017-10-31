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

import { Row } from './row';
import { toString } from './toString';
import { VirtualVector } from '../vector/virtual';
import { Vector, Column, Struct } from '../types';

export interface TableVector {
    toString(): string;
    toString(index: boolean): string;
    toString(options: { index: boolean }): string;
}

export class TableVector extends Vector<TableRow> implements Struct<string> {
    readonly length: number;
    readonly columns: Column[];
    constructor(argv: { columns: Column[] }) {
        super();
        this.columns = argv.columns || [];
        if (!this.length) {
            this.length = Math.max(...this.columns.map((col) => col.length)) | 0;
        }
    }
    get(index: number): TableRow {
        return new TableRow(this, index);
    }
    col(name: string) {
        return this.columns.find((col) => col.name === name) || null;
    }
    key(index: number) {
        return this.columns[index] ? this.columns[index].name : null;
    }
    select(...columns: string[]) {
        return new TableVector({ columns: columns.map((name) => this.col(name)!) });
    }
    concat(...tables: Vector<Row<string>>[]): Vector<Row<string>> {
        return new VirtualVector(Array, this, ...tables as any[]);
    }
    toString(x?: any) {
        return toString(this, x);
    }
}

export class TableRow extends Row<string> {
    toString() {
        return this.toArray().map((x) => JSON.stringify(x)).join(', ');
    }
}