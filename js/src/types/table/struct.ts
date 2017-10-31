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

export interface StructVector {
    toString(): string;
    toString(index: boolean): string;
    toString(options: { index: boolean }): string;
}

export class StructVector extends Vector<StructRow> implements Struct<number> {
    readonly length: number;
    readonly columns: Column[];
    constructor(argv: { columns: Column[] }) {
        super();
        this.columns = argv.columns;
        if (!this.length) {
            this.length = Math.max(...this.columns.map((col) => col.length)) | 0;
        }
    }
    get(index: number): StructRow {
        return new StructRow(this, index);
    }
    col(index: number) {
        return this.columns[index] || null;
    }
    key(index: number) {
        return this.columns[index] ? index : null;
    }
    select(...columns: number[]) {
        return new StructVector({ columns: columns.map((name) => this.col(name)!) });
    }
    concat(...structs: Vector<Row<number>>[]): Vector<Row<number>> {
        return new VirtualVector(Array, this, ...structs as any[]);
    }
    toString(x?: any) {
        return toString(this, x);
    }
}

export class StructRow extends Row<number> {
    toString() {
        return JSON.stringify(this);
    }
}
