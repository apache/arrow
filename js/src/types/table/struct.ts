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

import { RowVector } from './row';
import { toString } from './toString';
import { VirtualVector } from '../vector/virtual';
import { Row, Vector, Column, Struct } from '../types';

export interface StructVector {
    toString(): string;
    toString(index: boolean): string;
    toString(options: { index: boolean }): string;
}

export class StructVector<T = any> extends Vector<Row<T>> implements Struct<T> {
    readonly length: number;
    readonly columns: Column[];
    constructor(argv: { columns: Column[] }) {
        super();
        this.columns = argv.columns || [];
        if (!this.length) {
            this.length = Math.max(...this.columns.map((col) => col.length)) | 0;
        }
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
    concat(...structs: Vector<Row<T>>[]): Vector<Row<T>> {
        return new VirtualVector(Array, this, ...structs as any[]);
    }
    toString(x?: any) {
        return toString(this, x);
    }
}

export class StructRow<T> extends RowVector<T> {
    toString() {
        return JSON.stringify(this);
    }
}