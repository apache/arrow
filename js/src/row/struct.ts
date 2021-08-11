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

import { Data } from '../data';
import { Field } from '../schema';
import { Vector } from '../vector';
import { DataType, Struct } from '../type';
import { valueToString } from '../util/pretty';

/** @ignore */ const kParent = Symbol.for('parent');
/** @ignore */ const kRowIndex = Symbol.for('rowIndex');

export class StructRow<T extends { [key: string]: DataType } = any> {

    declare private [kRowIndex]: number;
    declare private [kParent]: Vector<Struct<T>>;

    constructor(parent: Data<Struct<T>>, rowIndex: number) {
        this[kParent] = new Vector([parent]);
        this[kRowIndex] = rowIndex;
        return new Proxy(this, new StructRowProxyHandler());
    }

    public toArray() { return Object.values(this.toJSON()); }

    public toJSON() {
        const i = this[kRowIndex];
        const parent = this[kParent];
        const keys = parent.type.children;
        const json = {} as { [P in string & keyof T]: T[P]['TValue'] };
        for (let j = -1, n = keys.length; ++j < n;) {
            json[keys[j].name as string & keyof T] = parent.getChildAt(j)![i];
        }
        return json;
    }

    public [Symbol.for('nodejs.util.inspect.custom')]() {
        return `{${
            [...this].map(([key, val]) =>
                `${valueToString(key)}: ${valueToString(val)}`
            ).join(', ')
        }}`;
    }

    [Symbol.iterator](): IterableIterator<[
        keyof T, { [P in keyof T]: T[P]['TValue'] | null }[keyof T]
    ]> {
        return new StructRowIterator(this[kParent], this[kRowIndex]);
    }
}

class StructRowIterator<T extends { [key: string]: DataType } = any>
    implements IterableIterator<[
        keyof T, { [P in keyof T]: T[P]['TValue'] | null }[keyof T]
    ]> {

    declare private rowIndex: number;
    declare private childIndex: number;
    declare private numChildren: number;
    declare private parent: Vector<Struct<T>>;
    declare private childFields: Field<T[keyof T]>[];

    constructor(parent: Vector<Struct<T>>, rowIndex: number) {
        this.childIndex = 0;
        this.parent = parent;
        this.rowIndex = rowIndex;
        this.childFields = parent.type.children;
        this.numChildren = this.childFields.length;
    }

    [Symbol.iterator]() { return this; }

    next() {
        const i = this.childIndex;
        if (i < this.numChildren) {
            this.childIndex = i + 1;
            return {
                done: false,
                value: [
                    this.childFields[i].name,
                    this.parent.getChildAt(i)![this.rowIndex]
                ]
            } as IteratorYieldResult<[any, any]>;
        }
        return { done: true, value: null } as IteratorReturnResult<null>;
    }
}

Object.defineProperties(StructRow.prototype, {
    [Symbol.toStringTag]: { enumerable: false, configurable: false, value: 'Row' },
    [kParent]: { writable: true, enumerable: false, configurable: false, value: null },
    [kRowIndex]: { writable: true, enumerable: false, configurable: false, value: -1 },
});

class StructRowProxyHandler<T extends { [key: string]: DataType } = any> implements ProxyHandler<StructRow<T>> {
    isExtensible() { return false; }
    deleteProperty() { return false; }
    preventExtensions() { return true; }
    ownKeys(row: StructRow<T>) {
        return row[kParent].type.children.map((f) => f.name);
    }
    has(row: StructRow<T>, key: string) {
        return row[kParent].type.children.findIndex((f) => f.name === key) !== -1;
    }
    getOwnPropertyDescriptor(row: StructRow<T>, key: string) {
        if (row[kParent].type.children.findIndex((f) => f.name === key) !== -1) {
            return { writable: true, enumerable: true, configurable: true };
        }
        return undefined;
    }
    get(row: StructRow<T>, key: string) {
        // Look up key in row first
        if (Reflect.has(row, key)) {
            return (row as any)[key];
        }
        const idx = row[kParent].type.children.findIndex((f) => f.name === key);
        if (idx !== -1) {
            const val = row[kParent].getChildAt(idx)![row[kRowIndex]];
            // Cache key/val lookups
            Reflect.set(row, key, val);
            return val;
        }
    }
    set(row: StructRow<T>, key: string, val: any) {
        const idx = row[kParent].type.children.findIndex((f) => f.name === key);
        if (idx !== -1) {
            row[kParent].getChildAt(idx)![row[kRowIndex]] = val;
            // Cache key/val lookups
            return Reflect.set(row, key, val);
        } else if (Reflect.has(row, key)) {
            return Reflect.set(row, key, val);
        }
        return false;
    }
}
