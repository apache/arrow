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
import { DataType, Struct } from '../type';
import { valueToString } from '../util/pretty';
import { instance as getVisitor } from '../visitor/get';
import { instance as setVisitor } from '../visitor/set';

/** @ignore */ const kParent = Symbol.for('parent');
/** @ignore */ const kRowIndex = Symbol.for('rowIndex');

export class StructRow<T extends { [key: string]: DataType } = any> {

    static bind<T extends { [key: string]: DataType } = any>(base: StructRow<T>, index: number) {
        const bound = Object.create(base);
        bound[kRowIndex] = index;
        return bound;
    }

    private [kRowIndex]: number;
    private [kParent]: Data<Struct<T>>;

    constructor(parent: Data<Struct<T>>) {
        this[kParent] = parent;
        return defineRowProxyProperties(this);
    }

    public toArray() { return Object.values(this.toJSON()); }

    public toJSON() {
        const i = this[kRowIndex];
        const cols = this[kParent].children;
        const keys = this[kParent].type.children;
        const json = {} as { [P in string & keyof T]: T[P]['TValue'] };
        for (let j = -1, n = keys.length; ++j < n;) {
            json[keys[j].name as string & keyof T] = getVisitor.visit(cols[j], i);
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

    private rowIndex: number;
    private childIndex: number;
    private numChildren: number;
    private children: Data<any>[];
    private childFields: Field<T[keyof T]>[];

    constructor(data: Data<Struct<T>>, rowIndex: number) {
        this.rowIndex = rowIndex;
        this.childIndex = 0;
        this.children = data.children;
        this.childFields = data.type.children;
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
                    getVisitor.visit(this.children[i], this.rowIndex)
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

/** @ignore */
const defineRowProxyProperties = (() => {
    const desc = {
        get: null as any,
        set: null as any,
        enumerable: true,
        configurable: false,
    };
    return <T extends StructRow>(base: T) => {
        const children = base[kParent].children;
        const getter = (childIndex: number) => function(this: T) {
            return getVisitor.visit(children[childIndex], this[kRowIndex]);
        };
        const setter = (childIndex: number) => function(this: T, value: any) {
            return setVisitor.visit(children[childIndex], this[kRowIndex], value);
        };
        const fields = base[kParent].type.children;
        for (let i = -1, n = fields.length; ++i < n;) {
            desc.get = getter(i);
            desc.set = setter(i);
            if (!Object.prototype.hasOwnProperty.call(base, fields[i].name)) {
                Object.defineProperty(base, fields[i].name, desc);
            }
        }
        desc.get = desc.set = null;
        return base;
    };
})();
