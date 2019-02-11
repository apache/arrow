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

import { Field } from '../schema';
import { MapVector } from '../vector/map';
import { DataType } from '../type';
import { valueToString } from '../util/pretty';
import { StructVector } from '../vector/struct';

/** @ignore */ const columnDescriptor = { enumerable: true, configurable: false, get: () => {} };
/** @ignore */ const lengthDescriptor = { writable: false, enumerable: false, configurable: false, value: -1 };
/** @ignore */ const rowParentDescriptor = { writable: false, enumerable: false, configurable: false, value: null as any };

export class Row<T extends { [key: string]: DataType }> implements Iterable<T[keyof T]['TValue']> {
    [key: string]: T[keyof T]['TValue'];
    // @ts-ignore
    public parent: MapVector<T> | StructVector<T>;
    // @ts-ignore
    public rowIndex: number;
    // @ts-ignore
    public readonly length: number;
    constructor(rowIndex: number) {
        this.rowIndex = rowIndex;
    }
    *[Symbol.iterator]() {
        for (let i = -1, n = this.length; ++i < n;) {
            yield this[i];
        }
    }
    public get<K extends keyof T>(key: K) { return (this as any)[key] as T[K]['TValue']; }
    public toJSON(): any {
        return DataType.isStruct(this.parent.type) ? [...this] :
            Object.getOwnPropertyNames(this).reduce((props: any, prop: string) => {
                return (props[prop] = (this as any)[prop]) && props || props;
            }, {});
    }
    public toString() {
        return DataType.isStruct(this.parent.type) ?
            [...this].map((x) => valueToString(x)).join(', ') :
            Object.getOwnPropertyNames(this).reduce((props: any, prop: string) => {
                return (props[prop] = valueToString((this as any)[prop])) && props || props;
            }, {});
    }
}

interface RowConstructor<T extends { [key: string]: DataType }> {
    readonly prototype: Row<T>;
    new(rowIndex: number): T & Row<T>;
}

/** @ignore */
export class RowProxyGenerator<T extends { [key: string]: DataType }> {
    /** @nocollapse */
    public static new<T extends { [key: string]: DataType }>(parent: MapVector<T> | StructVector<T>, schemaOrFields: T | Field[], fieldsAreEnumerable = false): RowProxyGenerator<T> {
        let schema: T, fields: Field[];
        if (Array.isArray(schemaOrFields)) {
            fields = schemaOrFields;
        } else {
            schema = schemaOrFields;
            fieldsAreEnumerable = true;
            fields = Object.keys(schema).map((x) => new Field(x, schema[x]));
        }
        return new RowProxyGenerator<T>(parent, fields, fieldsAreEnumerable);
    }

    private RowProxy: RowConstructor<T>;

    private constructor(parent: MapVector<T> | StructVector<T>, fields: Field[], fieldsAreEnumerable: boolean) {
        class BoundRow extends Row<T> {}

        const proto = BoundRow.prototype;

        rowParentDescriptor.value = parent;
        lengthDescriptor.value = fields.length;
        Object.defineProperty(proto, 'parent', rowParentDescriptor);
        Object.defineProperty(proto, 'length', lengthDescriptor);
        fields.forEach((field, columnIndex) => {
            columnDescriptor.get = function() {
                const child = (this as any as Row<T>).parent.getChildAt(columnIndex);
                return child ? child.get((this as any as Row<T>).rowIndex) : null;
            };
            // set configurable to true to ensure Object.defineProperty
            // doesn't throw in the case of duplicate column names
            columnDescriptor.configurable = true;
            columnDescriptor.enumerable = fieldsAreEnumerable;
            Object.defineProperty(proto, field.name, columnDescriptor);
            columnDescriptor.configurable = false;
            columnDescriptor.enumerable = !fieldsAreEnumerable;
            Object.defineProperty(proto, columnIndex, columnDescriptor);
            columnDescriptor.get = null as any;
        });

        this.RowProxy = (BoundRow as any);
    }
    public bind(rowIndex: number) {
        const bound = Object.create(this.RowProxy.prototype);
        bound.rowIndex = rowIndex;
        return bound;
        //return new this.RowProxy(rowIndex);
    }
}
