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
import { DataType, RowLike } from '../type';
import { valueToString } from '../util/pretty';
import { StructVector } from '../vector/struct';

/** @ignore */ const columnDescriptor = { enumerable: true, configurable: false, get: () => {} };
/** @ignore */ const lengthDescriptor = { writable: false, enumerable: false, configurable: false, value: -1 };
/** @ignore */ const rowIndexDescriptor = { writable: false, enumerable: false, configurable: true, value: null as any };
/** @ignore */ const rowParentDescriptor = { writable: false, enumerable: false, configurable: false, value: null as any };
/** @ignore */ const row = { parent: rowParentDescriptor, rowIndex: rowIndexDescriptor };

/** @ignore */
export class Row<T extends { [key: string]: DataType }> implements Iterable<T[keyof T]['TValue']> {
    [key: string]: T[keyof T]['TValue'];
    /** @nocollapse */
    public static new<T extends { [key: string]: DataType }>(schemaOrFields: T | Field[], fieldsAreEnumerable = false): RowLike<T> & Row<T> {
        let schema: T, fields: Field[];
        if (Array.isArray(schemaOrFields)) {
            fields = schemaOrFields;
        } else {
            schema = schemaOrFields;
            fieldsAreEnumerable = true;
            fields = Object.keys(schema).map((x) => new Field(x, schema[x]));
        }
        return new Row<T>(fields, fieldsAreEnumerable) as RowLike<T> & Row<T>;
    }
    // @ts-ignore
    private parent: TParent;
    // @ts-ignore
    private rowIndex: number;
    // @ts-ignore
    public readonly length: number;
    private constructor(fields: Field[], fieldsAreEnumerable: boolean) {
        lengthDescriptor.value = fields.length;
        Object.defineProperty(this, 'length', lengthDescriptor);
        fields.forEach((field, columnIndex) => {
            columnDescriptor.get = this._bindGetter(columnIndex);
            // set configurable to true to ensure Object.defineProperty
            // doesn't throw in the case of duplicate column names
            columnDescriptor.configurable = true;
            columnDescriptor.enumerable = fieldsAreEnumerable;
            Object.defineProperty(this, field.name, columnDescriptor);
            columnDescriptor.configurable = false;
            columnDescriptor.enumerable = !fieldsAreEnumerable;
            Object.defineProperty(this, columnIndex, columnDescriptor);
            columnDescriptor.get = null as any;
        });
    }
    *[Symbol.iterator](this: RowLike<T>) {
        for (let i = -1, n = this.length; ++i < n;) {
            yield this[i];
        }
    }
    private _bindGetter(colIndex: number) {
        return function (this: Row<T>) {
            let child = this.parent.getChildAt(colIndex);
            return child ? child.get(this.rowIndex) : null;
        };
    }
    public get<K extends keyof T>(key: K) { return (this as any)[key] as T[K]['TValue']; }
    public bind<TParent extends MapVector<T> | StructVector<T>>(parent: TParent, rowIndex: number) {
        rowIndexDescriptor.value = rowIndex;
        rowParentDescriptor.value = parent;
        const bound = Object.create(this, row);
        rowIndexDescriptor.value = null;
        rowParentDescriptor.value = null;
        return bound as RowLike<T>;
    }
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
