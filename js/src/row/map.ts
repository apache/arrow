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
import { DataType, Struct } from '../type';
import { valueToString } from '../util/pretty';
import { instance as getVisitor } from '../visitor/get';
import { instance as setVisitor } from '../visitor/set';
import { instance as indexOfVisitor } from '../visitor/indexof';
import { instance as iteratorVisitor } from '../visitor/iterator';

/** @ignore */ export const kKeys = Symbol.for('keys');
/** @ignore */ export const kVals = Symbol.for('vals');

export class MapRow<K extends DataType = any, V extends DataType = any> {
    private [kKeys]: Data<K>;
    private [kVals]: Data<V>;
    constructor(slice: Data<Struct<{ key: K; value: V }>>) {
        this[kKeys] = slice.children[0] as Data<K>;
        this[kVals] = slice.children[1] as Data<V>;
    }
    [Symbol.iterator]() {
        return new MapRowIterator(this[kKeys], this[kVals]);
    }
    public get size() { return this[kKeys].length; }
    public toArray() { return Object.values(this.toJSON()); }
    public toJSON() {
        const keys = this[kKeys];
        const vals = this[kVals];
        const json = {} as { [P in K['TValue']]: V['TValue'] };
        for (let i = -1, n = keys.length; ++i < n;) {
            json[getVisitor.visit(keys, i)] = getVisitor.visit(vals, i);
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
}

class MapRowIterator<K extends DataType = any, V extends DataType = any>
    implements IterableIterator<[K['TValue'], V['TValue'] | null]> {

    private keys: Data<K>;
    private vals: Data<V>;
    private numKeys: number;
    private keyIndex: number;

    constructor(keys: Data<K>, vals: Data<V>) {
        this.keys = keys;
        this.vals = vals;
        this.keyIndex = 0;
        this.numKeys = keys.length;
    }

    [Symbol.iterator]() { return this; }

    next() {
        const i = this.keyIndex;
        if (i === this.numKeys) {
            return { done: true, value: null } as IteratorReturnResult<null>;
        }
        this.keyIndex++;
        return {
            done: false,
            value: [
                getVisitor.visit(this.keys, i),
                getVisitor.visit(this.vals, i),
            ] as [K['TValue'], V['TValue'] | null]
        };
    }
}

/** @ignore */
class MapRowProxyHandler implements ProxyHandler<MapRow> {
    isExtensible() { return false; }
    deleteProperty() { return false; }
    preventExtensions() { return true; }
    ownKeys(row: MapRow) {
        return [...iteratorVisitor.visit(Reflect.get(row, kKeys))].map(valueToString);
    }
    has(row: MapRow, key: string) {
        const idx = indexOfVisitor.visit(Reflect.get(row, kKeys), key);
        return idx !== -1 ? true : Reflect.has(row, key);
    }
    get(_: any, key: string, row: MapRow) {
        if (!Reflect.has(row, key)) {
            const idx = indexOfVisitor.visit(Reflect.get(row, kKeys), key);
            if (idx !== -1) {
                const val = getVisitor.visit(Reflect.get(row, kVals), idx);
                // Cache key/val lookups
                Reflect.set(row, key, val);
                return val;
            }
        }
        return Reflect.get(row, key);
    }
    set(_: any, key: string, val: any, row: MapRow) {
        const idx = indexOfVisitor.visit(Reflect.get(row, kKeys), key);
        if (idx !== -1) {
            setVisitor.visit(Reflect.get(row, kVals), idx, val);
            // Cache key/val lookups
            return Reflect.set(row, key, val);
        } else if (Reflect.has(row, key)) {
            return Reflect.set(row, key, val);
        }
        return false;
    }
}

Object.defineProperties(MapRow.prototype, {
    [Symbol.toStringTag]: { enumerable: false, configurable: false, value: 'Row' },
    [kKeys]: { writable: true, enumerable: false, configurable: false, value: null },
    [kVals]: { writable: true, enumerable: false, configurable: false, value: null },
});

Object.setPrototypeOf(MapRow.prototype, new Proxy({}, new MapRowProxyHandler()));
