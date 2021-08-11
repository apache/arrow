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
import { Vector } from '../vector';
import { DataType, Struct } from '../type';
import { valueToString } from '../util/pretty';

/** @ignore */ export const kKeys = Symbol.for('keys');
/** @ignore */ export const kVals = Symbol.for('vals');

export class MapRow<K extends DataType = any, V extends DataType = any> {

    [key: string]: V['TValue'];

    declare private [kKeys]: Vector<K>;
    declare private [kVals]: Vector<V>;
    constructor(slice: Data<Struct<{ key: K; value: V }>>) {
        this[kKeys] = new Vector([slice.children[0] as Data<K>]);
        this[kVals] = new Vector([slice.children[1] as Data<V>]);
        return new Proxy(this, new MapRowProxyHandler<K, V>());
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
            json[keys[i]] = vals[i];
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

    private keys: Vector<K>;
    private vals: Vector<V>;
    private numKeys: number;
    private keyIndex: number;

    constructor(keys: Vector<K>, vals: Vector<V>) {
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
                this.keys[i],
                this.vals[i],
            ] as [K['TValue'], V['TValue'] | null]
        };
    }
}

/** @ignore */
class MapRowProxyHandler<K extends DataType = any, V extends DataType = any> implements ProxyHandler<MapRow<K, V>> {
    isExtensible() { return false; }
    deleteProperty() { return false; }
    preventExtensions() { return true; }
    ownKeys(row: MapRow<K, V>) {
        return row[kKeys].toArray().map(String);
    }
    has(row: MapRow<K, V>, key: string | symbol) {
        return row[kKeys].indexOf(key) !== -1;
    }
    getOwnPropertyDescriptor(row: MapRow<K, V>, key: string | symbol) {
        const idx = row[kKeys].indexOf(key);
        if (idx !== -1) {
            return { writable: true, enumerable: true, configurable: true };
        }
        return undefined;
    }
    get(row: MapRow<K, V>, key: string | symbol) {
        // Look up key in row first
        if (Reflect.has(row, key)) {
            return (row as any)[key];
        }
        const idx = row[kKeys].indexOf(key);
        if (idx !== -1) {
            const val = row[kVals][idx];
            // Cache key/val lookups
            Reflect.set(row, key, val);
            return val;
        }
    }
    set(row: MapRow<K, V>, key: string | symbol, val: V) {
        const idx = row[kKeys].indexOf(key);
        if (idx !== -1) {
            row[kVals][idx] = val;
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
