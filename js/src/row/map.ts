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

import { Data } from '../data.js';
import { Vector } from '../vector.js';
import { DataType, Struct } from '../type.js';
import { valueToString } from '../util/pretty.js';
import { instance as getVisitor } from '../visitor/get.js';
import { instance as setVisitor } from '../visitor/set.js';

/** @ignore */ export const kKeys = Symbol.for('keys');
/** @ignore */ export const kVals = Symbol.for('vals');
/** @ignore */ export const kKeysAsStrings = Symbol.for('kKeysAsStrings');
/** @ignore */ export const _kKeysAsStrings = Symbol.for('_kKeysAsStrings');

export class MapRow<K extends DataType = any, V extends DataType = any> {

    [key: string]: V['TValue'];

    declare private [kKeys]: Vector<K>;
    declare private [kVals]: Data<V>;
    declare private [_kKeysAsStrings]: string[];

    constructor(slice: Data<Struct<{ key: K; value: V }>>) {
        this[kKeys] = new Vector([slice.children[0]]).memoize() as Vector<K>;
        this[kVals] = slice.children[1] as Data<V>;
        return new Proxy(this, new MapRowProxyHandler<K, V>());
    }

    /** @ignore */
    get [kKeysAsStrings]() {
        return this[_kKeysAsStrings] || (this[_kKeysAsStrings] = Array.from(this[kKeys].toArray(), String));
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
            json[keys.get(i)] = getVisitor.visit(vals, i);
        }
        return json;
    }

    public toString() {
        return `{${[...this].map(([key, val]) =>
            `${valueToString(key)}: ${valueToString(val)}`
        ).join(', ')
            }}`;
    }

    public [Symbol.for('nodejs.util.inspect.custom')]() {
        return this.toString();
    }
}

class MapRowIterator<K extends DataType = any, V extends DataType = any>
    implements IterableIterator<[K['TValue'], V['TValue'] | null]> {

    private keys: Vector<K>;
    private vals: Data<V>;
    private numKeys: number;
    private keyIndex: number;

    constructor(keys: Vector<K>, vals: Data<V>) {
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
                this.keys.get(i),
                getVisitor.visit(this.vals, i),
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
        return row[kKeysAsStrings];
    }
    has(row: MapRow<K, V>, key: string | symbol) {
        return row[kKeysAsStrings].includes(key as string);
    }
    getOwnPropertyDescriptor(row: MapRow<K, V>, key: string | symbol) {
        const idx = row[kKeysAsStrings].indexOf(key as string);
        if (idx !== -1) {
            return { writable: true, enumerable: true, configurable: true };
        }
        return;
    }
    get(row: MapRow<K, V>, key: string | symbol) {
        // Look up key in row first
        if (Reflect.has(row, key)) {
            return (row as any)[key];
        }
        const idx = row[kKeysAsStrings].indexOf(key as string);
        if (idx !== -1) {
            const val = getVisitor.visit(Reflect.get(row, kVals), idx);
            // Cache key/val lookups
            Reflect.set(row, key, val);
            return val;
        }
    }
    set(row: MapRow<K, V>, key: string | symbol, val: V) {
        const idx = row[kKeysAsStrings].indexOf(key as string);
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
    [_kKeysAsStrings]: { writable: true, enumerable: false, configurable: false, value: null },
});
