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

import { Vector } from '../vector';
import { StructVector } from './struct';
import { valueToString } from '../util/pretty';
import { DataType, Struct, RowLike } from '../type';

/** @ignore */ const kParent = Symbol.for('parent');
/** @ignore */ const kRowIndex = Symbol.for('rowIndex');
/** @ignore */ const kKeyToIdx = Symbol.for('keyToIdx');
/** @ignore */ const kIdxToVal = Symbol.for('idxToVal');
/** @ignore */ const kCustomInspect = Symbol.for('nodejs.util.inspect.custom');

abstract class Row<K extends PropertyKey = any, V = any> implements Map<K, V> {

    public readonly size: number;
    public readonly [Symbol.toStringTag]: string;

    protected [kRowIndex]: number;
    protected [kParent]: Vector<Struct>;
    protected [kKeyToIdx]: Map<K, number>;
    protected [kIdxToVal]: V[];

    constructor(parent: Vector<Struct>, numKeys: number) {
        this[kParent] = parent;
        this.size = numKeys;
    }

    public abstract keys(): IterableIterator<K>;
    public abstract values(): IterableIterator<V>;
    public abstract getKey(idx: number): K;
    public abstract getIndex(key: K): number;
    public abstract getValue(idx: number): V;
    public abstract setValue(idx: number, val: V): void;

    public entries() { return this[Symbol.iterator](); }

    public has(key: K) { return this.get(key) !== undefined; }

    public get(key: K) {
        let val = undefined;
        if (key != null) {
            const ktoi = this[kKeyToIdx] || (this[kKeyToIdx] = new Map());
            let idx = ktoi.get(key);
            if (idx !== undefined) {
                const itov = this[kIdxToVal] || (this[kIdxToVal] = new Array(this.size));
                ((val = itov[idx]) !== undefined) || (itov[idx] = val = this.getValue(idx));
            } else if ((idx = this.getIndex(key)) > -1) {
                ktoi.set(key, idx);
                const itov = this[kIdxToVal] || (this[kIdxToVal] = new Array(this.size));
                ((val = itov[idx]) !== undefined) || (itov[idx] = val = this.getValue(idx));
            }
        }
        return val;
    }

    public set(key: K, val: V) {
        if (key != null) {
            const ktoi = this[kKeyToIdx] || (this[kKeyToIdx] = new Map());
            let idx = ktoi.get(key);
            if (idx === undefined) {
                ktoi.set(key, idx = this.getIndex(key));
            }
            if (idx > -1) {
                const itov = this[kIdxToVal] || (this[kIdxToVal] = new Array(this.size));
                itov[idx] = <any> this.setValue(idx, val);
            }
        }
        return this;
    }

    public clear(): void { throw new Error(`Clearing ${this[Symbol.toStringTag]} not supported.`); }

    public delete(_: K): boolean { throw new Error(`Deleting ${this[Symbol.toStringTag]} values not supported.`); }

    public *[Symbol.iterator](): IterableIterator<[K, V]> {

        const ki = this.keys();
        const vi = this.values();
        const ktoi = this[kKeyToIdx] || (this[kKeyToIdx] = new Map());
        const itov = this[kIdxToVal] || (this[kIdxToVal] = new Array(this.size));

        for (let k: K, v: V, i = 0, kr: IteratorResult<K>, vr: IteratorResult<V>;
            !((kr = ki.next()).done || (vr = vi.next()).done);
            ++i
        ) {
            k = kr.value;
            v = vr.value;
            itov[i] = v;
            ktoi.has(k) || ktoi.set(k, i);
            yield [k, v];
        }
    }

    public forEach(callbackfn: (value: V, key: K, map: Map<K, V>) => void, thisArg?: any): void {

        const ki = this.keys();
        const vi = this.values();
        const callback = thisArg === undefined ? callbackfn :
            (v: V, k: K, m: Map<K, V>) => callbackfn.call(thisArg, v, k, m);
        const ktoi = this[kKeyToIdx] || (this[kKeyToIdx] = new Map());
        const itov = this[kIdxToVal] || (this[kIdxToVal] = new Array(this.size));

        for (let k: K, v: V, i = 0, kr: IteratorResult<K>, vr: IteratorResult<V>;
            !((kr = ki.next()).done || (vr = vi.next()).done);
            ++i
        ) {
            k = kr.value;
            v = vr.value;
            itov[i] = v;
            ktoi.has(k) || ktoi.set(k, i);
            callback(v, k, this);
        }
    }

    public toArray() { return [...this.values()]; }
    public toJSON() {
        const obj = {} as any;
        this.forEach((val, key) => obj[key] = val);
        return obj;
    }

    public inspect() { return this.toString(); }
    public [kCustomInspect]() { return this.toString(); }
    public toString() {
        const str: string[] = [];
        this.forEach((val, key) => {
            key = valueToString(key);
            val = valueToString(val);
            str.push(`${key}: ${val}`);
        });
        return `{ ${str.join(', ')} }`;
    }

    protected static [Symbol.toStringTag] = ((proto: Row) => {
        Object.defineProperties(proto, {
            'size': { writable: true, enumerable: false, configurable: false, value: 0 },
            [kParent]: { writable: true, enumerable: false, configurable: false, value: null },
            [kRowIndex]: { writable: true, enumerable: false, configurable: false, value: -1 },
        });
        return (proto as any)[Symbol.toStringTag] = 'Row';
    })(Row.prototype);
}

export class MapRow<K extends DataType = any, V extends DataType = any> extends Row<K['TValue'], V['TValue'] | null> {
    constructor(slice: Vector<Struct<{ key: K; value: V }>>) {
        super(slice, slice.length);
        return createRowProxy(this);
    }
    public keys() {
        return this[kParent].getChildAt(0)![Symbol.iterator]();
    }
    public values() {
        return this[kParent].getChildAt(1)![Symbol.iterator]();
    }
    public getKey(idx: number): K['TValue'] {
        return this[kParent].getChildAt(0)!.get(idx);
    }
    public getIndex(key: K['TValue']): number {
        return this[kParent].getChildAt(0)!.indexOf(key);
    }
    public getValue(index: number): V['TValue'] | null {
        return this[kParent].getChildAt(1)!.get(index);
    }
    public setValue(index: number, value: V['TValue'] | null): void {
        this[kParent].getChildAt(1)!.set(index, value);
    }
}

export class StructRow<T extends { [key: string]: DataType } = any> extends Row<keyof T, T[keyof T]['TValue'] | null> {
    constructor(parent: StructVector<T>) {
        super(parent, parent.type.children.length);
        return defineRowProxyProperties(this);
    }
    public *keys() {
        for (const field of this[kParent].type.children) {
            yield field.name as keyof T;
        }
    }
    public *values() {
        for (const field of this[kParent].type.children) {
            yield (this as RowLike<T>)[field.name];
        }
    }
    public getKey(idx: number): keyof T {
        return this[kParent].type.children[idx].name as keyof T;
    }
    public getIndex(key: keyof T): number {
        return this[kParent].type.children.findIndex((f) => f.name === key);
    }
    public getValue(index: number): T[keyof T]['TValue'] | null {
        return this[kParent].getChildAt(index)!.get(this[kRowIndex]);
    }
    public setValue(index: number, value: T[keyof T]['TValue'] | null): void {
        return this[kParent].getChildAt(index)!.set(this[kRowIndex], value);
    }
}

Object.setPrototypeOf(Row.prototype, Map.prototype);

/** @ignore */
const defineRowProxyProperties = (() => {
    const desc = { enumerable: true, configurable: false, get: null as any, set: null as any };
    return <T extends Row>(row: T) => {
        let idx = -1;
        const ktoi = row[kKeyToIdx] || (row[kKeyToIdx] = new Map());
        const getter = (key: any) => function(this: T) { return this.get(key); };
        const setter = (key: any) => function(this: T, val: any) { return this.set(key, val); };
        for (const key of row.keys()) {
            ktoi.set(key, ++idx);
            desc.get = getter(key);
            desc.set = setter(key);
            Object.prototype.hasOwnProperty.call(row, key) || (desc.enumerable = true, Object.defineProperty(row, key, desc));
            Object.prototype.hasOwnProperty.call(row, idx) || (desc.enumerable = false, Object.defineProperty(row, idx, desc));
        }
        desc.get = desc.set = null;
        return row;
    };
})();

/** @ignore */
const createRowProxy = (() => {
    if (typeof Proxy === 'undefined') {
        return defineRowProxyProperties;
    }
    const has = Row.prototype.has;
    const get = Row.prototype.get;
    const set = Row.prototype.set;
    const getKey = Row.prototype.getKey;
    const RowProxyHandler: ProxyHandler<Row> = {
        isExtensible() { return false; },
        deleteProperty() { return false; },
        preventExtensions() { return true; },
        ownKeys(row: Row) { return [...row.keys()].map((x) => `${x}`); },
        has(row: Row, key: PropertyKey) {
            switch (key) {
                case 'getKey': case 'getIndex': case 'getValue': case 'setValue': case 'toArray': case 'toJSON': case 'inspect':
                case 'constructor': case 'isPrototypeOf': case 'propertyIsEnumerable': case 'toString': case 'toLocaleString': case 'valueOf':
                case 'size': case 'has': case 'get': case 'set': case 'clear': case 'delete': case 'keys': case 'values': case 'entries': case 'forEach':
                case '__proto__': case '__defineGetter__': case '__defineSetter__': case 'hasOwnProperty': case '__lookupGetter__': case '__lookupSetter__':
                case Symbol.iterator: case Symbol.toStringTag: case kParent: case kRowIndex: case kIdxToVal: case kKeyToIdx: case kCustomInspect:
                    return true;
            }
            if (typeof key === 'number' && !row.has(key)) {
                key = row.getKey(key);
            }
            return row.has(key);
        },
        get(row: Row, key: PropertyKey, receiver: any) {
            switch (key) {
                case 'getKey': case 'getIndex': case 'getValue': case 'setValue': case 'toArray': case 'toJSON': case 'inspect':
                case 'constructor': case 'isPrototypeOf': case 'propertyIsEnumerable': case 'toString': case 'toLocaleString': case 'valueOf':
                case 'size': case 'has': case 'get': case 'set': case 'clear': case 'delete': case 'keys': case 'values': case 'entries': case 'forEach':
                case '__proto__': case '__defineGetter__': case '__defineSetter__': case 'hasOwnProperty': case '__lookupGetter__': case '__lookupSetter__':
                case Symbol.iterator: case Symbol.toStringTag: case kParent: case kRowIndex: case kIdxToVal: case kKeyToIdx: case kCustomInspect:
                    return Reflect.get(row, key, receiver);
            }
            if (typeof key === 'number' && !has.call(receiver, key)) {
                key = getKey.call(receiver, key);
            }
            return get.call(receiver, key);
        },
        set(row: Row, key: PropertyKey, val: any, receiver: any) {
            switch (key) {
                case kParent: case kRowIndex: case kIdxToVal: case kKeyToIdx:
                    return Reflect.set(row, key, val, receiver);
                case 'getKey': case 'getIndex': case 'getValue': case 'setValue': case 'toArray': case 'toJSON': case 'inspect':
                case 'constructor': case 'isPrototypeOf': case 'propertyIsEnumerable': case 'toString': case 'toLocaleString': case 'valueOf':
                case 'size': case 'has': case 'get': case 'set': case 'clear': case 'delete': case 'keys': case 'values': case 'entries': case 'forEach':
                case '__proto__': case '__defineGetter__': case '__defineSetter__': case 'hasOwnProperty': case '__lookupGetter__': case '__lookupSetter__':
                case Symbol.iterator: case Symbol.toStringTag:
                    return false;
            }
            if (typeof key === 'number' && !has.call(receiver, key)) {
                key = getKey.call(receiver, key);
            }
            return has.call(receiver, key) ? !!set.call(receiver, key, val) : false;
        },
    };
    return <T extends Row>(row: T) => new Proxy(row, RowProxyHandler) as T;
})();
