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

import { Vector } from '../vector.js';
import { MapRow } from '../row/map.js';
import { StructRow } from '../row/struct.js';
import { compareArrayLike } from '../util/buffer.js';

/** @ignore */
type RangeLike = { length: number; stride?: number };
/** @ignore */
type ClampRangeThen<T extends RangeLike> = (source: T, offset: number, length: number) => any;

/** @ignore */
let tmp: number;
export function clampRange<T extends RangeLike>(source: T, begin: number | undefined, end: number | undefined): [number, number];
export function clampRange<T extends RangeLike, N extends ClampRangeThen<T> = ClampRangeThen<T>>(source: T, begin: number | undefined, end: number | undefined, then: N): ReturnType<N>;
/** @ignore */
export function clampRange<T extends RangeLike, N extends ClampRangeThen<T> = ClampRangeThen<T>>(source: T, begin: number | undefined, end: number | undefined, then?: N) {

    // Adjust args similar to Array.prototype.slice. Normalize begin/end to
    // clamp between 0 and length, and wrap around on negative indices, e.g.
    // slice(-1, 5) or slice(5, -1)
    const { length: len = 0 } = source;
    let lhs = typeof begin !== 'number' ? 0 : begin;
    let rhs = typeof end !== 'number' ? len : end;
    // wrap around on negative start/end positions
    (lhs < 0) && (lhs = ((lhs % len) + len) % len);
    (rhs < 0) && (rhs = ((rhs % len) + len) % len);
    // ensure lhs <= rhs
    (rhs < lhs) && (tmp = lhs, lhs = rhs, rhs = tmp);
    // ensure rhs <= length
    (rhs > len) && (rhs = len);

    return then ? then(source, lhs, rhs) : [lhs, rhs];
}

/** @ignore */
export const wrapIndex = (index: number, len: number) => index < 0 ? (len + index) : index;

const isNaNFast = (value: any) => value !== value;

/** @ignore */
export function createElementComparator(search: any) {
    const typeofSearch = typeof search;
    // Compare primitives
    if (typeofSearch !== 'object' || search === null) {
        // Compare NaN
        if (isNaNFast(search)) {
            return isNaNFast;
        }
        return (value: any) => value === search;
    }
    // Compare Dates
    if (search instanceof Date) {
        const valueOfSearch = search.valueOf();
        return (value: any) => value instanceof Date ? (value.valueOf() === valueOfSearch) : false;
    }
    // Compare TypedArrays
    if (ArrayBuffer.isView(search)) {
        return (value: any) => value ? compareArrayLike(search, value) : false;
    }
    // Compare Maps and Rows
    if (search instanceof Map) { return createMapComparator(search); }
    // Compare Array-likes
    if (Array.isArray(search)) { return createArrayLikeComparator(search); }
    // Compare Vectors
    if (search instanceof Vector) { return createVectorComparator(search); }
    return createObjectComparator(search, true);
    // Compare non-empty Objects
    // return createObjectComparator(search, search instanceof Proxy);
}

/** @ignore */
function createArrayLikeComparator(lhs: ArrayLike<any>) {
    const comparators = [] as ((x: any) => boolean)[];
    for (let i = -1, n = lhs.length; ++i < n;) {
        comparators[i] = createElementComparator(lhs[i]);
    }
    return createSubElementsComparator(comparators);
}

/** @ignore */
function createMapComparator(lhs: Map<any, any>) {
    let i = -1;
    const comparators = [] as ((x: any) => boolean)[];
    for (const v of lhs.values()) comparators[++i] = createElementComparator(v);
    return createSubElementsComparator(comparators);
}

/** @ignore */
function createVectorComparator(lhs: Vector<any>) {
    const comparators = [] as ((x: any) => boolean)[];
    for (let i = -1, n = lhs.length; ++i < n;) {
        comparators[i] = createElementComparator(lhs.get(i));
    }
    return createSubElementsComparator(comparators);
}

/** @ignore */
function createObjectComparator(lhs: any, allowEmpty = false) {
    const keys = Object.keys(lhs);
    // Only compare non-empty Objects
    if (!allowEmpty && keys.length === 0) { return () => false; }
    const comparators = [] as ((x: any) => boolean)[];
    for (let i = -1, n = keys.length; ++i < n;) {
        comparators[i] = createElementComparator(lhs[keys[i]]);
    }
    return createSubElementsComparator(comparators, keys);
}

function createSubElementsComparator(comparators: ((x: any) => boolean)[], keys?: Iterable<string>) {
    return (rhs: any) => {
        if (!rhs || typeof rhs !== 'object') {
            return false;
        }
        switch (rhs.constructor) {
            case Array: return compareArray(comparators, rhs);
            case Map:
                return compareObject(comparators, rhs, rhs.keys());
            case MapRow:
            case StructRow:
            case Object:
            case undefined: // support `Object.create(null)` objects
                return compareObject(comparators, rhs, keys || Object.keys(rhs));
        }
        return rhs instanceof Vector ? compareVector(comparators, rhs) : false;
    };
}

function compareArray(comparators: ((x: any) => boolean)[], arr: any[]) {
    const n = comparators.length;
    if (arr.length !== n) { return false; }
    for (let i = -1; ++i < n;) {
        if (!(comparators[i](arr[i]))) { return false; }
    }
    return true;
}

function compareVector(comparators: ((x: any) => boolean)[], vec: Vector) {
    const n = comparators.length;
    if (vec.length !== n) { return false; }
    for (let i = -1; ++i < n;) {
        if (!(comparators[i](vec.get(i)))) { return false; }
    }
    return true;
}

function compareObject(comparators: ((x: any) => boolean)[], obj: Map<any, any>, keys: Iterable<string>) {

    const lKeyItr = keys[Symbol.iterator]();
    const rKeyItr = obj instanceof Map ? obj.keys() : Object.keys(obj)[Symbol.iterator]();
    const rValItr = obj instanceof Map ? obj.values() : Object.values(obj)[Symbol.iterator]();

    let i = 0;
    const n = comparators.length;
    let rVal = rValItr.next();
    let lKey = lKeyItr.next();
    let rKey = rKeyItr.next();

    for (; i < n && !lKey.done && !rKey.done && !rVal.done;
        ++i, lKey = lKeyItr.next(), rKey = rKeyItr.next(), rVal = rValItr.next()) {
        if (lKey.value !== rKey.value || !comparators[i](rVal.value)) {
            break;
        }
    }
    if (i === n && lKey.done && rKey.done && rVal.done) {
        return true;
    }
    lKeyItr.return && lKeyItr.return();
    rKeyItr.return && rKeyItr.return();
    rValItr.return && rValItr.return();
    return false;
}
