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

import { ReadableInterop, ArrowJSONLike } from '../io/interfaces';

/** @internal */
type FSReadStream = import('fs').ReadStream;
/** @internal */
type FileHandle = import('fs').promises.FileHandle;

/** @internal */
export interface Subscription {
    unsubscribe: () => void;
}

/** @internal */
export interface Observer<T> {
    closed?: boolean;
    next: (value: T) => void;
    error: (err: any) => void;
    complete: () => void;
}

/** @internal */
export interface Observable<T> {
    subscribe: (observer: Observer<T>) => Subscription;
}

/** @internal */
const [BigIntCtor, BigIntAvailable] = (() => {
    const BigIntUnavailableError = () => { throw new Error('BigInt is not available in this environment'); };
    function BigIntUnavailable() { throw BigIntUnavailableError(); }
    BigIntUnavailable.asIntN = () => { throw BigIntUnavailableError(); };
    BigIntUnavailable.asUintN = () => { throw BigIntUnavailableError(); };
    return typeof BigInt !== 'undefined' ? [BigInt, true] : [<any> BigIntUnavailable, false];
})() as [BigIntConstructor, boolean];

/** @internal */
const [BigInt64ArrayCtor, BigInt64ArrayAvailable] = (() => {
    const BigInt64ArrayUnavailableError = () => { throw new Error('BigInt64Array is not available in this environment'); };
    class BigInt64ArrayUnavailable {
        static get BYTES_PER_ELEMENT() { return 8; }
        static of() { throw BigInt64ArrayUnavailableError(); }
        static from() { throw BigInt64ArrayUnavailableError(); }
        constructor() { throw BigInt64ArrayUnavailableError(); }
    }
    return typeof BigInt64Array !== 'undefined' ? [BigInt64Array, true] : [<any> BigInt64ArrayUnavailable, false];
})() as [BigInt64ArrayConstructor, boolean];

/** @internal */
const [BigUint64ArrayCtor, BigUint64ArrayAvailable] = (() => {
    const BigUint64ArrayUnavailableError = () => { throw new Error('BigUint64Array is not available in this environment'); };
    class BigUint64ArrayUnavailable {
        static get BYTES_PER_ELEMENT() { return 8; }
        static of() { throw BigUint64ArrayUnavailableError(); }
        static from() { throw BigUint64ArrayUnavailableError(); }
        constructor() { throw BigUint64ArrayUnavailableError(); }
    }
    return typeof BigUint64Array !== 'undefined' ? [BigUint64Array, true] : [<any> BigUint64ArrayUnavailable, false];
})() as [BigUint64ArrayConstructor, boolean];

export { BigIntCtor as BigInt, BigIntAvailable };
export { BigInt64ArrayCtor as BigInt64Array, BigInt64ArrayAvailable };
export { BigUint64ArrayCtor as BigUint64Array, BigUint64ArrayAvailable };

/** @internal */ const isNumber = (x: any) => typeof x === 'number';
/** @internal */ const isBoolean = (x: any) => typeof x === 'boolean';
/** @internal */ const isFunction = (x: any) => typeof x === 'function';
/** @internal */
// eslint-disable-next-line @typescript-eslint/ban-types
export const isObject = (x: any): x is Object => x != null && Object(x) === x;

/** @internal */
export const isPromise = <T = any>(x: any): x is PromiseLike<T> => {
    return isObject(x) && isFunction(x.then);
};

/** @internal */
export const isObservable = <T = any>(x: any): x is Observable<T> => {
    return isObject(x) && isFunction(x.subscribe);
};

/** @internal */
export const isIterable = <T = any>(x: any): x is Iterable<T> => {
    return isObject(x) && isFunction(x[Symbol.iterator]);
};

/** @internal */
export const isAsyncIterable = <T = any>(x: any): x is AsyncIterable<T> => {
    return isObject(x) && isFunction(x[Symbol.asyncIterator]);
};

/** @internal */
export const isArrowJSON = (x: any): x is ArrowJSONLike  => {
    return isObject(x) && isObject(x['schema']);
};

/** @internal */
export const isArrayLike = <T = any>(x: any): x is ArrayLike<T> => {
    return isObject(x) && isNumber(x['length']);
};

/** @internal */
export const isIteratorResult = <T = any>(x: any): x is IteratorResult<T> => {
    return isObject(x) && ('done' in x) && ('value' in x);
};

/** @internal */
export const isUnderlyingSink = <T = any>(x: any): x is UnderlyingSink<T> => {
    return isObject(x) &&
        isFunction(x['abort']) &&
        isFunction(x['close']) &&
        isFunction(x['start']) &&
        isFunction(x['write']);
};

/** @internal */
export const isFileHandle = (x: any): x is FileHandle => {
    return isObject(x) && isFunction(x['stat']) && isNumber(x['fd']);
};

/** @internal */
export const isFSReadStream = (x: any): x is FSReadStream => {
    return isReadableNodeStream(x) && isNumber((<any> x)['bytesRead']);
};

/** @internal */
export const isFetchResponse = (x: any): x is Response => {
    return isObject(x) && isReadableDOMStream(x['body']);
};

/** @internal */
export const isWritableDOMStream = <T = any>(x: any): x is WritableStream<T> => {
    return isObject(x) &&
        isFunction(x['abort']) &&
        isFunction(x['getWriter']) &&
        !(x instanceof ReadableInterop);
};

/** @internal */
export const isReadableDOMStream = <T = any>(x: any): x is ReadableStream<T> => {
    return isObject(x) &&
        isFunction(x['cancel']) &&
        isFunction(x['getReader']) &&
        !(x instanceof ReadableInterop);
};

/** @internal */
export const isWritableNodeStream = (x: any): x is NodeJS.WritableStream => {
    return isObject(x) &&
        isFunction(x['end']) &&
        isFunction(x['write']) &&
        isBoolean(x['writable']) &&
        !(x instanceof ReadableInterop);
};

/** @internal */
export const isReadableNodeStream = (x: any): x is NodeJS.ReadableStream => {
    return isObject(x) &&
        isFunction(x['read']) &&
        isFunction(x['pipe']) &&
        isBoolean(x['readable']) &&
        !(x instanceof ReadableInterop);
};
