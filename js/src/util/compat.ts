export interface Subscription {
    unsubscribe: () => void;
}

export interface Observer<T> {
    closed?: boolean;
    next: (value: T) => void;
    error: (err: any) => void;
    complete: () => void;
}

export interface Observable<T> {
    subscribe: (observer: Observer<T>) => Subscription;
}

/**
 * @ignore
 */
export function isPromise(x: any): x is PromiseLike<any> {
    return x != null && Object(x) === x && typeof x['then'] === 'function';
}

/**
 * @ignore
 */
export function isObservable(x: any): x is Observable<any> {
    return x != null && Object(x) === x && typeof x['subscribe'] === 'function';
}

/**
 * @ignore
 */
export function isArrayLike(x: any): x is ArrayLike<any> {
    return x != null && Object(x) === x && typeof x['length'] === 'number';
}

/**
 * @ignore
 */
export function isIterable(x: any): x is Iterable<any> {
    return x != null && Object(x) === x && typeof x[Symbol.iterator] !== 'undefined';
}

/**
 * @ignore
 */
export function isAsyncIterable(x: any): x is AsyncIterable<any> {
    return x != null && Object(x) === x && typeof x[Symbol.asyncIterator] !== 'undefined';
}
