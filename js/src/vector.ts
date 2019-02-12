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

import { Data } from './data';
import { DataType } from './type';
import { Chunked } from './vector/chunked';

/** @ignore */
export interface Clonable<R extends Vector> {
    clone(...args: any[]): R;
}

/** @ignore */
export interface Sliceable<R extends Vector> {
    slice(begin?: number, end?: number): R;
}

/** @ignore */
export interface Applicative<T extends DataType, R extends Chunked> {
    concat(...others: Vector<T>[]): R;
    readonly [Symbol.isConcatSpreadable]: boolean;
}

export interface AbstractVector<T extends DataType = any>
    extends Clonable<Vector<T>>,
            Sliceable<Vector<T>>,
            Applicative<T, Chunked<T>> {

    readonly TType: T['TType'];
    readonly TArray: T['TArray'];
    readonly TValue: T['TValue'];
}

export abstract class AbstractVector<T extends DataType = any> implements Iterable<T['TValue'] | null> {

    public abstract readonly data: Data<T>;
    public abstract readonly type: T;
    public abstract readonly typeId: T['TType'];
    public abstract readonly length: number;
    public abstract readonly stride: number;
    public abstract readonly nullCount: number;
    public abstract readonly numChildren: number;

    public abstract readonly ArrayType: T['ArrayType'];

    public abstract isValid(index: number): boolean;
    public abstract get(index: number): T['TValue'] | null;
    public abstract set(index: number, value: T['TValue'] | null): void;
    public abstract indexOf(value: T['TValue'] | null, fromIndex?: number): number;
    public abstract [Symbol.iterator](): IterableIterator<T['TValue'] | null>;

    public abstract toArray(): T['TArray'];
    public abstract getChildAt<R extends DataType = any>(index: number): Vector<R> | null;
}

export { AbstractVector as Vector };
