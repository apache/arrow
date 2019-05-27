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

import { bignumToBigInt } from '../util/bn';
import { BigInt64Array } from '../util/compat';
import { FlatBuilder, BuilderOptions } from './base';
import { Int, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64 } from '../type';

export interface IntBuilder<T extends Int = Int, TNull = any> extends FlatBuilder<T, TNull> {
    nullBitmap: Uint8Array; values: T['TArray'];
}

export interface Int8Builder<TNull = any> extends IntBuilder<Int8, TNull> {}
export interface Int16Builder<TNull = any> extends IntBuilder<Int16, TNull> {}
export interface Int32Builder<TNull = any> extends IntBuilder<Int32, TNull> {}
export interface Int64Builder<TNull = any> extends IntBuilder<Int64, TNull> {}
export interface Uint8Builder<TNull = any> extends IntBuilder<Uint8, TNull> {}
export interface Uint16Builder<TNull = any> extends IntBuilder<Uint16, TNull> {}
export interface Uint32Builder<TNull = any> extends IntBuilder<Uint32, TNull> {}
export interface Uint64Builder<TNull = any> extends IntBuilder<Uint64, TNull> {}

export class IntBuilder<T extends Int = Int, TNull = any> extends FlatBuilder<T, TNull> {}

export class Int8Builder<TNull = any> extends IntBuilder<Int8, TNull> {}
export class Int16Builder<TNull = any> extends IntBuilder<Int16, TNull> {}
export class Int32Builder<TNull = any> extends IntBuilder<Int32, TNull> {}
export class Int64Builder<TNull = any> extends IntBuilder<Int64, TNull> {
    constructor(options: BuilderOptions<Int64, TNull>) {
        if (options['nullValues']) {
            options['nullValues'] = (options['nullValues'] as TNull[]).map(toMaybeBigInt);
        }
        super(options);
    }
    isValid(value: Int32Array | bigint | TNull) {
        return this._isValid(toMaybeBigInt(value));
    }
}

export class Uint8Builder<TNull = any> extends IntBuilder<Uint8, TNull> {}
export class Uint16Builder<TNull = any> extends IntBuilder<Uint16, TNull> {}
export class Uint32Builder<TNull = any> extends IntBuilder<Uint32, TNull> {}
export class Uint64Builder<TNull = any> extends IntBuilder<Uint64, TNull> {
    constructor(options: BuilderOptions<Uint64, TNull>) {
        if (options['nullValues']) {
            options['nullValues'] = (options['nullValues'] as TNull[]).map(toMaybeBigInt);
        }
        super(options);
    }
    isValid(value: Uint32Array | bigint | TNull) {
        return this._isValid(toMaybeBigInt(value));
    }
}

const toMaybeBigInt = ((memo: any) => (value: any) => {
    if (ArrayBuffer.isView(value)) {
        memo.buffer = value.buffer;
        memo.byteOffset = value.byteOffset;
        memo.byteLength = value.byteLength;
        value = bignumToBigInt(memo);
        memo.buffer = null;
    }
    return value;
})({ 'BigIntArray': BigInt64Array });
