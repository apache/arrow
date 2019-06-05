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
import { WideBufferBuilder } from './buffer';
import { FixedWidthBuilder, BuilderOptions } from '../builder';
import { BigInt64ArrayAvailable, BigInt64Array } from '../util/compat';
import { BigUint64ArrayAvailable, BigUint64Array } from '../util/compat';
import { Int, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64 } from '../type';

/** @ignore */
export class IntBuilder<T extends Int = Int, TNull = any> extends FixedWidthBuilder<T, TNull> {
    public setValue(index: number, value: T['TValue']) {
        this._values.set(index, value);
    }
}

/** @ignore */
export class Int8Builder<TNull = any> extends IntBuilder<Int8, TNull> {}
/** @ignore */
export class Int16Builder<TNull = any> extends IntBuilder<Int16, TNull> {}
/** @ignore */
export class Int32Builder<TNull = any> extends IntBuilder<Int32, TNull> {}
/** @ignore */
export class Int64Builder<TNull = any> extends IntBuilder<Int64, TNull> {
    constructor(options: BuilderOptions<Int64, TNull>) {
        if (options['nullValues']) {
            options['nullValues'] = (options['nullValues'] as TNull[]).map(toBigInt);
        }
        super(options);
        if (BigInt64ArrayAvailable) {
            this._values = <any> new WideBufferBuilder(new BigInt64Array(0), 2);
        }
    }
    public get values64() { return (this._values as any).buffer64 as BigInt64Array; }
    public isValid(value: Int32Array | bigint | TNull) { return super.isValid(toBigInt(value)); }
}

/** @ignore */
export class Uint8Builder<TNull = any> extends IntBuilder<Uint8, TNull> {}
/** @ignore */
export class Uint16Builder<TNull = any> extends IntBuilder<Uint16, TNull> {}
/** @ignore */
export class Uint32Builder<TNull = any> extends IntBuilder<Uint32, TNull> {}
/** @ignore */
export class Uint64Builder<TNull = any> extends IntBuilder<Uint64, TNull> {
    constructor(options: BuilderOptions<Uint64, TNull>) {
        if (options['nullValues']) {
            options['nullValues'] = (options['nullValues'] as TNull[]).map(toBigInt);
        }
        super(options);
        if (BigUint64ArrayAvailable) {
            this._values = <any> new WideBufferBuilder(new BigUint64Array(0), 2);
        }
    }
    public get values64() { return (this._values as any).buffer64 as BigUint64Array; }
    public isValid(value: Uint32Array | bigint | TNull) { return super.isValid(toBigInt(value)); }
}

const toBigInt = ((memo: any) => (value: any) => {
    if (ArrayBuffer.isView(value)) {
        memo.buffer = value.buffer;
        memo.byteOffset = value.byteOffset;
        memo.byteLength = value.byteLength;
        value = bignumToBigInt(memo);
        memo.buffer = null;
    }
    return value;
})({ 'BigIntArray': BigInt64Array });
