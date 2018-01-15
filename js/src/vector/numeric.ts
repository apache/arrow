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

import { Vector } from './vector';
import { VirtualVector } from './virtual';
import { TypedArray, TypedArrayConstructor } from './types';

export class NumericVector<T, TArray extends TypedArray> extends Vector<T> {
    readonly data: TArray;
    readonly length: number;
    readonly stride: number;
    constructor(argv: { data: TArray }) {
        super();
        const data = (ArrayBuffer.isView(argv) ? argv : argv.data) as TArray;
        this.length = ((this.data = data).length / this.stride) | 0;
    }
    get(index: number) {
        return this.data[index] as any;
    }
    concat(...vectors: Vector<T>[]): Vector<T> {
        return new VirtualVector(this.data.constructor as TypedArrayConstructor, this, ...vectors);
    }
    slice<R = TArray>(start?: number, end?: number): R {
        const { data, stride } = this, from = start! | 0;
        const to = end === undefined ? data.length : Math.max(end | 0, from);
        return data.subarray(Math.min(from, to) * stride | 0, to * stride | 0) as any as R;
    }
}

export class FixedWidthNumericVector<T, TArray extends TypedArray> extends NumericVector<T, TArray> {
    get(index: number) {
        return this.data.slice(this.stride * index, this.stride * (index + 1)) as TArray;
    }
}

export class BoolVector extends NumericVector<boolean, Uint8Array> {
    static pack(values: Iterable<any>) {
        let n = 0, i = 0;
        let xs: number[] = [];
        let bit = 0, byte = 0;
        for (const value of values) {
            value && (byte |= 1 << bit);
            if (++bit === 8) {
                xs[i++] = byte;
                byte = bit = 0;
            }
        }
        if (i === 0 || bit > 0) { xs[i++] = byte; }
        if (i % 8 && (n = i + 8 - i % 8)) {
            do { xs[i] = 0; } while (++i < n);
        }
        return new Uint8Array(xs);
    }
    get(index: number) {
        return (this.data[index >> 3] & 1 << index % 8) !== 0;
    }
    set(index: number, value: boolean) {
        if (index > -1 === false) {
            return;
        } else if (value) {
            this.data[index >> 3] |=  (1 << (index % 8));
        } else {
            this.data[index >> 3] &= ~(1 << (index % 8));
        }
    }
}

export class Int8Vector extends NumericVector<number, Int8Array> {}
export class Int16Vector extends NumericVector<number, Int16Array> {}
export class Int32Vector extends NumericVector<number, Int32Array> {}
export class Int64Vector extends FixedWidthNumericVector<number, Int32Array> {}

export class Uint8Vector extends NumericVector<number, Uint8Array> {}
export class Uint16Vector extends NumericVector<number, Uint16Array> {}
export class Uint32Vector extends NumericVector<number, Uint32Array> {}
export class Uint64Vector extends FixedWidthNumericVector<number, Uint32Array> {}

export class Float16Vector extends NumericVector<number, Uint16Array> {
    get(index: number) {
        return Math.min((super.get(index)! -  32767) / 32767, 1);
    }
}

export class Float32Vector extends NumericVector<number, Float32Array> {}
export class Float64Vector extends NumericVector<number, Float64Array> {}

export class Date32Vector extends NumericVector<Date, Int32Array> {
    public readonly unit: string;
    constructor(argv: { data: Int32Array, unit: string }) {
        super(argv);
        this.unit = argv.unit;
    }
    get(index: number): Date {
        return new Date(86400000 * (super.get(index) as any));
    }
}

export class Date64Vector extends NumericVector<Date, Int32Array> {
    public readonly unit: string;
    constructor(argv: { unit: string, data: Int32Array }) {
        super(argv);
        this.unit = argv.unit;
    }
    get(index: number): Date {
        return new Date(4294967296   * /* 2^32 */
            (super.get(index * 2 + 1) as any) + /* high */
            (super.get(index * 2) as any)       /*  low */
        );
    }
}

export class Time32Vector extends NumericVector<number, Int32Array> {
    public readonly unit: string;
    constructor(argv: { data: Int32Array, unit: string }) {
        super(argv);
        this.unit = argv.unit;
    }
}

export class Time64Vector extends FixedWidthNumericVector<number, Uint32Array> {
    public readonly unit: string;
    constructor(argv: { unit: string, data: Uint32Array }) {
        super(argv);
        this.unit = argv.unit;
    }
}

export class DecimalVector extends FixedWidthNumericVector<number, Uint32Array> {
    readonly scale: number;
    readonly precision: number;
    constructor(argv: { precision: number, scale: number, data: Uint32Array }) {
        super(argv);
        this.scale = argv.scale;
        this.precision = argv.precision;
    }
}

export class TimestampVector extends FixedWidthNumericVector<number, Uint32Array> {
    readonly unit: string;
    readonly timezone: string;
    constructor(argv: { unit: string, timezone: string, data: Uint32Array }) {
        super(argv);
        this.unit = argv.unit;
        this.timezone = argv.timezone;
    }
}

export interface NumericVectorConstructor<T, TArray extends TypedArray> {
    readonly prototype: NumericVector<T, TArray>;
    new (argv: { data: TArray }): NumericVector<T, TArray>;
}

(DecimalVector.prototype as any).stride = 4;
(NumericVector.prototype as any).stride = 1;
(FixedWidthNumericVector.prototype as any).stride = 2;
