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
import { TextDecoder } from 'text-encoding';
import { IndexVector, BitVector, ValidityArgs } from './typed';

export class ListVectorBase<T> extends Vector<T> {
    protected values: Vector<T>;
    protected offsets: IndexVector;
    constructor(validity: ValidityArgs, values: Vector<any>, offsets: IndexVector) {
        super();
        this.values = values;
        this.offsets = offsets;
        validity && (this.validity = BitVector.from(validity));
    }
    get(index: number) {
        let batch, from, to, { offsets } = this;
        if (!this.validity.get(index) ||
            /* return null if `to` is null */
            ((to = offsets.get(index + 1)) === null) || !(
            /*
            return null if `batch` is less than than 0. this check is placed
            second to avoid creating the [from, batch] tuple if `to` is null
            */
            ([from, batch] = offsets.get(index, true) as number[]) && batch > -1)) {
            return null;
        }
        return this.values.slice(from, to, batch) as any;
    }
    concat(vector: ListVectorBase<T>) {
        return (this.constructor as typeof ListVectorBase).from(this,
            this.length + vector.length,
            this.validity.concat(vector.validity),
            this.values.concat(vector.values),
            this.offsets.concat(vector.offsets)
        );
    }
    *[Symbol.iterator]() {
        let v, r1, r2, { values } = this;
        let it = this.offsets[Symbol.iterator]();
        let iv = this.validity[Symbol.iterator]();
        while (!(v = iv.next()).done && !(r1 = it.next()).done && !(r2 = it.next()).done) {
            yield !v.value ? null : values.slice(r1.value[0], r2.value, r1.value[1]) as any;
        }
    }
}

export class ListVector<T> extends ListVectorBase<T[]> {}
export class Utf8Vector extends ListVectorBase<string> {
    protected static decoder = new TextDecoder(`utf-8`);
    get(index: number) {
        let chars = super.get(index) as any;
        return chars ? Utf8Vector.decoder.decode(chars) : null;
    }
    *[Symbol.iterator]() {
        let decoder = Utf8Vector.decoder;
        for (const chars of super[Symbol.iterator]()) {
            yield !chars ? null : decoder.decode(chars);
        }
    }
}

export class FixedSizeListVector<T> extends Vector<T[]> {
    protected size: number;
    protected values: Vector<T>;
    constructor(size: number, validity: ValidityArgs, values: Vector<T>) {
        super();
        this.values = values;
        this.size = Math.abs(size | 0) || 1;
        validity && (this.validity = BitVector.from(validity));
    }
    get(index: number) {
        return !this.validity.get(index) ? null : this.values.slice(
            this.size * index, this.size * (index + 1)
        ) as T[];
    }
    concat(vector: FixedSizeListVector<T>) {
        return FixedSizeListVector.from(this,
            this.length + vector.length,
            this.size,
            this.validity.concat(vector.validity),
            this.values.concat(vector.values)
        );
    }
    *[Symbol.iterator]() {
        let v, i = -1;
        let { size, length, values } = this;
        let iv = this.validity[Symbol.iterator]();
        while (!(v = iv.next()).done && ++i < length) {
            yield !v.value ? null : values.slice(size * i, size * (i + 1)) as T[];
        }
    }
}
