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

import { Vector as Vector_ } from './vector/vector';
import { StructVector as StructVector_ } from './vector/struct';
import { DictionaryVector as DictionaryVector_ } from './vector/dictionary';
import { ListVector as ListVector_, Utf8Vector as Utf8Vector_, FixedSizeListVector as FixedSizeListVector_ } from './vector/list';
import {
    TypedVector as TypedVector_, BitVector as BitVector_,
    DateVector as DateVector_, IndexVector as IndexVector_,
    Int8Vector as Int8Vector_, Int16Vector as Int16Vector_,
    Int32Vector as Int32Vector_, Int64Vector as Int64Vector_,
    Uint8Vector as Uint8Vector_, Uint16Vector as Uint16Vector_,
    Uint32Vector as Uint32Vector_, Uint64Vector as Uint64Vector_,
    Float32Vector as Float32Vector_, Float64Vector as Float64Vector_,
} from './vector/typed';

export const vectors = {
    Vector: Vector_,
    BitVector: BitVector_,
    ListVector: ListVector_,
    Utf8Vector: Utf8Vector_,
    DateVector: DateVector_,
    IndexVector: IndexVector_,
    TypedVector: TypedVector_,
    Int8Vector: Int8Vector_,
    Int16Vector: Int16Vector_,
    Int32Vector: Int32Vector_,
    Int64Vector: Int64Vector_,
    Uint8Vector: Uint8Vector_,
    Uint16Vector: Uint16Vector_,
    Uint32Vector: Uint32Vector_,
    Uint64Vector: Uint64Vector_,
    Float32Vector: Float32Vector_,
    Float64Vector: Float64Vector_,
    StructVector: StructVector_,
    DictionaryVector: DictionaryVector_,
    FixedSizeListVector: FixedSizeListVector_,
};

export namespace vectors {
    export type Vector<T> =  Vector_<T>;
    export type BitVector =  BitVector_;
    export type ListVector<T> =  ListVector_<T>;
    export type Utf8Vector =  Utf8Vector_;
    export type DateVector =  DateVector_;
    export type IndexVector =  IndexVector_;
    export type Int8Vector =  Int8Vector_;
    export type Int16Vector =  Int16Vector_;
    export type Int32Vector =  Int32Vector_;
    export type Int64Vector =  Int64Vector_;
    export type Uint8Vector =  Uint8Vector_;
    export type Uint16Vector =  Uint16Vector_;
    export type Uint32Vector =  Uint32Vector_;
    export type Uint64Vector =  Uint64Vector_;
    export type Float32Vector =  Float32Vector_;
    export type Float64Vector =  Float64Vector_;
    export type StructVector =  StructVector_;
    export type DictionaryVector<T> =  DictionaryVector_<T>;
    export type FixedSizeListVector<T> =  FixedSizeListVector_<T>;
    export type TypedVector<T, TArray> =  TypedVector_<T, TArray>;
}

/* These exports are needed for the closure umd targets */
try {
    const Arrow = eval('exports');
    if (typeof Arrow === 'object') {
        // string indexers tell closure compiler not to rename these properties
        Arrow['vectors'] = {};
        Arrow['vectors']['Vector'] = Vector_;
        Arrow['vectors']['BitVector'] = BitVector_;
        Arrow['vectors']['ListVector'] = ListVector_;
        Arrow['vectors']['Utf8Vector'] = Utf8Vector_;
        Arrow['vectors']['DateVector'] = DateVector_;
        Arrow['vectors']['IndexVector'] = IndexVector_;
        Arrow['vectors']['Int8Vector'] = Int8Vector_;
        Arrow['vectors']['Int16Vector'] = Int16Vector_;
        Arrow['vectors']['Int32Vector'] = Int32Vector_;
        Arrow['vectors']['Int64Vector'] = Int64Vector_;
        Arrow['vectors']['Uint8Vector'] = Uint8Vector_;
        Arrow['vectors']['Uint16Vector'] = Uint16Vector_;
        Arrow['vectors']['Uint32Vector'] = Uint32Vector_;
        Arrow['vectors']['Uint64Vector'] = Uint64Vector_;
        Arrow['vectors']['Float32Vector'] = Float32Vector_;
        Arrow['vectors']['Float64Vector'] = Float64Vector_;
        Arrow['vectors']['StructVector'] = StructVector_;
        Arrow['vectors']['DictionaryVector'] = DictionaryVector_;
        Arrow['vectors']['FixedSizeListVector'] = FixedSizeListVector_;
    }
} catch (e) { /* not the UMD bundle */ }
/** end closure exports */
