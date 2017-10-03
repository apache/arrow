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

import { Table } from './table';
import { readBuffers } from './reader/arrow';
import { Vector } from './vector/vector';
import { StructVector } from './vector/struct';
import { DictionaryVector } from './vector/dictionary';
import { ListVector, Utf8Vector, FixedSizeListVector } from './vector/list';
import {
    TypedVector, BitVector,
    DateVector, IndexVector,
    Int8Vector, Int16Vector,
    Int32Vector, Int64Vector,
    Uint8Vector, Uint16Vector,
    Uint32Vector, Uint64Vector,
    Float32Vector, Float64Vector,
} from './vector/typed';

export {
    Table, readBuffers,
    Vector,
    BitVector,
    ListVector,
    Utf8Vector,
    DateVector,
    IndexVector,
    TypedVector,
    Int8Vector,
    Int16Vector,
    Int32Vector,
    Int64Vector,
    Uint8Vector,
    Uint16Vector,
    Uint32Vector,
    Uint64Vector,
    Float32Vector,
    Float64Vector,
    StructVector,
    DictionaryVector,
    FixedSizeListVector,
};

/* These exports are needed for the closure umd targets */
try {
    const Arrow = eval('exports');
    if (typeof Arrow === 'object') {
        // string indexers tell closure compiler not to rename these properties
        Arrow['Table'] = Table;
        Arrow['readBuffers'] = readBuffers;
        Arrow['Vector'] = Vector;
        Arrow['BitVector'] = BitVector;
        Arrow['ListVector'] = ListVector;
        Arrow['Utf8Vector'] = Utf8Vector;
        Arrow['DateVector'] = DateVector;
        Arrow['IndexVector'] = IndexVector;
        Arrow['TypedVector'] = TypedVector;
        Arrow['Int8Vector'] = Int8Vector;
        Arrow['Int16Vector'] = Int16Vector;
        Arrow['Int32Vector'] = Int32Vector;
        Arrow['Int64Vector'] = Int64Vector;
        Arrow['Uint8Vector'] = Uint8Vector;
        Arrow['Uint16Vector'] = Uint16Vector;
        Arrow['Uint32Vector'] = Uint32Vector;
        Arrow['Uint64Vector'] = Uint64Vector;
        Arrow['Float32Vector'] = Float32Vector;
        Arrow['Float64Vector'] = Float64Vector;
        Arrow['StructVector'] = StructVector;
        Arrow['DictionaryVector'] = DictionaryVector;
        Arrow['FixedSizeListVector'] = FixedSizeListVector;
    }
} catch (e) { /* not the UMD bundle */ }
/* end closure exports */
