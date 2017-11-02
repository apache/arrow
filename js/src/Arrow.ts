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

import { readBuffers } from './reader/arrow';

import { Vector } from './types/types';
import { ListVector } from './types/list';
import { Utf8Vector } from './types/utf8';
import { BoolVector } from './types/vector/bool';
import { DateVector } from './types/vector/date';
import { RowVector } from './types/table/row';
import { TableVector } from './types/table/table';
import { StructVector } from './types/table/struct';
import { DictionaryVector } from './types/dictionary';
import { FixedSizeListVector } from './types/fixedsizelist';
import { LongVector, Int64Vector, Uint64Vector, } from './types/vector/long';
import {
    TypedVector,
    Int8Vector,
    Int16Vector,
    Int32Vector,
    Uint8Vector,
    Uint16Vector,
    Uint32Vector,
    Float32Vector,
    Float64Vector
} from './types/vector/typed';

import './types/table/from';

export {
    Vector,
    readBuffers,
    DictionaryVector,
    RowVector as Row,
    TableVector as Table,
    StructVector, Utf8Vector,
    ListVector, FixedSizeListVector,
    BoolVector, TypedVector, LongVector,
    DateVector, Float32Vector, Float64Vector,
    Int8Vector, Int16Vector, Int32Vector, Int64Vector,
    Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector,
};

/* These exports are needed for the closure umd targets */
try {
    const Arrow = eval('exports');
    if (typeof Arrow === 'object') {
        // string indexers tell closure compiler not to rename these properties
        Arrow['Vector'] = Vector;
        Arrow['Table'] = TableVector;
        Arrow['readBuffers'] = readBuffers;
        Arrow['BoolVector'] = BoolVector;
        Arrow['Utf8Vector'] = Utf8Vector;
        Arrow['ListVector'] = ListVector;
        Arrow['StructVector'] = StructVector;
        Arrow['DictionaryVector'] = DictionaryVector;
        Arrow['FixedSizeListVector'] = FixedSizeListVector;
        Arrow['LongVector'] = LongVector;
        Arrow['TypedVector'] = TypedVector;
        Arrow['DateVector'] = DateVector;
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
    }
} catch (e) { /* not the UMD bundle */ }
/* end closure exports */
