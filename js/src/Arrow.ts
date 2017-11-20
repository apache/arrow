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

import { Table } from './vector/table';
import { Vector } from './vector/vector';
import { Utf8Vector } from './vector/utf8';
import { DictionaryVector } from './vector/dictionary';
import { StructVector, StructRow } from './vector/struct';
import { readVectors, readVectorsAsync } from './reader/arrow';
import { ListVector, BinaryVector, FixedSizeListVector } from './vector/list';

import {
    BoolVector,
    Int8Vector,
    Int16Vector,
    Int32Vector,
    Int64Vector,
    Uint8Vector,
    Uint16Vector,
    Uint32Vector,
    Uint64Vector,
    Float16Vector,
    Float32Vector,
    Float64Vector,
    Date32Vector,
    Date64Vector,
    Time32Vector,
    Time64Vector,
    DecimalVector,
    TimestampVector,
} from './vector/numeric';

export { Table, Vector, StructRow };
export { readVectors, readVectorsAsync };
export { NumericVectorConstructor } from './vector/numeric';
export { List, TypedArray, TypedArrayConstructor } from './vector/types';
export {
    BoolVector,
    ListVector,
    Utf8Vector,
    Int8Vector,
    Int16Vector,
    Int32Vector,
    Int64Vector,
    Uint8Vector,
    Uint16Vector,
    Uint32Vector,
    Uint64Vector,
    Date32Vector,
    Date64Vector,
    Time32Vector,
    Time64Vector,
    BinaryVector,
    StructVector,
    Float16Vector,
    Float32Vector,
    Float64Vector,
    DecimalVector,
    TimestampVector,
    DictionaryVector,
    FixedSizeListVector,
};

/* These exports are needed for the closure umd targets */
try {
    const Arrow = eval('exports');
    if (typeof Arrow === 'object') {
        // string indexers tell closure compiler not to rename these properties
        Arrow['readVectors'] = readVectors;
        Arrow['readVectorsAsync'] = readVectorsAsync;
        Arrow['Table'] = Table;
        Arrow['Vector'] = Vector;
        Arrow['StructRow'] = StructRow;
        Arrow['BoolVector'] = BoolVector;
        Arrow['ListVector'] = ListVector;
        Arrow['Utf8Vector'] = Utf8Vector;
        Arrow['Int8Vector'] = Int8Vector;
        Arrow['Int16Vector'] = Int16Vector;
        Arrow['Int32Vector'] = Int32Vector;
        Arrow['Int64Vector'] = Int64Vector;
        Arrow['Uint8Vector'] = Uint8Vector;
        Arrow['Uint16Vector'] = Uint16Vector;
        Arrow['Uint32Vector'] = Uint32Vector;
        Arrow['Uint64Vector'] = Uint64Vector;
        Arrow['Date32Vector'] = Date32Vector;
        Arrow['Date64Vector'] = Date64Vector;
        Arrow['Time32Vector'] = Time32Vector;
        Arrow['Time64Vector'] = Time64Vector;
        Arrow['BinaryVector'] = BinaryVector;
        Arrow['StructVector'] = StructVector;
        Arrow['Float16Vector'] = Float16Vector;
        Arrow['Float32Vector'] = Float32Vector;
        Arrow['Float64Vector'] = Float64Vector;
        Arrow['DecimalVector'] = DecimalVector;
        Arrow['TimestampVector'] = TimestampVector;
        Arrow['DictionaryVector'] = DictionaryVector;
        Arrow['FixedSizeListVector'] = FixedSizeListVector;
    }
} catch (e) { /* not the UMD bundle */ }
/* end closure exports */
