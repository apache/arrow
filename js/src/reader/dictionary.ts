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

import { readVector } from './vector';
import { MessageBatch } from './message';
import { DictionaryVector } from '../types/dictionary';
import * as Schema_ from '../format/Schema_generated';
import { IteratorState, Dictionaries } from './arrow';
import Field = Schema_.org.apache.arrow.flatbuf.Field;

export function readDictionary<T>(field: Field, batch: MessageBatch, iterator: IteratorState, dictionaries: Dictionaries): DictionaryVector<T> | null {
    let vector: DictionaryVector<T> | null, id, encoding = field.dictionary();
    if (encoding && batch.id === (id = encoding.id().toFloat64().toString())) {
        return readVector<T>(field, batch, iterator, null) as DictionaryVector<T>;
    }
    for (let i = -1, n = field.childrenLength() | 0; ++i < n;) {
        if (vector = readDictionary<T>(field.children(i)!, batch, iterator, dictionaries)) {
            return vector;
        }
    }
    return null;
}
