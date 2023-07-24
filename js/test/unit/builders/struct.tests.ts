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

import { Field, FixedSizeList, Float64, makeBuilder, Struct } from 'apache-arrow';

describe('StructBuilder', () => {
    test('Appending Null', () => {
        const listField = new Field('l', new FixedSizeList(2, new Field('item', new Float64())), true);

        const builder = makeBuilder({
            type: new Struct([listField]),
            nullValues: [null, undefined]
        });

        builder.append(null);

        expect(builder.numChildren).toBe(1);
        expect(builder.nullCount).toBe(1);
        expect(builder).toHaveLength(1);
        expect(builder.children[0].numChildren).toBe(1);
        expect(builder.children[0].nullCount).toBe(1);
        expect(builder.children[0]).toHaveLength(1);
    });
});
