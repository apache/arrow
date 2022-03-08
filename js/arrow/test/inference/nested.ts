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

import {
    Bool,
    DataType,
    Dictionary,
    Field,
    Int8,
    List,
    makeData,
    Struct,
    Utf8,
    Vector
} from 'apache-arrow';

type NamedSchema = { a: Int8; b: Utf8; c: Dictionary<List<Bool>>;[idx: string]: DataType };
type IndexSchema = { 0: Int8; 1: Utf8; 2: Dictionary<List<Bool>>;[idx: number]: DataType };

checkIndexTypes({ 0: new Int8(), 1: new Utf8(), 2: new Dictionary<List<Bool>>(null!, null!) } as IndexSchema);
checkNamedTypes({ a: new Int8(), b: new Utf8(), c: new Dictionary<List<Bool>>(null!, null!) } as NamedSchema);

function checkIndexTypes(schema: IndexSchema) {

    const data = makeData({
        type: new Struct<IndexSchema>(
            Object.keys(schema).map((x) => new Field(x, schema[(<any>x)]))),
        length: 0, offset: 0, nullCount: 0, nullBitmap: null, children: []
    });

    const row = new Vector([data]).get(0)!;

    const check_0 = (x = row[0]) => expect(typeof x === 'number').toBe(true);
    const check_1 = (x = row[1]) => expect(typeof x === 'string').toBe(true);
    const check_2 = (x = row[2]) => expect(x instanceof Vector).toBe(true);

    check_0();
    check_1();
    check_2();
    check_0(row[0]);
    check_1(row[1]);
    check_2(row[2]);
}

function checkNamedTypes(schema: NamedSchema) {

    const data = makeData({
        type: new Struct<NamedSchema>(
            Object.keys(schema).map((x) => new Field(x, schema[x]))),
        length: 0, offset: 0, nullCount: 0, nullBitmap: null, children: []
    });

    const row = new Vector([data]).get(0)!;

    const check_a = (x = row.a) => expect(typeof x === 'number').toBe(true);
    const check_b = (x = row.b) => expect(typeof x === 'string').toBe(true);
    const check_c = (x = row.c) => expect(x instanceof Vector).toBe(true);

    check_a();
    check_b();
    check_c();
    check_a(row.a);
    check_b(row.b);
    check_c(row.c);
}
