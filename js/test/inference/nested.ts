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

import { Data } from '../../src/data';
import { Field } from '../../src/schema';
import { DataType } from '../../src/type';
import { Vector, BoolVector } from '../../src/vector/index';
import { RowProxyGenerator as Row } from '../../src/vector/row';
import { Bool, Int8, Utf8, List, Dictionary, Struct, Map_ } from '../../src/type';

type NamedSchema = { a: Int8, b: Utf8, c: Dictionary<List<Bool>>; [idx: string]: DataType; };
type IndexSchema = { 0: Int8, 1: Utf8, 2: Dictionary<List<Bool>>; [idx: number]: DataType; };

checkIndexTypes({ 0: new Int8(), 1: new Utf8(), 2: new Dictionary<List<Bool>>(null!, null!) } as IndexSchema);
checkNamedTypes({ a: new Int8(), b: new Utf8(), c: new Dictionary<List<Bool>>(null!, null!) } as NamedSchema);

function checkIndexTypes(schema: IndexSchema) {

    const data = Data.Struct(new Struct<IndexSchema>(
        Object.keys(schema).map((x) => new Field(x, schema[(<any> x)]))
    ), 0, 0, 0, null, []);

    const row = Row.new(Vector.new(data), schema).bind(0);

    const check_0 = (x = row[0]) => expect(typeof x === 'number').toBe(true);
    const check_1 = (x = row[1]) => expect(typeof x === 'string').toBe(true);
    const check_2 = (x = row[2]) => expect(x instanceof BoolVector).toBe(true);

    check_0() && check_0(row[0]) && check_0(row.get(0));
    check_1() && check_1(row[1]) && check_1(row.get(1));
    check_2() && check_2(row[2]) && check_2(row.get(2));
}

function checkNamedTypes(schema: NamedSchema) {

    const data = Data.Map(new Map_<NamedSchema>(
        Object.keys(schema).map((x) => new Field(x, schema[x]))
    ), 0, 0, 0, null, []);

    const row = Row.new(Vector.new(data), schema).bind(0);

    const check_a = (x = row.a) => expect(typeof x === 'number').toBe(true);
    const check_b = (x = row.b) => expect(typeof x === 'string').toBe(true);
    const check_c = (x = row.c) => expect(x instanceof BoolVector).toBe(true);

    check_a() && check_a(row.a) && check_a(row.get('a'));
    check_b() && check_b(row.b) && check_b(row.get('b'));
    check_c() && check_c(row.c) && check_c(row.get('c'));
}
