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

/* eslint-disable jest/no-standalone-expect */

import { Data } from 'apache-arrow/data';
import { Field } from 'apache-arrow/schema';
import { Column } from 'apache-arrow/column';
import { Vector } from 'apache-arrow/vector';
import { Bool, Int8, Utf8, List, Dictionary, Struct } from 'apache-arrow/type';

const boolType = new Bool();
const boolVector = Vector.new(Data.Bool(boolType, 0, 10, 0, null, new Uint8Array(2)));

const boolColumn = new Column(new Field('bool', boolType), [
    Vector.new(Data.Bool(boolType, 0, 10, 0, null, new Uint8Array(2))),
    Vector.new(Data.Bool(boolType, 0, 10, 0, null, new Uint8Array(2))),
    Vector.new(Data.Bool(boolType, 0, 10, 0, null, new Uint8Array(2))),
]);

expect(typeof boolVector.get(0) === 'boolean').toBe(true);
expect(typeof boolColumn.get(0) === 'boolean').toBe(true);

type IndexSchema = {
    0: Int8;
    1: Utf8;
    2: Dictionary<List<Bool>>;
};

const structChildFields = [
    { name: 0, type: new Int8() },
    { name: 1, type: new Utf8() },
    { name: 2, type: new Dictionary<List<Bool>>(null!, null!) }
].map(({ name, type }) => new Field('' + name, type));

const structType = new Struct<IndexSchema>(structChildFields);
const structVector = Vector.new(Data.Struct(structType, 0, 0, 0, null, []));
const structColumn = new Column(new Field('struct', structType), [
    Vector.new(Data.Struct(structType, 0, 0, 0, null, [])),
    Vector.new(Data.Struct(structType, 0, 0, 0, null, [])),
    Vector.new(Data.Struct(structType, 0, 0, 0, null, [])),
]);

const [x1, y1, z1] = structVector.get(0)!;
const [x2, y2, z2] = structColumn.get(0)!;

console.log(x1, y1, z1);
console.log(x2, y2, z2);
