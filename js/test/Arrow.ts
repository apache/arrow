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

/* tslint:disable */
// Dynamically load an Ix target build based on command line arguments

const target = process.env.TEST_TARGET;
const format = process.env.TEST_MODULE;
const resolve = require('path').resolve;

// these are duplicated in the gulpfile :<
const targets = [`es5`, `es2015`, `esnext`];
const formats = [`cjs`, `esm`, `cls`, `umd`];

function throwInvalidImportError(name: string, value: string, values: string[]) {
    throw new Error('Unrecognized ' + name + ' \'' + value + '\'. Please run tests with \'--' + name + ' <any of ' + values.join(', ') + '>\'');
}

if (!~targets.indexOf(target)) throwInvalidImportError('target', target, targets);
if (!~formats.indexOf(format)) throwInvalidImportError('module', format, formats);

let Arrow: any = require(resolve(`./targets/${target}/${format}/Arrow.js`));

import {
    Table as Table_,
    readBuffers as readBuffers_,
    Vector as Vector_,
    BitVector as BitVector_,
    ListVector as ListVector_,
    Utf8Vector as Utf8Vector_,
    DateVector as DateVector_,
    IndexVector as IndexVector_,
    TypedVector as TypedVector_,
    Int8Vector as Int8Vector_,
    Int16Vector as Int16Vector_,
    Int32Vector as Int32Vector_,
    Int64Vector as Int64Vector_,
    Uint8Vector as Uint8Vector_,
    Uint16Vector as Uint16Vector_,
    Uint32Vector as Uint32Vector_,
    Uint64Vector as Uint64Vector_,
    Float32Vector as Float32Vector_,
    Float64Vector as Float64Vector_,
    StructVector as StructVector_,
    DictionaryVector as DictionaryVector_,
    FixedSizeListVector as FixedSizeListVector_,
} from '../src/Arrow';

export let Table = Arrow.Table as typeof Table_;
export let readBuffers = Arrow.readBuffers as typeof readBuffers_;
export let Vector = Arrow.Vector as typeof Vector_;
export let BitVector = Arrow.BitVector as typeof BitVector_;
export let ListVector = Arrow.ListVector as typeof ListVector_;
export let Utf8Vector = Arrow.Utf8Vector as typeof Utf8Vector_;
export let DateVector = Arrow.DateVector as typeof DateVector_;
export let IndexVector = Arrow.IndexVector as typeof IndexVector_;
export let TypedVector = Arrow.TypedVector as typeof TypedVector_;
export let Int8Vector = Arrow.Int8Vector as typeof Int8Vector_;
export let Int16Vector = Arrow.Int16Vector as typeof Int16Vector_;
export let Int32Vector = Arrow.Int32Vector as typeof Int32Vector_;
export let Int64Vector = Arrow.Int64Vector as typeof Int64Vector_;
export let Uint8Vector = Arrow.Uint8Vector as typeof Uint8Vector_;
export let Uint16Vector = Arrow.Uint16Vector as typeof Uint16Vector_;
export let Uint32Vector = Arrow.Uint32Vector as typeof Uint32Vector_;
export let Uint64Vector = Arrow.Uint64Vector as typeof Uint64Vector_;
export let Float32Vector = Arrow.Float32Vector as typeof Float32Vector_;
export let Float64Vector = Arrow.Float64Vector as typeof Float64Vector_;
export let StructVector = Arrow.StructVector as typeof StructVector_;
export let DictionaryVector = Arrow.DictionaryVector as typeof DictionaryVector_;
export let FixedSizeListVector = Arrow.FixedSizeListVector as typeof FixedSizeListVector_;
