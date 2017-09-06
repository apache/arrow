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
let ArrowInternal: any = require(resolve(`./targets/${target}/${format}/Arrow.internal.js`));

import { vectors as vectors_ } from '../src/Arrow.internal';
import { Table as Table_, readBuffers as readBuffers_ } from '../src/Arrow';

export let Table = Arrow.Table as typeof Table_;
export let readBuffers = Arrow.readBuffers as typeof readBuffers_;

export let vectors: typeof vectors_ = ArrowInternal.vectors;
export namespace vectors {
    export type Vector<T> = vectors_.Vector<T>;
    export type BitVector = vectors_.BitVector;
    export type ListVector<T> = vectors_.ListVector<T>;
    export type Utf8Vector = vectors_.Utf8Vector;
    export type DateVector = vectors_.DateVector;
    export type IndexVector = vectors_.IndexVector;
    export type Int8Vector = vectors_.Int8Vector;
    export type Int16Vector = vectors_.Int16Vector;
    export type Int32Vector = vectors_.Int32Vector;
    export type Int64Vector = vectors_.Int64Vector;
    export type Uint8Vector = vectors_.Uint8Vector;
    export type Uint16Vector = vectors_.Uint16Vector;
    export type Uint32Vector = vectors_.Uint32Vector;
    export type Uint64Vector = vectors_.Uint64Vector;
    export type Float32Vector = vectors_.Float32Vector;
    export type Float64Vector = vectors_.Float64Vector;
    export type StructVector = vectors_.StructVector;
    export type DictionaryVector<T> = vectors_.DictionaryVector<T>;
    export type FixedSizeListVector<T> = vectors_.FixedSizeListVector<T>;
};

