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

const path = require('path');
const target = process.env.TEST_TARGET!;
const format = process.env.TEST_MODULE!;
const useSrc = process.env.TEST_TS_SOURCE === `true`;

// these are duplicated in the gulpfile :<
const targets = [`es5`, `es2015`, `esnext`];
const formats = [`cjs`, `esm`, `cls`, `umd`];

function throwInvalidImportError(name: string, value: string, values: string[]) {
    throw new Error('Unrecognized ' + name + ' \'' + value + '\'. Please run tests with \'--' + name + ' <any of ' + values.join(', ') + '>\'');
}

let modulePath = ``;

if (useSrc) modulePath = '../src';
else if (target === `ts` || target === `apache-arrow`) modulePath = target;
else if (!~targets.indexOf(target)) throwInvalidImportError('target', target, targets);
else if (!~formats.indexOf(format)) throwInvalidImportError('module', format, formats);
else modulePath = path.join(target, format);

export { List } from '../src/Arrow';
export { TypedArray } from '../src/Arrow';
export { TypedArrayConstructor } from '../src/Arrow';
export { NumericVectorConstructor } from '../src/Arrow';

import * as Arrow_ from '../src/Arrow';
export let Arrow: typeof Arrow_ = require(path.resolve(`./targets`, modulePath, `Arrow`));
export default Arrow;