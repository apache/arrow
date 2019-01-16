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
// Dynamically load an Arrow target build based on command line arguments

import '@mattiasbuelens/web-streams-polyfill';

/* tslint:disable */
// import this before assigning window global since it does a `typeof window` check
require('web-stream-tools');

(<any> global).window = (<any> global).window || global;

// Fix for Jest in node v10.x
Object.defineProperty(ArrayBuffer, Symbol.hasInstance, {
    writable: true,
    configurable: true,
    value(inst: any) {
        return inst && inst.constructor && inst.constructor.name === 'ArrayBuffer';
    }
});

// these are duplicated in the gulpfile :<
const targets = [`es5`, `es2015`, `esnext`];
const formats = [`cjs`, `esm`, `cls`, `umd`];

const path = require('path');
const target = process.env.TEST_TARGET!;
const format = process.env.TEST_MODULE!;
const useSrc = process.env.TEST_TS_SOURCE === `true` || (!~targets.indexOf(target) || !~formats.indexOf(format));

let modulePath = ``;

if (useSrc) modulePath = '../src';
else if (target === `ts` || target === `apache-arrow`) modulePath = target;
else modulePath = path.join(target, format);

modulePath = path.resolve(`./targets`, modulePath);
modulePath = path.join(modulePath, `Arrow${format === 'umd' ? '' : '.node'}`);
const Arrow: typeof import('../src/Arrow') = require(modulePath);

export = Arrow;
