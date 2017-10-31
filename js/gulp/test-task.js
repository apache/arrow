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

const path = require('path');
const child_process = require(`child_process`);
const { argv } = require('./argv');
const { memoizeTask } = require('./memoize-task');

const jestArgv = [];
argv.update && jestArgv.push(`-u`);
argv.verbose && jestArgv.push(`--verbose`);
argv.coverage && jestArgv.push(`--coverage`);

const debugArgv = [`--runInBand`, `--env`, `jest-environment-node-debug`];
const jest = require.resolve(path.join(`..`, `node_modules`, `.bin`, `jest`));

const testTask = ((cache, execArgv, testOptions) => memoizeTask(cache, function test(target, format, debug = false) {
    const opts = Object.assign({}, testOptions);
    const args = !debug ? [...execArgv] : [...debugArgv, ...execArgv];
    opts.env = Object.assign({}, opts.env, { TEST_TARGET: target, TEST_MODULE: format });
    return !debug ?
        child_process.spawn(jest, args, opts) :
        child_process.exec(`node --inspect-brk ${jest} ${args.join(` `)}`, opts);
}))({}, jestArgv, {
    env: Object.assign({}, process.env),
    stdio: [`ignore`, `inherit`, `inherit`],
});

module.exports = testTask;
module.exports.testTask = testTask;
