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

const { npmPkgName } = require('./util');
const { memoizeTask } = require('./memoize-task');

const uglifyTask = require('./uglify-task');
const closureTask = require('./closure-task');
const typescriptTask = require('./typescript-task');
const { arrowTask, arrowTSTask } = require('./arrow-task');

const buildTask = ((cache) => memoizeTask(cache, function build(target, format, ...args) {
    return target === npmPkgName               ? arrowTask(target, format, ...args)()
         : target === `ts`                     ? arrowTSTask(target, format, ...args)()
         : format === `umd` ? target === `es5` ? closureTask(target, format, ...args)()
                                               : uglifyTask(target, format, ...args)()
                                               : typescriptTask(target, format, ...args)();
}))({});

module.exports = buildTask;
module.exports.buildTask = buildTask;