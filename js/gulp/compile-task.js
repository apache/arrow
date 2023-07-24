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

import { Observable } from 'rxjs';
import { npmPkgName } from './util.js';
import { memoizeTask } from './memoize-task.js';

import closureTask from './closure-task.js';
import typescriptTask from './typescript-task.js';
import { arrowTask, arrowTSTask } from './arrow-task.js';

const compileTask = ((cache) => memoizeTask(cache, function compile(target, format, ...args) {
    return target === `src`                    ? Observable.empty()
         : target === npmPkgName               ? arrowTask(target, format, ...args)()
         : target === `ts`                     ? arrowTSTask(target, format, ...args)()
         : format === `umd`                    ? closureTask(target, format, ...args)()
                                               : typescriptTask(target, format, ...args)();
}))({});

export default compileTask;
