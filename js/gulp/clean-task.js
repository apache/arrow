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

import { deleteAsync as del } from 'del';
import { targetDir } from './util.js';
import memoizeTask from './memoize-task.js';
import { catchError } from 'rxjs/operators';
import { from as ObservableFrom, EMPTY as ObservableEmpty } from 'rxjs';

export const cleanTask = ((cache) => memoizeTask(cache, function clean(target, format) {
    const dir = targetDir(target, format);
    return ObservableFrom(del(dir))
        .pipe(catchError((e) => ObservableEmpty()));
}))({});

export default cleanTask;
