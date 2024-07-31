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

import args from 'command-line-args';
export const argv = args([
    { name: `all`, type: Boolean },
    { name: 'verbose', alias: `v`, type: Boolean },
    { name: `target`, type: String, defaultValue: `` },
    { name: `module`, type: String, defaultValue: `` },
    { name: `coverage`, type: Boolean, defaultValue: false },
    { name: `tests`, type: String, multiple: true, defaultValue: [`test/unit/`] },
    { name: `targets`, alias: `t`, type: String, multiple: true, defaultValue: [] },
    { name: `modules`, alias: `m`, type: String, multiple: true, defaultValue: [] },
], { partial: true });

export const { targets, modules } = argv;

if (argv.target === `src`) {
    argv.target && !targets.length && targets.push(argv.target);
} else {
    argv.target && !targets.length && targets.push(argv.target);
    argv.module && !modules.length && modules.push(argv.module);
    (argv.all || !targets.length) && targets.push(`all`);
    (argv.all || !modules.length) && modules.push(`all`);
}
