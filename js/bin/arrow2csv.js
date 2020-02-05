#! /usr/bin/env node

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

const Path = require(`path`);
const here = Path.resolve(__dirname, '../');
const tsnode = require.resolve(`ts-node/register`);
const arrow2csv = Path.join(here, `src/bin/arrow2csv.ts`);
const env = { ...process.env, TS_NODE_TRANSPILE_ONLY: `true` };

require('child_process').spawn(`node`, [
    `-r`, tsnode, arrow2csv, ...process.argv.slice(2)
], { cwd: here, env, stdio: `inherit` });
