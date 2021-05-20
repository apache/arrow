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

import {readFileSync} from 'fs';
import {resolve, parse} from 'path';
import {sync} from 'glob';

const filenames = sync(resolve(__dirname, `../test/data/tables/`, `*.arrow`));

export default filenames.map(filename => {
    const { name } = parse(filename);
    return {
        name,
        buffers: [readFileSync(filename)],
        countBys: ['origin', 'destination'],
        counts: [
            {column: 'lat',    test: 'gt' as 'gt' | 'eq', value: 0        },
            {column: 'lng',    test: 'gt' as 'gt' | 'eq', value: 0        },
            {column: 'origin', test: 'eq' as 'gt' | 'eq', value: 'Seattle'},
        ],
    };
});

