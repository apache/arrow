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

import { Column } from '../types';
import { TableVector } from './table';
import { readBuffers } from '../../reader/arrow';

export function fromBuffers(...bytes: Array<Uint8Array | Buffer | string>) {
    let columns: Column<any>[] = null as any;
    for (let vectors of readBuffers(...bytes)) {
        columns = !columns ? vectors : columns.map((v, i) => v.concat(vectors[i]) as Column<any>);
    }
    return new TableVector({ columns });
}

TableVector.from = fromBuffers;

declare module './table' {
    namespace TableVector { export let from: typeof fromBuffers; }
}