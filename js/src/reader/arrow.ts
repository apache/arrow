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

import { readJSON } from './json';
import { readBuffers, readBuffersAsync } from './buffer';
import { readVectors, readVectorsAsync } from './vector';
import { Vector } from '../vector/vector';

export { readJSON };
export { readBuffers, readBuffersAsync };
export { readVectors, readVectorsAsync };

export function* read(sources: Iterable<Uint8Array | Buffer | string> | object | string) {
    let input: any = sources;
    let batches: Iterable<Vector[]>;
    if (typeof input === 'string') {
        try { input = JSON.parse(input); }
        catch (e) { input = sources; }
    }
    if (!input || typeof input !== 'object') {
        batches = (typeof input === 'string') ? readVectors(readBuffers([input])) : [];
    } else {
        batches = (typeof input[Symbol.iterator] === 'function')
            ? readVectors(readBuffers(input))
            : readVectors(readJSON(input));
    }
    yield* batches;
}

export async function* readAsync(sources: AsyncIterable<Uint8Array | Buffer | string>) {
    for await (let vectors of readVectorsAsync(readBuffersAsync(sources))) {
        yield vectors;
    }
}
