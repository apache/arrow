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

import * as fs from 'fs';
import * as path from 'path';
import * as glob from 'glob';

export const sources = (process.env.TEST_SOURCES
    ? JSON.parse(process.env.TEST_SOURCES + '')
    : [`cpp`, `java`]) as ['cpp' | 'java'];

export const formats = (process.env.TEST_FORMATS
    ? JSON.parse(process.env.TEST_FORMATS + '')
    : [`file`, `stream`]) as ['file' | 'stream'];

export const config = sources.reduce((sources, source) => ({
    ...sources,
    [source]: formats.reduce((formats, format) => ({
        ...formats,
        [format]: loadArrows(source, format)
    }), {})
}), {}) as {
    [k in 'cpp' | 'java']: {
        [k in 'file' | 'stream']: Arrows
    }
};

export type Arrows = { name: string, buffers: Uint8Array[] }[];

function loadArrows(source: string, format: string) {
    const arrows = [];
    const filenames = glob.sync(path.resolve(__dirname, `data/${source}/${format}`, `*.arrow`));
    for (const filename of filenames) {
        const { name } = path.parse(filename);
        arrows.push({ name, buffers: [fs.readFileSync(filename)] });
    }
    return arrows as Arrows;
}
