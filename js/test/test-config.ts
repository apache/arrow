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
const arrowFormats = ['file', 'stream'];
const arrowFileNames = ['simple', 'struct', 'dictionary', 'dictionary2', 'multi_dictionary'];
const multipartArrows = ['count', 'latlong', 'origins'];
export let arrowTestConfigurations = [] as (string | Buffer)[][];

arrowTestConfigurations = arrowFormats.reduce((configs, format) => {
    return arrowFileNames.reduce((configs, name) => {
        const arrowPath = path.resolve(__dirname, `./arrows/${format}/${name}.arrow`);
        try {
            const arrowFile = fs.readFileSync(arrowPath);
            return [...configs, [`${name} ${format} Arrow`, arrowFile]];
        } catch (e) {}
        return configs;
    }, configs);
}, arrowTestConfigurations);

arrowTestConfigurations = multipartArrows.reduce((configs, folder) => {
    const schemaPath = path.resolve(__dirname, `./arrows/multi/${folder}/schema.arrow`);
    const recordsPath = path.resolve(__dirname, `./arrows/multi/${folder}/records.arrow`);
    return [...configs, [`multipart ${folder} Arrow`, fs.readFileSync(schemaPath), fs.readFileSync(recordsPath)]];
}, arrowTestConfigurations);

export default arrowTestConfigurations;
