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

const esbuild = require('esbuild');
const alias = require('esbuild-plugin-alias');
const { resolve } = require('path');
const { readdirSync } = require('fs');

const fileNames = readdirSync(resolve(__dirname, `..`))
    .filter(fileName => fileName.endsWith('.js'))
    .map(fileName => fileName.replace(/\.js$/, ''));

(async () => {
    const result = await esbuild.build({
        entryPoints: fileNames.map(fileName => resolve(__dirname, `../${fileName}.js`)),
        bundle: true,
        minify: true,
        treeShaking: true,
        metafile: true,
        entryNames: '[name]-bundle',
        outdir: resolve(__dirname, './'),
        plugins: [
            alias({
                'apache-arrow': resolve(__dirname, '../../../targets/apache-arrow/Arrow.dom.mjs'),
            }),
        ],
    });

    for (const [bundle, outputs] of Object.entries(result.metafile.outputs)) {
        console.log(`${bundle}: ${Math.floor(outputs.bytes / 1024)} Kb`);
    }
})()
