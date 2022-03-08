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

import gulp from "gulp";
import size from "gulp-vinyl-size";
import gulpRename from "gulp-rename";
import terser from "gulp-terser";
import source from "vinyl-source-stream";
import buffer from "vinyl-buffer";
import { observableFromStreams } from "./util.js";
import { forkJoin as ObservableForkJoin } from "rxjs";
import { resolve, join } from "path";
import { readdirSync } from "fs";
import { execSync } from 'child_process';

import gulpEsbuild from "gulp-esbuild";
import esbuildAlias from "esbuild-plugin-alias";

import rollupStream from "@rollup/stream";
import { default as nodeResolve } from "@rollup/plugin-node-resolve";
import rollupAlias from "@rollup/plugin-alias";

import { BundleAnalyzerPlugin } from "webpack-bundle-analyzer";
import webpack from "webpack-stream";
import named from "vinyl-named";

import { fileURLToPath } from 'url';
import { dirname } from 'path';
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const bundleDir = resolve(__dirname, '../test/bundle');

const fileNames = readdirSync(bundleDir)
    .filter(fileName => fileName.endsWith('.js'))
    .map(fileName => fileName.replace(/\.js$/, ''));

const target = `apache-arrow`;

const bundlesGlob = join(bundleDir, '**.js');
const esbuildDir = join(bundleDir, 'esbuild');
export const esbuildTask = (minify = true) => () => observableFromStreams(
    gulp.src(bundlesGlob),
    gulpEsbuild({
        bundle: true,
        minify,
        treeShaking: true,
        plugins: [
            esbuildAlias({
                'apache-arrow': resolve(__dirname, `../targets/${target}/Arrow.dom.mjs`),
            }),
        ],
    }),
    gulpRename((p) => { p.basename += '-bundle'; }),
    gulp.dest(esbuildDir),
    size({ gzip: true })
);

const rollupDir = join(bundleDir, 'rollup');
export const rollupTask = (minify = true) => () => ObservableForkJoin(
    fileNames.map(fileName => observableFromStreams(
        rollupStream({
            input: join(bundleDir, `${fileName}.js`),
            output: { format: 'cjs' },
            plugins: [
                rollupAlias({
                    entries: { 'apache-arrow': resolve(__dirname, `../targets/${target}/`) }
                }),
                nodeResolve()
            ],
            onwarn: (message) => {
                if (message.code === 'CIRCULAR_DEPENDENCY') return
                console.error(message);
            }
        }),
        source(`${fileName}-bundle.js`),
        buffer(),
        ...(minify ? [terser()] : []),
        gulp.dest(rollupDir),
        size({ gzip: true })
    ))
)

const webpackDir = join(bundleDir, 'webpack');
export const webpackTask = (opts = { minify: true, analyze: false }) => () => observableFromStreams(
    gulp.src(bundlesGlob),
    named(),
    webpack({
        mode: opts?.minify == false ? 'development' : 'production',
        optimization: {
            usedExports: true
        },
        output: {
            filename: '[name]-bundle.js'
        },
        module: {
            rules: [
                {
                    resolve: {
                        fullySpecified: false,
                    }
                }
            ]
        },
        resolve: {
            alias: { 'apache-arrow': resolve(__dirname, `../targets/${target}/`) }
        },
        stats: 'errors-only',
        plugins: opts?.analyze ? [new BundleAnalyzerPlugin()] : []
    }),
    gulp.dest(webpackDir),
    size({ gzip: true })
);

export const execBundleTask = () => () => observableFromStreams(
    gulp.src(join(bundleDir, '**/**-bundle.js')),
    async (generator) => {
        for await (const file of generator) {
            console.log(`executing ${file.path}`);
            execSync(`node ${file.path}`);
        }
    }
);
