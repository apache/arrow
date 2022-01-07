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

const gulp = require('gulp');
const size = require('gulp-vinyl-size');
const gulpRename = require('gulp-rename');
const terser = require('gulp-terser');
const source = require('vinyl-source-stream');
const buffer = require('vinyl-buffer');
const {
    observableFromStreams
} = require('./util');
const {
    forkJoin: ObservableForkJoin,
} = require('rxjs');
const { resolve, join } = require('path');
const { readdirSync } = require('fs');

const gulpEsbuild = require('gulp-esbuild');
const esbuildAlias = require('esbuild-plugin-alias');

const rollupStream = require('@rollup/stream');
const nodeResolve = require('@rollup/plugin-node-resolve').default;
const rollupAlias = require('@rollup/plugin-alias');

const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;
const webpack = require('webpack-stream');
const named = require('vinyl-named');

const bundleDir = resolve(__dirname, '../test/bundle');

const fileNames = readdirSync(bundleDir)
    .filter(fileName => fileName.endsWith('.js'))
    .map(fileName => fileName.replace(/\.js$/, ''));

const bundlesGlob = join(bundleDir, '**.js');
const esbuildDir = join(bundleDir, 'esbuild');
const esbuildTask = (minify = true) => () => observableFromStreams(
    gulp.src(bundlesGlob),
    gulpEsbuild({
        bundle: true,
        minify,
        treeShaking: true,
        plugins: [
            esbuildAlias({
                'apache-arrow': resolve(__dirname, '../targets/apache-arrow/Arrow.dom.mjs'),
            }),
        ],
    }),
    gulpRename((p) => { p.basename += '-bundle'; }),
    gulp.dest(esbuildDir),
    size({ gzip: true })
);

const rollupDir = join(bundleDir, 'rollup');
const rollupTask = (minify = true) => () => ObservableForkJoin(
    fileNames.map(fileName => observableFromStreams(
        rollupStream({
            input: join(bundleDir, `${fileName}.js`),
            output: { format: 'cjs' },
            plugins: [
                rollupAlias({
                    entries: { 'apache-arrow': resolve(__dirname, '../targets/apache-arrow/') }
                }),
                nodeResolve({ browser: true })
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
const webpackTask = (opts = { minify: true, analyze: false }) => () => observableFromStreams(
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
            alias: { 'apache-arrow': resolve(__dirname, '../targets/apache-arrow/') }
        },
        stats: 'errors-only',
        plugins: opts?.analyze ? [new BundleAnalyzerPlugin()] : []
    }),
    gulp.dest(webpackDir),
    size({ gzip: true })
)

module.exports.esbuildTask = esbuildTask;
module.exports.rollupTask = rollupTask;
module.exports.webpackTask = webpackTask;
