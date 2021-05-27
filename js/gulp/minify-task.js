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

const {
    targetDir,
    mainExport,
    UMDSourceTargets,
    shouldRunInChildProcess,
    spawnGulpCommandInChildProcess,
} = require('./util');

const path = require('path');
const webpack = require(`webpack`);
const { memoizeTask } = require('./memoize-task');
const { compileBinFiles } = require('./typescript-task');
const { Observable, ReplaySubject } = require('rxjs');
const TerserPlugin = require(`terser-webpack-plugin`);

const minifyTask = ((cache, commonConfig) => memoizeTask(cache, function minifyJS(target, format) {

    if (shouldRunInChildProcess(target, format)) {
        return spawnGulpCommandInChildProcess('compile', target, format);
    }

    const sourceTarget = UMDSourceTargets[target];
    const out = targetDir(target, format), src = targetDir(sourceTarget, `cls`);

    const targetConfig = { ...commonConfig,
        output: { ...commonConfig.output,
            path: path.resolve(`./${out}`) } };

    const webpackConfigs = [mainExport].map((entry) => ({
        ...targetConfig,
        mode: 'production',
        name: entry,
        entry: { [entry]: path.resolve(`${src}/${entry}.dom.js`) },
        plugins: [
            ...(targetConfig.plugins || []),
            new webpack.SourceMapDevToolPlugin({
                filename: `[name].${target}.min.js.map`,
                moduleFilenameTemplate: ({ resourcePath }) =>
                    resourcePath
                        .replace(/\s/, `_`)
                        .replace(/\.\/node_modules\//, ``)
            })
        ],
        optimization: {
            minimize: true,
            minimizer: [
                new TerserPlugin({
                    terserOptions: {
                        ecma: target,
                        output: { comments: false },
                        compress: { unsafe: true }
                    },
                })
            ]
        }
    }));

    const compilers = webpack(webpackConfigs);
    return Observable
            .bindNodeCallback(compilers.run.bind(compilers))()
            .merge(compileBinFiles(target, format)).takeLast(1)
            .multicast(new ReplaySubject()).refCount();
}))({}, {
    resolve: { mainFields: [`module`, `main`] },
    module: { rules: [{ test: /\.js$/, enforce: `pre`, use: [`source-map-loader`] }] },
    output: { filename: '[name].js', library: mainExport, libraryTarget: `umd`, umdNamedDefine: true, globalObject: 'this' },
});

module.exports = minifyTask;
module.exports.minifyTask = minifyTask;
