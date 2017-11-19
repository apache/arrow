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
    ESKeywords,
    UMDSourceTargets,
    uglifyLanguageNames,
    observableFromStreams
} = require('./util');

const path = require('path');
const webpack = require(`webpack`);
const { memoizeTask } = require('./memoize-task');
const { Observable, ReplaySubject } = require('rxjs');
const UglifyJSPlugin = require(`uglifyjs-webpack-plugin`);
const esmRequire = require(`@std/esm`)(module, { cjs: true, esm: `js` });

const uglifyTask = ((cache, commonConfig) => memoizeTask(cache, function uglifyJS(target, format) {

    const sourceTarget = UMDSourceTargets[target];
    const PublicNames = reservePublicNames(sourceTarget, `cls`);
    const out = targetDir(target, format), src = targetDir(sourceTarget, `cls`);

    const targetConfig = { ...commonConfig,
        output: { ...commonConfig.output,
            path: path.resolve(`./${out}`) } };

    const webpackConfigs = [
        [mainExport, PublicNames]
    ].map(([entry, reserved]) => ({
        ...targetConfig,
        name: entry,
        entry: { [entry]: path.resolve(`${src}/${entry}.js`) },
        plugins: [
            ...(targetConfig.plugins || []),
            new webpack.SourceMapDevToolPlugin({
                filename: `[name].${target}.min.js.map`,
                moduleFilenameTemplate: ({ resourcePath }) =>
                    resourcePath
                        .replace(/\s/, `_`)
                        .replace(/\.\/node_modules\//, ``)
            }),
            new UglifyJSPlugin({
                sourceMap: true,
                uglifyOptions: {
                    ecma: uglifyLanguageNames[target],
                    compress: { unsafe: true },
                    output: { comments: false, beautify: false },
                    mangle: { eval: true, safari10: true, // <-- Works around a Safari 10 bug: // https://github.com/mishoo/UglifyJS2/issues/1753
                        properties: { reserved, keep_quoted: true }
                    }
                },
            })
        ]
    }));

    const compilers = webpack(webpackConfigs);
    return Observable
            .bindNodeCallback(compilers.run.bind(compilers))()
            .multicast(new ReplaySubject()).refCount();
}))({}, {
    resolve: { mainFields: [`module`, `main`] },
    module: { rules: [{ test: /\.js$/, enforce: `pre`, use: [`source-map-loader`] }] },
    output: { filename: '[name].js', library: mainExport, libraryTarget: `umd`, umdNamedDefine: true },
});

module.exports = uglifyTask;
module.exports.uglifyTask = uglifyTask;

const reservePublicNames = ((ESKeywords) => function reservePublicNames(target, format) {
    const publicModulePath = `../${targetDir(target, format)}/${mainExport}.js`;
    return [
        ...ESKeywords,
        ...reserveExportedNames(esmRequire(publicModulePath))
    ];
})(ESKeywords);

// Reflect on the Arrow modules to come up with a list of keys to save from Uglify's
// mangler. Assume all the non-inherited static and prototype members of the Arrow
// module and its direct exports are public, and should be preserved through minification.
const reserveExportedNames = (entryModule) => (
    Object
        .getOwnPropertyNames(entryModule)
        .filter((name) => (
            typeof entryModule[name] === `object` ||
            typeof entryModule[name] === `function`
        ))
        .map((name) => [name, entryModule[name]])
        .reduce((reserved, [name, value]) => {
            const fn = function() {};
            const ownKeys = value && Object.getOwnPropertyNames(value) || [];
            const protoKeys = typeof value === `function` && Object.getOwnPropertyNames(value.prototype) || [];
            const publicNames = [...ownKeys, ...protoKeys].filter((x) => x !== `default` && x !== `undefined` && !(x in fn));
            return [...reserved, name, ...publicNames];
        }, []
    )
);
