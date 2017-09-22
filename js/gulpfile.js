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

const del = require(`del`);
const gulp = require(`gulp`);
const path = require(`path`);
const pump = require(`pump`);
const ts = require(`gulp-typescript`);
const streamMerge = require(`merge2`);
const sourcemaps = require(`gulp-sourcemaps`);
const child_process = require(`child_process`);
const gulpJsonTransform = require(`gulp-json-transform`);
const closureCompiler = require(`google-closure-compiler`).gulp();

const knownTargets = [`es5`, `es2015`, `esnext`];
const knownModules = [`cjs`, `esm`, `cls`, `umd`];

// see: https://github.com/google/closure-compiler/blob/c1372b799d94582eaf4b507a4a22558ff26c403c/src/com/google/javascript/jscomp/CompilerOptions.java#L2988
const gCCTargets = {
    es5: `ECMASCRIPT5`,
    es2015: `ECMASCRIPT_2015`,
    es2016: `ECMASCRIPT_2016`,
    es2017: `ECMASCRIPT_2017`,
    esnext: `ECMASCRIPT_NEXT`
};

const tsProjects = [];
const argv = require(`command-line-args`)([
    { name: `all`, alias: `a`, type: Boolean },
    { name: 'update', alias: 'u', type: Boolean },
    { name: 'verbose', alias: 'v', type: Boolean },
    { name: `target`, type: String, defaultValue: `` },
    { name: `module`, type: String, defaultValue: `` },
    { name: `coverage`, type: Boolean, defaultValue: false },
    { name: `targets`, alias: `t`, type: String, multiple: true, defaultValue: [] },
    { name: `modules`, alias: `m`, type: String, multiple: true, defaultValue: [] }
]);

const { targets, modules } = argv;

argv.target && !targets.length && targets.push(argv.target);
argv.module && !modules.length && modules.push(argv.module);
(argv.all || !targets.length) && targets.push(`all`);
(argv.all || !modules.length) && modules.push(`all`);

for (const [target, format] of combinations([`all`, `all`])) {
    const combo = `${target}:${format}`;
    gulp.task(`test:${combo}`, gulp.series(testTask(target, format, combo, `targets/${target}/${format}`)));
    gulp.task(`clean:${combo}`, gulp.series(cleanTask(target, format, combo, `targets/${target}/${format}`)));
    gulp.task(`build:${combo}`, gulp.series(buildTask(target, format, combo, `targets/${target}/${format}`)));
    gulp.task(`bundle:${combo}`, gulp.series(bundleTask(target, format, combo, `targets/${target}/${format}`)));
    gulp.task(`package:${combo}`, gulp.series(packageTask(target, format, combo, `targets/${target}/${format}`)));
    gulp.task(`test:debug:${combo}`, gulp.series(testTask(target, format, combo, `targets/${target}/${format}`, true)));
}

gulp.task(`test`, gulp.series(runTaskCombos(`test`)));
gulp.task(`clean`, gulp.parallel(runTaskCombos(`clean`)));
gulp.task(`build`, gulp.parallel(runTaskCombos(`build`)));
gulp.task(`bundle`, gulp.parallel(runTaskCombos(`bundle`)));
gulp.task(`package`, gulp.parallel(runTaskCombos(`package`)));
gulp.task(`test:debug`, gulp.series(runTaskCombos(`test:debug`)));
gulp.task(`default`, gulp.task(`package`));

function runTaskCombos(name) {
    const combos = [];
    for (const [target, format] of combinations(targets, modules)) {
        if (format === `cls`) {
            continue;
        }
        combos.push(`${name}:${target}:${format}`);
    }
    return combos;
}

function cleanTask(target, format, taskName, outDir) {
    return function cleanTask() {
        const globs = [`${outDir}/**`];
        if (target === `es5` && format === `cjs`) {
            globs.push(`types`, `typings`);
        }
        return del(globs);
    };
}

function buildTask(target, format, taskName, outDir) {
    return format === `umd`
        ? closureTask(target, format, taskName, outDir)
        : typescriptTask(target, format, taskName, outDir);
}

function bundleTask(target, format, taskName, outDir) {
    return function bundleTask() {
        return streamMerge([
            pump(gulp.src([`LICENSE`, `README.md`]), gulp.dest(outDir), onError),
            pump(
                gulp.src(`package.json`),
                gulpJsonTransform((orig) => [
                    `version`, `description`, `keywords`,
                    `repository`, `author`, `homepage`, `bugs`, `license`,
                    `dependencies`, `peerDependencies`
                ].reduce((copy, key) => (
                    (copy[key] = orig[key]) && copy || copy
                ), {
                    main: `Arrow.js`,
                    types: `Arrow.d.ts`,
                    typings: `Arrow.d.ts`,
                    name: `@apache-arrow/${target}-${format}`
                }), 2),
                gulp.dest(outDir),
                onError
            )
        ]);
    }
}

function packageTask(target, format, taskName, outDir) {
    return [`build:${taskName}`, `bundle:${taskName}`];
}

function testTask(target, format, taskName, outDir, debug) {
    const jestOptions = !debug ? [] : [
        `--runInBand`, `--env`, `jest-environment-node-debug`];
    argv.update && jestOptions.unshift(`-u`);
    argv.verbose && jestOptions.unshift(`--verbose`);
    argv.coverage && jestOptions.unshift(`--coverage`);
    const jestPath = `./node_modules/.bin/jest`;
    const debugOpts = jestOptions.join(' ');
    const spawnOptions = {
        stdio: [`ignore`, `inherit`, `inherit`],
        env: Object.assign({}, process.env, {
            TEST_TARGET: target, TEST_MODULE: format
        })
    };
    return function testTask() {
        return !debug ?
            child_process.spawn(jestPath, jestOptions, spawnOptions) :
            child_process.exec(`node --inspect-brk ${jestPath} ${debugOpts}`, spawnOptions);
    }
}

function closureTask(target, format, taskName, outDir) {
    const clsTarget = `es5`;
    const googleRoot = `targets/${clsTarget}/cls`;
    const languageIn = clsTarget === `es5` ? `es2015` : clsTarget;
    return [
        [`clean:${taskName}`, `build:${clsTarget}:cls`],
        function closureTask() {
            return closureStream(
                closureSrcs(),
                closureCompiler(closureArgs())
            ).on('end', () => del([`targets/${target}/cls/**`]));
        }
    ];
    function closureSrcs() {
        return gulp.src([
            `closure-compiler-scripts/*.js`,
            `${googleRoot}/**/*.js`,
            `!${googleRoot}/format/*.js`,
            `!${googleRoot}/Arrow.externs.js`,
        ], { base: `./` });
    }
    function closureStream(sources, compiler) {
        const streams = [
            sources,
            sourcemaps.init(),
            compiler,
            sourcemaps.write('.'),
            gulp.dest(outDir)
        ];
        // copy the ES5 UMD bundle to dist
        if (target === `es5`) {
            streams.push(gulp.dest(`dist`));
        }
        return pump(...streams, onError);
    }
    function closureArgs() {
        return {
            third_party: true,
            externs: `${googleRoot}/Arrow.externs.js`,
            warning_level: `QUIET`,
            dependency_mode: `LOOSE`,
            rewrite_polyfills: false,
            // formatting: `PRETTY_PRINT`,
            compilation_level: `ADVANCED`,
            assume_function_wrapper: true,
            js_output_file: `Arrow.js`,
            language_in: gCCTargets[languageIn],
            language_out: gCCTargets[clsTarget],
            entry_point: `${googleRoot}/Arrow.js`,
            output_wrapper:
`// Licensed to the Apache Software Foundation (ASF) under one
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
(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
    typeof define === 'function' && define.amd ? define(['exports'], factory) :
    (factory(global.Arrow = global.Arrow || {}));
}(this, (function (exports) {%output%}.bind(this))));`
        };
    }
}

function typescriptTask(target, format, taskName, outDir) {
    return [
        [`clean:${taskName}`],
        function typescriptTask() {
            const tsconfigPath = `tsconfig/tsconfig.${target}.${format}.json`;
            let { js, dts } = tsProjects.find((p) => p.target === target && p.format === format) || {};
            if (!js || !dts) {
                let tsProject = ts.createProject(tsconfigPath);
                ({ js, dts } = pump(
                    tsProject.src(),
                    sourcemaps.init(),
                    tsProject(ts.reporter.fullReporter(true)),
                    onError
                ));
                dts = [dts, gulp.dest(outDir)];
                js = [js, sourcemaps.write(), gulp.dest(outDir)];
                // copy types to the root
                if (target === `es5` && format === `cjs`) {
                    dts.push(gulp.dest(`types`));
                }
                tsProjects.push({
                    target, format,
                    js: js = pump(...js, onError),
                    dts: dts = pump(...dts, onError)
                });
            }
            return streamMerge([ dts, js ]);
        }
    ];
}

function* combinations(_targets, _modules) {

    const targets = known(knownTargets, _targets || [`all`]);
    const modules = known(knownModules, _modules || [`all`]);

    for (const format of modules) {
        for (const target of targets) {
            yield [target, format];
        }
    }

    function known(known, values) {
        return ~values.indexOf(`all`)
            ? known
            : Object.keys(
                values.reduce((map, arg) => ((
                    (known.indexOf(arg) !== -1) &&
                    (map[arg.toLowerCase()] = true)
                    || true) && map
                ), {})
            ).sort((a, b) => known.indexOf(a) - known.indexOf(b));
    }
}

function onError(err) {
    if (typeof err === 'number') {
        process.exit(err);
    } else if (err) {
        console.error(err.stack || err.toString());
        process.exit(1);
    }
}