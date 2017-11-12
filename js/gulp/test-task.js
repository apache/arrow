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

const del = require('del');
const path = require('path');
const { argv } = require('./argv');
const { promisify } = require('util');
const glob = promisify(require('glob'));
const mkdirp = promisify(require('mkdirp'));
const rimraf = promisify(require('rimraf'));
const child_process = require(`child_process`);
const { memoizeTask } = require('./memoize-task');
const exec = promisify(require('child_process').exec);

const jestArgv = [];
argv.update && jestArgv.push(`-u`);
argv.verbose && jestArgv.push(`--verbose`);
argv.coverage && jestArgv.push(`--coverage`);

const debugArgv = [`--runInBand`, `--env`, `jest-environment-node-debug`];
const jest = require.resolve(path.join(`..`, `node_modules`, `.bin`, `jest`));

const testTask = ((cache, execArgv, testOptions) => memoizeTask(cache, function test(target, format, debug = false) {
    const opts = Object.assign({}, testOptions);
    const args = !debug ? [...execArgv] : [...debugArgv, ...execArgv];
    opts.env = Object.assign({}, opts.env, {
        TEST_TARGET: target,
        TEST_MODULE: format,
        TEST_TS_SOURCE: !!argv.coverage,
        TEST_SOURCES: JSON.stringify(Array.isArray(argv.sources) ? argv.sources : [argv.sources]),
        TEST_FORMATS: JSON.stringify(Array.isArray(argv.formats) ? argv.formats : [argv.formats]),
    });
    return !debug ?
        child_process.spawn(jest, args, opts) :
        child_process.exec(`node --inspect-brk ${jest} ${args.join(` `)}`, opts);
}))({}, jestArgv, {
    env: Object.assign({}, process.env),
    stdio: [`ignore`, `inherit`, `inherit`],
});

module.exports = testTask;
module.exports.testTask = testTask;
module.exports.cleanTestData = cleanTestData;
module.exports.createTestData = createTestData;

async function cleanTestData() {
    return await del([
        `${path.resolve('./test/arrows/cpp')}/**`,
        `${path.resolve('./test/arrows/java')}/**`,
    ]);
}

async function createTestData() {
    const base = path.resolve('./test/arrows');
    await mkdirp(path.join(base, 'cpp/file'));
    await mkdirp(path.join(base, 'java/file'));
    await mkdirp(path.join(base, 'cpp/stream'));
    await mkdirp(path.join(base, 'java/stream'));
    const errors = [];
    const names = await glob(path.join(base, 'json/*.json'));
    for (let jsonPath of names) {
        const name = path.parse(path.basename(jsonPath)).name;
        const arrowCppFilePath = path.join(base, 'cpp/file', `${name}.arrow`);
        const arrowJavaFilePath = path.join(base, 'java/file', `${name}.arrow`);
        const arrowCppStreamPath = path.join(base, 'cpp/stream', `${name}.arrow`);
        const arrowJavaStreamPath = path.join(base, 'java/stream', `${name}.arrow`);
        try {
            await generateCPPFile(jsonPath, arrowCppFilePath);
            await generateCPPStream(arrowCppFilePath, arrowCppStreamPath);
        } catch (e) { errors.push(e.message); }
        try {
            await generateJavaFile(jsonPath, arrowJavaFilePath);
            await generateJavaStream(arrowJavaFilePath, arrowJavaStreamPath);
        } catch (e) { errors.push(e.message); }
    }
    if (errors.length) {
        console.error(errors.join(`\n`));
        process.exit(1);
    }
}

async function generateCPPFile(jsonPath, filePath) {
    await rimraf(filePath);
    return await exec(
        `../cpp/build/release/json-integration-test ${
        `--integration --mode=JSON_TO_ARROW`} ${
        `--json=${path.resolve(jsonPath)} --arrow=${filePath}`}`,
        { maxBuffer: Math.pow(2, 53) - 1 }
    );
}

async function generateCPPStream(filePath, streamPath) {
    await rimraf(streamPath);
    return await exec(
        `../cpp/build/release/file-to-stream ${filePath} > ${streamPath}`,
        { maxBuffer: Math.pow(2, 53) - 1 }
    );
}

async function generateJavaFile(jsonPath, filePath) {
    await rimraf(filePath);
    return await exec(
        `java -cp ../java/tools/target/arrow-tools-0.8.0-SNAPSHOT-jar-with-dependencies.jar ${
        `org.apache.arrow.tools.Integration -c JSON_TO_ARROW`} ${
        `-j ${path.resolve(jsonPath)} -a ${filePath}`}`,
        { maxBuffer: Math.pow(2, 53) - 1 }
    );
}

async function generateJavaStream(filePath, streamPath) {
    await rimraf(streamPath);
    return await exec(
        `java -cp ../java/tools/target/arrow-tools-0.8.0-SNAPSHOT-jar-with-dependencies.jar ${
        `org.apache.arrow.tools.FileToStream`} ${filePath} ${streamPath}`,
        { maxBuffer: Math.pow(2, 53) - 1 }
    );
}
