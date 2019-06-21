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
const readFile = promisify(require('fs').readFile);
const asyncDone = promisify(require('async-done'));
const exec = promisify(require('child_process').exec);
const parseXML = promisify(require('xml2js').parseString);

const jestArgv = [];
argv.verbose && jestArgv.push(`--verbose`);
argv.coverage
    ? jestArgv.push(`-c`, `jest.coverage.config.js`, `--coverage`, `-i`)
    : jestArgv.push(`-c`, `jest.config.js`, `-i`)

const jest = path.join(path.parse(require.resolve(`jest`)).dir, `../bin/jest.js`);
const testOptions = {
    stdio: [`ignore`, `inherit`, `inherit`],
    env: {
        ...process.env,
        // hide fs.promises/stream[Symbol.asyncIterator] warnings
        NODE_NO_WARNINGS: `1`,
        // prevent the user-land `readable-stream` module from
        // patching node's streams -- they're better now
        READABLE_STREAM: `disable`
    },
};

const testTask = ((cache, execArgv, testOptions) => memoizeTask(cache, function test(target, format) {
    const opts = { ...testOptions };
    const args = [...execArgv, `test/unit/`];
    opts.env = {
        ...opts.env,
        TEST_TARGET: target,
        TEST_MODULE: format,
        TEST_DOM_STREAMS: (target ==='src' || format === 'umd').toString(),
        TEST_NODE_STREAMS: (target ==='src' || format !== 'umd').toString(),
        TEST_TS_SOURCE: !!argv.coverage || (target === 'src') || (opts.env.TEST_TS_SOURCE === 'true')
    };
    return asyncDone(() => child_process.spawn(`node`, args, opts));
}))({}, [jest, ...jestArgv], testOptions);

module.exports = testTask;
module.exports.testTask = testTask;
module.exports.cleanTestData = cleanTestData;
module.exports.createTestData = createTestData;

// Pull C++ and Java paths from environment vars first, otherwise sane defaults
const ARROW_HOME = process.env.ARROW_HOME || path.resolve('../');
const ARROW_JAVA_DIR = process.env.ARROW_JAVA_DIR || path.join(ARROW_HOME, 'java');
const CPP_EXE_PATH = process.env.ARROW_CPP_EXE_PATH || path.join(ARROW_HOME, 'cpp/build/debug');
const ARROW_INTEGRATION_DIR = process.env.ARROW_INTEGRATION_DIR || path.join(ARROW_HOME, 'integration');
const CPP_JSON_TO_ARROW = path.join(CPP_EXE_PATH, 'arrow-json-integration-test');
const CPP_STREAM_TO_FILE = path.join(CPP_EXE_PATH, 'arrow-stream-to-file');
const CPP_FILE_TO_STREAM = path.join(CPP_EXE_PATH, 'arrow-file-to-stream');

const testFilesDir = path.join(ARROW_HOME, 'js/test/data');
const snapshotsDir = path.join(ARROW_HOME, 'js/test/__snapshots__');
const cppFilesDir = path.join(testFilesDir, 'cpp');
const javaFilesDir = path.join(testFilesDir, 'java');
const jsonFilesDir = path.join(testFilesDir, 'json');

async function cleanTestData() {
    return await del([
        `${cppFilesDir}/**`,
        `${javaFilesDir}/**`,
        `${jsonFilesDir}/**`,
        `${snapshotsDir}/**`
    ]);
}

async function createTestJSON() {
    await mkdirp(jsonFilesDir);
    await exec(`shx cp ${ARROW_INTEGRATION_DIR}/data/*.json ${jsonFilesDir}`);
    await exec(`python3 ${ARROW_INTEGRATION_DIR}/integration_test.py --write_generated_json ${jsonFilesDir}`);
}

async function createTestData() {

    let JAVA_TOOLS_JAR = process.env.ARROW_JAVA_INTEGRATION_JAR;
    if (!JAVA_TOOLS_JAR) {
        const pom_version = await
            readFile(path.join(ARROW_JAVA_DIR, 'pom.xml'))
                .then((pom) => parseXML(pom.toString()))
                .then((pomXML) => pomXML.project.version[0]);
        JAVA_TOOLS_JAR = path.join(ARROW_JAVA_DIR, `/tools/target/arrow-tools-${pom_version}-jar-with-dependencies.jar`);
    }

    await cleanTestData().then(createTestJSON);
    await mkdirp(path.join(cppFilesDir, 'file'));
    await mkdirp(path.join(javaFilesDir, 'file'));
    await mkdirp(path.join(cppFilesDir, 'stream'));
    await mkdirp(path.join(javaFilesDir, 'stream'));

    const errors = [];
    const names = await glob(path.join(jsonFilesDir, '*.json'));

    for (let jsonPath of names) {
        const name = path.parse(path.basename(jsonPath)).name;
        const arrowCppFilePath = path.join(cppFilesDir, 'file', `${name}.arrow`);
        const arrowJavaFilePath = path.join(javaFilesDir, 'file', `${name}.arrow`);
        const arrowCppStreamPath = path.join(cppFilesDir, 'stream', `${name}.arrow`);
        const arrowJavaStreamPath = path.join(javaFilesDir, 'stream', `${name}.arrow`);
        try {
            await generateCPPFile(path.resolve(jsonPath), arrowCppFilePath);
            await generateCPPStream(arrowCppFilePath, arrowCppStreamPath);
        } catch (e) { errors.push(`${e.stdout}\n${e.message}`); }
        try {
            await generateJavaFile(path.resolve(jsonPath), arrowJavaFilePath);
            await generateJavaStream(arrowJavaFilePath, arrowJavaStreamPath);
        } catch (e) { errors.push(`${e.stdout}\n${e.message}`); }
    }
    if (errors.length) {
        console.error(errors.join(`\n`));
        process.exit(1);
    }

    async function generateCPPFile(jsonPath, filePath) {
        await rimraf(filePath);
        return await exec(
            `${CPP_JSON_TO_ARROW} ${
            `--integration --mode=JSON_TO_ARROW`} ${
            `--json=${jsonPath} --arrow=${filePath}`}`,
            { maxBuffer: Math.pow(2, 53) - 1 }
        );
    }
    
    async function generateCPPStream(filePath, streamPath) {
        await rimraf(streamPath);
        return await exec(
            `${CPP_FILE_TO_STREAM} ${filePath} > ${streamPath}`,
            { maxBuffer: Math.pow(2, 53) - 1 }
        );
    }
    
    async function generateJavaFile(jsonPath, filePath) {
        await rimraf(filePath);
        return await exec(
            `java -cp ${JAVA_TOOLS_JAR} ${
            `org.apache.arrow.tools.Integration -c JSON_TO_ARROW`} ${
            `-j ${path.resolve(jsonPath)} -a ${filePath}`}`,
            { maxBuffer: Math.pow(2, 53) - 1 }
        );
    }
    
    async function generateJavaStream(filePath, streamPath) {
        await rimraf(streamPath);
        return await exec(
            `java -cp ${JAVA_TOOLS_JAR} ${
            `org.apache.arrow.tools.FileToStream`} ${filePath} ${streamPath}`,
            { maxBuffer: Math.pow(2, 53) - 1 }
        );
    }
}
