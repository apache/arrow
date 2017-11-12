const path = require('path');
const { promisify } = require('util');
const glob = promisify(require('glob'));
const mkdirp = promisify(require('mkdirp'));
const rimraf = promisify(require('rimraf'));
const exec = promisify(require('child_process').exec);

(async function() {
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
})();

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
