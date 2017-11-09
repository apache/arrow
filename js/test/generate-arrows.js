const path = require('path');
const { promisify } = require('util');
const mkdirp = require('mkdirp');
const glob = promisify(require('glob'));
const exec = promisify(require('child_process').exec);

(async function() {
    mkdirp.sync(path.resolve('./test/arrows/cpp/file'));
    mkdirp.sync(path.resolve('./test/arrows/java/file'));
    mkdirp.sync(path.resolve('./test/arrows/cpp/stream'));
    mkdirp.sync(path.resolve('./test/arrows/java/stream'));
    const names = await glob('./test/arrows/json/*.json');
    for (let jsonPath of names) {
        const name = path.parse(path.basename(jsonPath)).name;
        const arrowCppFilePath = path.resolve('./test/arrows/cpp/file', `${name}.arrow`);
        const arrowJavaFilePath = path.resolve('./test/arrows/java/file', `${name}.arrow`);
        const arrowCppStreamPath = path.resolve('./test/arrows/cpp/stream', `${name}.arrow`);
        const arrowJavaStreamPath = path.resolve('./test/arrows/java/stream', `${name}.arrow`);

        await generateCPPFile(jsonPath, arrowCppFilePath);
        await generateCPPStream(arrowCppFilePath, arrowCppStreamPath);
        await generateJavaFile(jsonPath, arrowJavaFilePath);
        await generateJavaStream(arrowJavaFilePath, arrowJavaStreamPath);
    }
})();

async function generateCPPFile(jsonPath, filePath) {
    return await exec(
        `../cpp/build/release/json-integration-test ${
        `--integration --mode=JSON_TO_ARROW`} ${
        `--json=${path.resolve(jsonPath)} --arrow=${filePath}`}`
    );
}

async function generateCPPStream(filePath, streamPath) {
    return await exec(`../cpp/build/release/file-to-stream ${filePath} > ${streamPath}`);
}

async function generateJavaFile(jsonPath, filePath) {
    return await exec(
        `java -cp ../java/tools/target/arrow-tools-0.8.0-SNAPSHOT-jar-with-dependencies.jar ${
        `org.apache.arrow.tools.Integration -c JSON_TO_ARROW`} ${
        `-j ${path.resolve(jsonPath)} -a ${filePath}`}`
    );
}

async function generateJavaStream(filePath, streamPath) {
    return await exec(
        `java -cp ../java/tools/target/arrow-tools-0.8.0-SNAPSHOT-jar-with-dependencies.jar ${
        `org.apache.arrow.tools.FileToStream`} ${filePath} ${streamPath}`);
}
