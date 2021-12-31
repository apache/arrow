import bundleSize from 'rollup-plugin-bundle-size';
import nodeResolve from '@rollup/plugin-node-resolve';
import alias from '@rollup/plugin-alias';
const { resolve } = require('path');
const { readdirSync } = require('fs');

const plugins = [
    alias({
        entries: {
            'apache-arrow': resolve(__dirname, '../../../targets/apache-arrow/')
        }
    }),
    nodeResolve(),
    bundleSize()
]

const fileNames = readdirSync(resolve(__dirname, `..`))
    .filter(fileName => fileName.endsWith('.js'))
    .map(fileName => fileName.replace(/\.js$/, ''));

export default fileNames.map(name => ({
    input: resolve(__dirname, `../${name}.js`),
    output: {
        file: resolve(__dirname, `./${name}-bundle.js`),
    },
    plugins,
}));
