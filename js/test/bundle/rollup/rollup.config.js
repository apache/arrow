import bundleSize from 'rollup-plugin-bundle-size';
import nodeResolve from '@rollup/plugin-node-resolve';
import alias from '@rollup/plugin-alias';
const { resolve } = require('path');

const plugins = [
    alias({
        entries: {
            'apache-arrow': resolve(__dirname, '../../../targets/apache-arrow/')
        }
    }),
    nodeResolve(),
    bundleSize()
]

export default ['table', 'makeTable', 'vector', 'deserialize'].map(name => ({
    input: resolve(__dirname, `../${name}.js`),
    output: {
        file: resolve(__dirname, `./${name}-bundle.js`),
    },
    plugins,
}));
