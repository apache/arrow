const { resolve } = require('path');
const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;
const { readdirSync } = require('fs');

const entries = Object.fromEntries(
    readdirSync(resolve(__dirname, `..`))
        .filter(fileName => fileName.endsWith('.js'))
        .map(fileName => fileName.replace(/\.js$/, ''))
        .map(fileName => [fileName, resolve(__dirname, `../${fileName}.js`)])
);

module.exports = env => ({
    mode: 'development',
    optimization: {
        usedExports: true
    },
    entry: entries,
    output: {
        path: resolve(__dirname, '.'),
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
        alias: {
            'apache-arrow': resolve(__dirname, '../../../targets/apache-arrow/')
        }
    },
    plugins: env.analyze ? [new BundleAnalyzerPlugin()] : []
});
