const { resolve } = require('path');
const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

module.exports = env => ({
    mode: 'development',
    optimization: {
        usedExports: true
    },
    entry: {
        table: resolve(__dirname, '../table.js'),
        makeTable: resolve(__dirname, '../makeTable.js'),
        vector: resolve(__dirname, '../makeTable.js')
    },
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
        // TODO: this should not be needed but without it Webpack uses the cjs files
        extensions: ['.mjs'],
        alias: {
            'apache-arrow': resolve(__dirname, '../../../targets/apache-arrow/')
        }
    },
    plugins: env.analyze ? [new BundleAnalyzerPlugin()] : []
});
