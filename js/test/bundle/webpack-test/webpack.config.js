const { resolve } = require('path');

module.exports = {
    mode: 'production',
    entry: {
        table: resolve(__dirname, './table.js'),
        makeTable: resolve(__dirname, './makeTable.js'),
        vector: resolve(__dirname, './makeTable.js')
    },
    output: {
        path: resolve(__dirname, '.'),
        filename: '[name]-bundle.js'
    },
    module: {
        rules: [
            {
                test: /\.m?js$/,
                resolve: {
                    fullySpecified: false,
                },
            },
        ],
    },
    resolve: {
        alias: {
            'apache-arrow': resolve(__dirname, '../../../targets/apache-arrow/'),
        }
    }
};
