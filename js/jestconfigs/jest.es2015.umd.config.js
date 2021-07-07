module.exports = {
    ...require('../jest.config'),
    "globals": {
        "ts-jest": {
            "diagnostics": false,
            "tsConfig": "test/tsconfig/tsconfig.es2015.umd.json"
        }
    },
    "moduleNameMapper": {
        "^apache-arrow(.*)": "<rootDir>/targets/es2015/umd/Arrow.dom.js"
    }
};
