module.exports = {
    ...require('../jest.config'),
    "globals": {
        "ts-jest": {
            "diagnostics": false,
            "tsConfig": "test/tsconfig/tsconfig.es2015.cjs.json"
        }
    },
    "moduleNameMapper": {
        "^apache-arrow(.*)": "<rootDir>/targets/es2015/cjs$1"
    }
};
