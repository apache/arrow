module.exports = {
    ...require('../jest.config'),
    "globals": {
        "ts-jest": {
            "diagnostics": false,
            "tsConfig": "test/tsconfig/tsconfig.es5.cjs.json"
        }
    },
    "moduleNameMapper": {
        "^apache-arrow(.*)": "<rootDir>/targets/es5/cjs$1"
    }
};
