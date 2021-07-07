module.exports = {
    ...require('../jest.config'),
    "globals": {
        "ts-jest": {
            "diagnostics": false,
            "tsConfig": "test/tsconfig/tsconfig.es5.umd.json"
        }
    },
    "moduleNameMapper": {
        "^apache-arrow(.*)": "<rootDir>/targets/es5/umd/Arrow.dom.js"
    }
};
