module.exports = {
    ...require('../jest.config'),
    "globals": {
        "ts-jest": {
            "diagnostics": false,
            "tsConfig": "test/tsconfig/tsconfig.esnext.esm.json"
        }
    },
    "moduleNameMapper": {
        "^apache-arrow(.*)": "<rootDir>/targets/esnext/esm$1"
    }
};
