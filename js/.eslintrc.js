module.exports = {
    env: {
        browser: true,
        es6: true,
        node: true,
    },
    parser: "@typescript-eslint/parser",
    parserOptions: {
        project: "tsconfig.json",
        sourceType: "module",
        ecmaVersion: 2020,
    },
    plugins: ["@typescript-eslint", "jest"],
    extends: [
        "eslint:recommended",
        "plugin:jest/recommended",
        "plugin:jest/style",
        "plugin:@typescript-eslint/recommended",
    ],
    rules: {
        "@typescript-eslint/indent": "off",
        "@typescript-eslint/member-delimiter-style": [
            "error",
            {
                multiline: {
                    delimiter: "semi",
                    requireLast: true,
                },
                singleline: {
                    delimiter: "semi",
                    requireLast: false,
                },
            },
        ],
        "@typescript-eslint/no-namespace": ["error", { "allowDeclarations": true }],
        "@typescript-eslint/no-empty-function": "off",
        "@typescript-eslint/no-unused-expressions": "off",
        "@typescript-eslint/no-use-before-define": "off",
        "@typescript-eslint/no-require-imports": "error",
        "@typescript-eslint/no-var-requires": "off",  // handled by rule above
        "@typescript-eslint/quotes": [
            "error",
            "single",
            {
                avoidEscape: true,
                allowTemplateLiterals: true
            },
        ],
        "@typescript-eslint/semi": ["error", "always"],
        "@typescript-eslint/type-annotation-spacing": "error",
        "@typescript-eslint/explicit-module-boundary-types": "off",
        "@typescript-eslint/no-explicit-any": "off",
        "@typescript-eslint/no-misused-new": "off",
        "@typescript-eslint/ban-ts-comment": "off",
        "@typescript-eslint/no-non-null-assertion": "off",
        "@typescript-eslint/no-unused-vars": "off",  // ts already takes care of this

        "brace-style": "off",
        "curly": ["error", "multi-line"],
        "eol-last": "error",
        "no-empty": "off",
        "no-multiple-empty-lines": "error",
        "no-trailing-spaces": "error",
        "no-var": "error",

        "no-cond-assign": "off",

        // rules for later:

        "prefer-const": ["off"],
        // "prefer-const": ["error", {
        //     "destructuring": "all"
        // }],

        // "one-var": ["error", "never"],

        // "brace-style": ["error", "1tbs", { "allowSingleLine": true }],
    },
};
