module.exports = {
    src: 'src',
    out: 'doc',
    mode: 'file',
    name: 'Apache Arrow',
    target: 'ES6',
    module: 'commonjs',
    tsconfig: 'tsconfig.json',
    excludePrivate: true,
    excludeProtected: true,
    excludeNotExported: true,
    includeDefinitions: true,
    ignoreCompilerErrors: true,
    exclude: [
        'src/fb/*.ts',
        'src/bin/*.ts'
    ]
};
