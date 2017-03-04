module.exports = {
  entry: './dist/arrow.js',
  output: {
    path: __dirname + '/dist',
    filename: 'arrow-bundle.js',
    libraryTarget: 'var',
    library: 'arrow'
  }
};
