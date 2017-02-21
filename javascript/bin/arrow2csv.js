#! /usr/bin/env node

var fs = require('fs')
var process = require('process');
var loadVectors = require('../dist/arrow.js').loadVectors;
var program = require('commander');

function list (val) {
    return val.split(',');
}

program
  .version('0.1.0')
  .usage('[options] <file>')
  .option('-s --schema <list>', 'A comma-separated list of column names', list)
  .parse(process.argv);

if (!program.schema) {
    program.outputHelp();
    process.exit(1);
}

var buf = fs.readFileSync(process.argv[process.argv.length - 1]);
var vectors = loadVectors(buf);

for (var i = 0; i < vectors[program.schema[0]].length; i += 1|0) {
    console.log(program.schema.map(function (field) {
        return '' + vectors[field].get(i);
    }).join(','));
}
