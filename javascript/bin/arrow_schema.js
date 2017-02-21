#! /usr/bin/env node

var fs = require('fs');
var process = require('process');
var loadSchema = require('../dist/arrow.js').loadSchema;

var buf = fs.readFileSync(process.argv[process.argv.length - 1]);
console.log(JSON.stringify(loadSchema(buf), null, '\t'));
