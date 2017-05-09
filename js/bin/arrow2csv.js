#! /usr/bin/env node

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

var fs = require('fs')
var process = require('process');
var arrow = require('../dist/arrow.js');
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
var reader = arrow.getReader(buf);
reader.loadNextBatch();

for (var i = 0; i < reader.getVector(program.schema[0]).length; i += 1|0) {
    console.log(program.schema.map(function (field) {
        return '' + reader.getVector(field).get(i);
    }).join(','));
}
