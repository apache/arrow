import * as arrow from './src/Arrow.dom.js'
import fs from 'fs'

const file = fs.readFileSync('/Users/dominik/Downloads/generated_decimal.arrow_file');
const table = arrow.tableFromIPC(file)
console.log(table.toString());

const json_file = fs.readFileSync('/Users/dominik/Downloads/generated_decimal.json');
const json_content = JSON.parse(Buffer.from(json_file).toString());
const table_json = arrow.tableFromIPC(json_content)
console.log(table_json.toString());

const json = await arrow.RecordBatchJSONWriter.writeAll(table).toString();

console.log(json);


const json2 = await arrow.RecordBatchJSONWriter.writeAll(table_json).toString();

console.log(json2);
