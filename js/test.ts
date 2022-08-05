import * as arrow from './src/Arrow.dom.js'
// import * as arrow from './targets/es5/cjs'

const LENGTH = 1000000;

const lat = Float32Array.from(
    { length: LENGTH },
    () => ((Math.random() - 0.5) * 2 * 90));
const lng = Float32Array.from(
    { length: LENGTH },
    () => ((Math.random() - 0.5) * 2 * 90));

const table = arrow.tableFromArrays({
    'lat': lat,
    'lng': lng
});

let total = 0;
for (const row of table) {
    total += row['lat'] + row['lng'];
}

console.log(total);
