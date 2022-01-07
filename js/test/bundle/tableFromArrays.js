import { tableFromArrays } from 'apache-arrow';

const table = tableFromArrays({
    a: [1, 2, 3]
});

console.log(table)
