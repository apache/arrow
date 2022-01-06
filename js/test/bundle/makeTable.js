import { makeTable } from 'apache-arrow';

const table = makeTable({
    a: new Uint8Array([1, 2, 3])
});

console.log(table)
