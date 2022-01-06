import { tableFrom } from 'apache-arrow';

const table = tableFrom({
    a: [1, 2, 3]
});

console.log(table)
