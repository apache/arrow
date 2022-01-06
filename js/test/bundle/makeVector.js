import { makeVector } from 'apache-arrow';

const vec = makeVector(new Uint8Array([1, 2, 3]));

console.log(vec.toArray());
