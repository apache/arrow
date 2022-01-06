import { vectorFromArray } from 'apache-arrow';

const vec = vectorFromArray([1, 2, 3]);

console.log(vec.toArray());
