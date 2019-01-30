import { Data } from '../../src/data';
import { Field } from '../../src/schema';
import { Column } from '../../src/column';
import { Vector } from '../../src/vector';
import { Bool, Int8, Utf8, List, Dictionary, Struct, Map_ } from '../../src/type';

const boolType = new Bool();
const boolVector = Vector.new(Data.Bool(boolType, 0, 10, 0, null, new Uint8Array(2)));

const boolColumn = new Column(new Field('bool', boolType), [
    Vector.new(Data.Bool(boolType, 0, 10, 0, null, new Uint8Array(2))),
    Vector.new(Data.Bool(boolType, 0, 10, 0, null, new Uint8Array(2))),
    Vector.new(Data.Bool(boolType, 0, 10, 0, null, new Uint8Array(2))),
]);

expect(typeof boolVector.get(0) === 'boolean').toBe(true);
expect(typeof boolColumn.get(0) === 'boolean').toBe(true);

type NamedSchema = {
    a: Int8,
    b: Utf8,
    c: Dictionary<List<Bool>>;
};

const mapChildFields = [
    { name: 'a', type: new Int8() },
    { name: 'b', type: new Utf8() },
    { name: 'c', type: new Dictionary<List<Bool>>(null!, null!) }
].map(({ name, type }) => new Field(name, type));

const mapType = new Map_<NamedSchema>(mapChildFields);

const mapVector = Vector.new(Data.Map(mapType, 0, 0, 0, null, []));
const mapColumn = new Column(new Field('map', mapType, false), [
    Vector.new(Data.Map(mapType, 0, 0, 0, null, [])),
    Vector.new(Data.Map(mapType, 0, 0, 0, null, [])),
    Vector.new(Data.Map(mapType, 0, 0, 0, null, [])),
]);

const { a: a1, b: b1, c: c1 } = mapVector.get(0)!;
const { a: a2, b: b2, c: c2 } = mapColumn.get(0)!;

console.log(a1, b1, c1);
console.log(a2, b2, c2);

type IndexSchema = {
    0: Int8,
    1: Utf8,
    2: Dictionary<List<Bool>>;
};

const structChildFields = [
    { name: 0, type: new Int8() },
    { name: 1, type: new Utf8() },
    { name: 2, type: new Dictionary<List<Bool>>(null!, null!) }
].map(({ name, type }) => new Field('' + name, type));

const structType = new Struct<IndexSchema>(structChildFields);
const structVector = Vector.new(Data.Struct(structType, 0, 0, 0, null, []));
const structColumn = new Column(new Field('struct', structType), [
    Vector.new(Data.Struct(structType, 0, 0, 0, null, [])),
    Vector.new(Data.Struct(structType, 0, 0, 0, null, [])),
    Vector.new(Data.Struct(structType, 0, 0, 0, null, [])),
]);

const [x1, y1, z1] = structVector.get(0)!;
const [x2, y2, z2] = structColumn.get(0)!;

console.log(x1, y1, z1);
console.log(x2, y2, z2);
