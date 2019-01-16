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

import { DataType, Dictionary } from './type';
import { Vector as VType } from './interfaces';

export class Schema<T extends { [key: string]: DataType } = any> {

    /** @nocollapse */
    public static from<T extends { [key: string]: DataType } = any>(vectors: VType<T[keyof T]>[], names: (keyof T)[] = []) {
        return new Schema<T>(vectors.map((v, i) => new Field('' + (names[i] || i), v.type)));
    }

    protected _fields: Field[];
    protected _metadata: Map<string, string>;
    protected _dictionaries: Map<number, DataType>;
    protected _dictionaryFields: Map<number, Field<Dictionary>[]>;
    public get fields(): Field[] { return this._fields; }
    public get metadata(): Map<string, string> { return this._metadata; }
    public get dictionaries(): Map<number, DataType> { return this._dictionaries; }
    public get dictionaryFields(): Map<number, Field<Dictionary>[]> { return this._dictionaryFields; }

    constructor(fields: Field[],
                metadata?: Map<string, string>,
                dictionaries?: Map<number, DataType>,
                dictionaryFields?: Map<number, Field<Dictionary>[]>) {
        this._fields = fields || [];
        this._metadata = metadata || new Map();
        if (!dictionaries || !dictionaryFields) {
            ({ dictionaries, dictionaryFields } = generateDictionaryMap(
                fields, dictionaries || new Map(), dictionaryFields || new Map()
            ));
        }
        this._dictionaries = dictionaries;
        this._dictionaryFields = dictionaryFields;
    }
    public get [Symbol.toStringTag]() { return 'Schema'; }
    public toString() {
        return `Schema<{ ${this._fields.map((f, i) => `${i}: ${f}`).join(', ')} }>`;
    }
    public select<K extends keyof T = any>(...columnNames: K[]) {
        const names = columnNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
        return new Schema<{ [P in K]: T[P] }>(this.fields.filter((f) => names[f.name]), this.metadata);
    }
}

export class Field<T extends DataType = DataType> {
    protected _type: T;
    protected _name: string;
    protected _nullable: true | false;
    protected _metadata?: Map<string, string> | null;
    constructor(name: string, type: T, nullable: true | false = false, metadata?: Map<string, string> | null) {
        this._name = name;
        this._type = type;
        this._nullable = nullable;
        this._metadata = metadata || new Map();
    }
    public get type() { return this._type; }
    public get name() { return this._name; }
    public get nullable() { return this._nullable; }
    public get metadata() { return this._metadata; }
    public get typeId() { return this._type.typeId; }
    public get [Symbol.toStringTag]() { return 'Field'; }
    public get indices() {
        return DataType.isDictionary(this._type) ? this._type.indices : this._type;
    }
    public toString() { return `${this.name}: ${this.type}`; }
}

/** @ignore */
function generateDictionaryMap(fields: Field[], dictionaries: Map<number, DataType>, dictionaryFields: Map<number, Field<Dictionary>[]>) {

    for (let i = -1, n = fields.length; ++i < n;) {
        const field = fields[i];
        const type = field.type;
        if (DataType.isDictionary(type)) {
            if (!dictionaryFields.get(type.id)) {
                dictionaryFields.set(type.id, []);
            }
            if (!dictionaries.has(type.id)) {
                dictionaries.set(type.id, type.dictionary);
                dictionaryFields.get(type.id)!.push(field as any);
            } else if (dictionaries.get(type.id) !== type.dictionary) {
                throw new Error(`Cannot create Schema containing two different dictionaries with the same Id`);
            }
        }
        if (type.children) {
            generateDictionaryMap(type.children, dictionaries, dictionaryFields);
        }
    }

    return { dictionaries, dictionaryFields };
}
