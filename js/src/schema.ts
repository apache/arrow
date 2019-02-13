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

import { Data } from './data';
import { Vector } from './vector';
import { DataType, Dictionary } from './type';
import { selectAndFlatten } from './util/array';
import { instance as comparer } from './visitor/typecomparator';

export class Schema<T extends { [key: string]: DataType } = any> {

    /** @nocollapse */
    public static from<T extends { [key: string]: DataType } = any>(chunks: (Data<T[keyof T]> | Vector<T[keyof T]>)[], names: (keyof T)[] = []) {
        return new Schema<T>(chunks.map((v, i) => new Field('' + (names[i] || i), v.type)));
    }

    public readonly fields: Field<T[keyof T]>[];
    public readonly metadata: Map<string, string>;
    public readonly dictionaries: Map<number, DataType>;
    public readonly dictionaryFields: Map<number, Field<Dictionary>[]>;

    constructor(fields: Field[] = [],
                metadata?: Map<string, string> | null,
                dictionaries?: Map<number, DataType> | null,
                dictionaryFields?: Map<number, Field<Dictionary>[]> | null) {
        this.fields = (fields || []) as Field<T[keyof T]>[];
        this.metadata = metadata || new Map();
        if (!dictionaries || !dictionaryFields) {
            ({ dictionaries, dictionaryFields } = generateDictionaryMap(
                fields, dictionaries || new Map(), dictionaryFields || new Map()
            ));
        }
        this.dictionaries = dictionaries;
        this.dictionaryFields = dictionaryFields;
    }
    public get [Symbol.toStringTag]() { return 'Schema'; }
    public toString() {
        return `Schema<{ ${this.fields.map((f, i) => `${i}: ${f}`).join(', ')} }>`;
    }

    public compareTo(other?: Schema | null): other is Schema<T> {
        return comparer.compareSchemas(this, other);
    }

    public select<K extends keyof T = any>(...columnNames: K[]) {
        const names = columnNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
        return new Schema<{ [P in K]: T[P] }>(this.fields.filter((f) => names[f.name]), this.metadata);
    }
    public selectAt<K extends T[keyof T] = any>(...columnIndices: number[]) {
        return new Schema<{ [key: string]: K }>(columnIndices.map((i) => this.fields[i]).filter(Boolean), this.metadata);
    }

    public assign<R extends { [key: string]: DataType } = any>(schema: Schema<R>): Schema<T & R>;
    public assign<R extends { [key: string]: DataType } = any>(...fields: (Field<R[keyof R]> | Field<R[keyof R]>[])[]): Schema<T & R>;
    public assign<R extends { [key: string]: DataType } = any>(...args: (Schema<R> | Field<R[keyof R]> | Field<R[keyof R]>[])[]) {

        const other = args[0] instanceof Schema ? args[0] as Schema<R>
            : new Schema<R>(selectAndFlatten<Field<R[keyof R]>>(Field, args));

        const curFields = [...this.fields] as Field[];
        const curDictionaries = [...this.dictionaries];
        const curDictionaryFields = this.dictionaryFields;
        const metadata = mergeMaps(this.metadata, other.metadata);
        const newFields = other.fields.filter((f2) => {
            const i = curFields.findIndex((f) => f.compareTo(f2));
            return ~i ? (curFields[i] = curFields[i].clone({
                metadata: mergeMaps(curFields[i].metadata, f2.metadata)
            })) && false : true;
        }) as Field[];

        const { dictionaries, dictionaryFields } = generateDictionaryMap(newFields, new Map(), new Map());
        const newDictionaries = [...dictionaries].filter(([y]) => !curDictionaries.every(([x]) => x === y));
        const newDictionaryFields = [...dictionaryFields].map(([id, newDictFields]) => {
            return [id, [...(curDictionaryFields.get(id) || []), ...newDictFields.map((f) => {
                const i = newFields.findIndex((f2) => f2.compareTo(f));
                const { dictionary, indices, isOrdered, dictionaryVector } = f.type;
                const type = new Dictionary(dictionary, indices, id, isOrdered, dictionaryVector);
                return newFields[i] = f.clone({ type });
            })]] as [number, Field<Dictionary>[]];
        });

        return new Schema<T & R>(
            [...curFields, ...newFields], metadata,
            new Map([...curDictionaries, ...newDictionaries]),
            new Map([...curDictionaryFields, ...newDictionaryFields])
        );
    }
}

export class Field<T extends DataType = DataType> {

    public readonly type: T;
    public readonly name: string;
    public readonly nullable: boolean;
    public readonly metadata: Map<string, string>;

    constructor(name: string, type: T, nullable = false, metadata?: Map<string, string> | null) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.metadata = metadata || new Map();
    }

    public get typeId() { return this.type.typeId; }
    public get [Symbol.toStringTag]() { return 'Field'; }
    public toString() { return `${this.name}: ${this.type}`; }
    public compareTo(other?: Field | null): other is Field<T> {
        return comparer.compareField(this, other);
    }
    public clone<R extends DataType = T>(props?: { name?: string, type?: R, nullable?: boolean, metadata?: Map<string, string> | null }): Field<R> {
        props || (props = {});
        return new Field<R>(
            props.name === undefined ? this.name : props.name,
            props.type === undefined ? this.type : props.type as any,
            props.nullable === undefined ? this.nullable : props.nullable,
            props.metadata === undefined ? this.metadata : props.metadata);
    }
}

/** @ignore */
function mergeMaps<TKey, TVal>(m1?: Map<TKey, TVal> | null, m2?: Map<TKey, TVal> | null): Map<TKey, TVal> {
    return new Map([...(m1 || new Map()), ...(m2 || new Map())]);
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

// Add these here so they're picked up by the externs creator
// in the build, and closure-compiler doesn't minify them away
(Schema.prototype as any).fields = null;
(Schema.prototype as any).metadata = null;
(Schema.prototype as any).dictionaries = null;
(Schema.prototype as any).dictionaryFields = null;

(Field.prototype as any).type = null;
(Field.prototype as any).name = null;
(Field.prototype as any).nullable = null;
(Field.prototype as any).metadata = null;
