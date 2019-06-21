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
import { selectArgs } from './util/args';
import { DataType, Dictionary } from './type';
import { selectFieldArgs } from './util/args';
import { instance as comparer } from './visitor/typecomparator';

type VectorMap = { [key: string]: Vector };
type Fields<T extends { [key: string]: DataType }> = (keyof T)[] | Field<T[keyof T]>[];
type ChildData<T extends { [key: string]: DataType }> = T[keyof T][] | Data<T[keyof T]>[] | Vector<T[keyof T]>[];

export class Schema<T extends { [key: string]: DataType } = any> {

    public static from<T extends { [key: string]: DataType } = any>(children: T): Schema<T>;
    public static from<T extends VectorMap = any>(children: T): Schema<{ [P in keyof T]: T[P]['type'] }>;
    public static from<T extends { [key: string]: DataType } = any>(children: ChildData<T>, fields?: Fields<T>): Schema<T>;
    /** @nocollapse */
    public static from(...args: any[]) {
        return Schema.new(args[0], args[1]);
    }

    public static new<T extends { [key: string]: DataType } = any>(children: T): Schema<T>;
    public static new<T extends VectorMap = any>(children: T): Schema<{ [P in keyof T]: T[P]['type'] }>;
    public static new<T extends { [key: string]: DataType } = any>(children: ChildData<T>, fields?: Fields<T>): Schema<T>;
    /** @nocollapse */
    public static new(...args: any[]) {
        return new Schema(selectFieldArgs(args)[0]);
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
            : new Schema<R>(selectArgs<Field<R[keyof R]>>(Field, args));

        const curFields = [...this.fields] as Field[];
        const curDictionaryFields = this.dictionaryFields;
        const metadata = mergeMaps(mergeMaps(new Map(), this.metadata), other.metadata);
        const newFields = other.fields.filter((f2) => {
            const i = curFields.findIndex((f) => f.name === f2.name);
            return ~i ? (curFields[i] = f2.clone({
                metadata: mergeMaps(mergeMaps(new Map(), curFields[i].metadata), f2.metadata)
            })) && false : true;
        }) as Field[];

        const { dictionaries: newDictionaries, dictionaryFields } = generateDictionaryMap(newFields, new Map(), new Map());
        const newDictionaryFields = [...dictionaryFields].map(([id, newDictFields]) => {
            return [id, [...(curDictionaryFields.get(id) || []), ...newDictFields.map((f) => {
                return newFields[newFields.findIndex((f2) => f.name === f2.name)] = f.clone();
            })]] as [number, Field<Dictionary>[]];
        });

        return new Schema<T & R>(
            [...curFields, ...newFields], metadata,
            new Map([...this.dictionaries, ...newDictionaries]),
            new Map([...curDictionaryFields, ...newDictionaryFields])
        );
    }
}

export class Field<T extends DataType = any> {

    public static new<T extends DataType = any>(props: { name: string | number, type: T, nullable?: boolean, metadata?: Map<string, string> | null }): Field<T>;
    public static new<T extends DataType = any>(name: string | number | Field<T>, type: T, nullable?: boolean, metadata?: Map<string, string> | null): Field<T>;
    /** @nocollapse */
    public static new<T extends DataType = any>(...args: any[]) {
        let [name, type, nullable, metadata] = args;
        if (args[0] && typeof args[0] === 'object') {
            ({ name } = args[0]);
            (type === undefined) && (type = args[0].type);
            (nullable === undefined) && (nullable = args[0].nullable);
            (metadata === undefined) && (metadata = args[0].metadata);
        }
        return new Field<T>(`${name}`, type, nullable, metadata);
    }

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
    public clone<R extends DataType = T>(props: { name?: string | number, type?: R, nullable?: boolean, metadata?: Map<string, string> | null }): Field<R>;
    public clone<R extends DataType = T>(name?: string | number | Field<T>, type?: R, nullable?: boolean, metadata?: Map<string, string> | null): Field<R>;
    public clone<R extends DataType = T>(...args: any[]) {
        let [name, type, nullable, metadata] = args;
        (!args[0] || typeof args[0] !== 'object')
            ? ([name = this.name, type = this.type, nullable = this.nullable, metadata = this.metadata] = args)
            : ({name = this.name, type = this.type, nullable = this.nullable, metadata = this.metadata} = args[0]);
        return Field.new<R>(name, type, nullable, metadata);
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
