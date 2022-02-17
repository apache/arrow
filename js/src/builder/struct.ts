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

/* eslint-disable unicorn/no-array-for-each */

import { Field } from '../schema.js';
import { Builder } from '../builder.js';
import { Struct, TypeMap } from '../type.js';

/** @ignore */
export class StructBuilder<T extends TypeMap = any, TNull = any> extends Builder<Struct<T>, TNull> {
    /**
     * Write a value (or null-value sentinel) at the supplied index.
     * If the value matches one of the null-value representations, a 1-bit is
     * written to the null `BitmapBufferBuilder`, otherwise, a 0 is written. The
     * value is then passed to `Builder.prototype.setValue()`.
     * @param {number} index The index of the value to write.
     * @param {T['TValue'] | TNull } value The value to write at the supplied index.
     * @returns {this} The updated `Builder` instance.
     */
    public set(index: number, value: Struct<T>['TValue'] | TNull) {
        this.setValid(index, this.isValid(value));
        this.setValue(index, value);
        return this;
    }

    public setValue(index: number, value: Struct<T>['TValue'] | TNull) {
        const children = this.children;
        if (this.isValid(value) && 'constructor' in value) {
            switch (Array.isArray(value) || value.constructor) {
                case true: return this.type.children.forEach((_, i) => children[i].set(index, value[i]));
                case Map: return this.type.children.forEach((f, i) => children[i].set(index, value.get(f.name)));
                default: return this.type.children.forEach((f, i) => children[i].set(index, value[f.name]));
            }
        } else { // Is a null value
            return this.type.children.forEach((_, i) => children[i].set(index, value));
        }
    }
    public addChild(child: Builder, name = `${this.numChildren}`) {
        const childIndex = this.children.push(child);
        this.type = new Struct([...this.type.children, new Field(name, child.type, true)]);
        return childIndex;
    }
}
