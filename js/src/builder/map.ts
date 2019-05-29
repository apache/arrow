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

import { Field } from '../schema';
import { DataType, Map_ } from '../type';
import { Builder, NestedBuilder } from './base';

export class MapBuilder<T extends { [key: string]: DataType } = any, TNull = any> extends NestedBuilder<Map_<T>, TNull> {
    public addChild(child: Builder<T[keyof T]>, name = `${this.numChildren}`) {
        const type = this._type;
        const childIndex = this.children.push(child);
        this._type = new Map_([...type.children, new Field(name, child.type)], type.keysSorted);
        return childIndex;
    }
}
