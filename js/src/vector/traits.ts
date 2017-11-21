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

import { Vector } from './vector';
import { BoolVector } from './numeric';
import { fb, FieldBuilder, FieldNodeBuilder } from '../format/arrow';

export type Field = ( fb.Schema.Field | FieldBuilder );
export type FieldNode = ( fb.Message.FieldNode | FieldNodeBuilder );

function isField(x: any): x is Field {
    return x instanceof fb.Schema.Field || x instanceof FieldBuilder;
}

function isFieldNode(x: any): x is FieldNode {
    return x instanceof fb.Message.FieldNode || x instanceof FieldNodeBuilder;
}

export function isFieldArgv(x: any): x is { field: Field, fieldNode: FieldNode } {
    return x && isField(x.field) && isFieldNode(x.fieldNode);
}

export function isNullableArgv(x: any): x is { validity: Uint8Array } {
    return x && x.validity && ArrayBuffer.isView(x.validity) && x.validity instanceof Uint8Array;
}

type Ctor<TArgv> = new (argv: TArgv) => Vector;

export const nullableMixin = <T extends Vector, TArgv>(superclass: new (argv: TArgv) => T) =>
    class extends (superclass as Ctor<TArgv>) {
        readonly validity: Vector<boolean>;
        constructor(argv: TArgv & { validity: Uint8Array }) {
            super(argv);
            this.validity = new BoolVector({ data: argv.validity });
        }
        get(index: number) {
            return this.validity.get(index) ? super.get(index) : null;
        }
    };

export const fieldMixin = <T extends Vector, TArgv>(superclass: new (argv: TArgv) => T) =>
    class extends (superclass as Ctor<TArgv>) implements Vector {
        readonly field: Field;
        readonly type: string;
        readonly length: number;
        readonly stride: number;
        readonly nullable: boolean;
        readonly nullCount: number;
        readonly fieldNode: FieldNode;
        constructor(argv: TArgv & { field: Field, fieldNode: FieldNode }) {
            super(argv);
            const { field, fieldNode } = argv;
            this.field = field;
            this.fieldNode = fieldNode;
            this.nullable = field.nullable();
            this.type = fb.Schema.Type[field.typeType()];
            this.length = fieldNode.length().low | 0;
            this.nullCount = fieldNode.nullCount().low;
        }
        get name() { return this.field.name()!; }
        get metadata()  {
            const { field } = this, data = new Map<string, string>();
            for (let entry, key, i = -1, n = field && field.customMetadataLength() | 0; ++i < n;) {
                if ((entry = field.customMetadata(i)) && (key = entry.key()) != null) {
                    data.set(key, entry.value()!);
                }
            }
            return data;
        }
    };
