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

export { Builder } from '../builder.js';
export type { BuilderOptions } from '../builder.js';
export { BoolBuilder } from './bool.js';
export { NullBuilder } from './null.js';
export { DateBuilder, DateDayBuilder, DateMillisecondBuilder } from './date.js';
export { DecimalBuilder } from './decimal.js';
export { DictionaryBuilder } from './dictionary.js';
export { FixedSizeBinaryBuilder } from './fixedsizebinary.js';
export { FloatBuilder, Float16Builder, Float32Builder, Float64Builder } from './float.js';
export { IntBuilder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, Uint8Builder, Uint16Builder, Uint32Builder, Uint64Builder } from './int.js';
export { TimeBuilder, TimeSecondBuilder, TimeMillisecondBuilder, TimeMicrosecondBuilder, TimeNanosecondBuilder } from './time.js';
export { TimestampBuilder, TimestampSecondBuilder, TimestampMillisecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder } from './timestamp.js';
export { IntervalBuilder, IntervalDayTimeBuilder, IntervalYearMonthBuilder } from './interval.js';
export { Utf8Builder } from './utf8.js';
export { BinaryBuilder } from './binary.js';
export { ListBuilder } from './list.js';
export { FixedSizeListBuilder } from './fixedsizelist.js';
export { MapBuilder } from './map.js';
export { StructBuilder } from './struct.js';
export { UnionBuilder, SparseUnionBuilder, DenseUnionBuilder } from './union.js';

import { Field } from '../schema.js';
import { DataType } from '../type.js';
import { BuilderType as B } from '../interfaces.js';
import { Builder, BuilderOptions } from '../builder.js';
import { instance as getBuilderConstructor } from '../visitor/builderctor.js';

export function newBuilder<T extends DataType = any, TNull = any>(options: BuilderOptions<T, TNull>): B<T, TNull> {

    const type = options.type;
    const builder = new (getBuilderConstructor.getVisitFn<T>(type)())(options) as Builder<T, TNull>;

    if (type.children && type.children.length > 0) {

        const children = options['children'] || [] as BuilderOptions[];
        const defaultOptions = { 'nullValues': options['nullValues'] };
        const getChildOptions = Array.isArray(children)
            ? ((_: Field, i: number) => children[i] || defaultOptions)
            : (({ name }: Field) => children[name] || defaultOptions);

        for (const [index, field] of type.children.entries()) {
            const { type } = field;
            const opts = getChildOptions(field, index);
            builder.children.push(newBuilder({ ...opts, type }));
        }
    }

    return builder as B<T, TNull>;
}
