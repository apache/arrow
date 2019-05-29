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

import { Builder, BuilderOptions } from './base';
import { BoolBuilder } from './bool';
import { NullBuilder } from './null';
import { DateBuilder, DateDayBuilder, DateMillisecondBuilder } from './date';
import { DecimalBuilder } from './decimal';
import { DictionaryBuilder } from './dictionary';
import { FixedSizeBinaryBuilder } from './fixedsizebinary';
import { FloatBuilder, Float16Builder, Float32Builder, Float64Builder } from './float';
import { IntBuilder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, Uint8Builder, Uint16Builder, Uint32Builder, Uint64Builder } from './int';
import { TimeBuilder, TimeSecondBuilder, TimeMillisecondBuilder, TimeMicrosecondBuilder, TimeNanosecondBuilder } from './time';
import { TimestampBuilder, TimestampSecondBuilder, TimestampMillisecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder } from './timestamp';
import { IntervalBuilder, IntervalDayTimeBuilder, IntervalYearMonthBuilder } from './interval';
import { Utf8Builder } from './utf8';
import { BinaryBuilder } from './binary';
import { ListBuilder } from './list';
import { FixedSizeListBuilder } from './fixedsizelist';
import { MapBuilder } from './map';
import { StructBuilder } from './struct';
import { UnionBuilder, SparseUnionBuilder, DenseUnionBuilder } from './union';

import { Type } from '../enum';
import { DataType } from '../type';
import { Builder as B } from '../interfaces';
import { instance as setVisitor } from '../visitor/set';
import { instance as getBuilderConstructor } from '../visitor/builderctor';
import { Field } from '../schema';

declare module './base' {
    namespace Builder {
        export { newBuilder as new };
    }
}

/** @nocollapse */
Builder.new = newBuilder;

/** @ignore */
function newBuilder<T extends DataType = any, TNull = any>(options: BuilderOptions<T, TNull>): B<T, TNull> {

    const type = options.type;
    const builder = new (getBuilderConstructor.getVisitFn<T>(type)())(options) as Builder<T, TNull>;

    if (type.children && type.children.length > 0) {

        const children = options['children'] || [] as BuilderOptions[];
        const defaultOptions = { 'nullValues': options['nullValues'] };
        const getChildOptions = Array.isArray(children)
            ? ((_: Field, i: number) => children[i] || defaultOptions)
            : (({ name }: Field) => children[name] || defaultOptions);
    
        type.children.forEach((field, index) => {
            const { type } = field;
            const opts = getChildOptions(field, index);
            builder.children.push(newBuilder({ ...opts, type }));
        });
    }

    return builder as B<T, TNull>;
}

export { Builder, BuilderOptions };
export { BoolBuilder };
export { NullBuilder };
export { DateBuilder, DateDayBuilder, DateMillisecondBuilder };
export { DecimalBuilder };
export { DictionaryBuilder };
export { FixedSizeBinaryBuilder };
export { FloatBuilder, Float16Builder, Float32Builder, Float64Builder };
export { IntBuilder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, Uint8Builder, Uint16Builder, Uint32Builder, Uint64Builder };
export { IntervalBuilder, IntervalDayTimeBuilder, IntervalYearMonthBuilder };
export { TimeBuilder, TimeSecondBuilder, TimeMillisecondBuilder, TimeMicrosecondBuilder, TimeNanosecondBuilder };
export { TimestampBuilder, TimestampSecondBuilder, TimestampMillisecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder };
export { Utf8Builder };
export { BinaryBuilder };
export { ListBuilder };
export { FixedSizeListBuilder };
export { MapBuilder };
export { StructBuilder };
export { UnionBuilder, SparseUnionBuilder, DenseUnionBuilder };

(Object.keys(Type) as any[])
    .map((T: any) => Type[T] as any)
    .filter((T: any): T is Type => typeof T === 'number' && T !== Type.NONE)
    .forEach((typeId) => {
        const BuilderCtor = getBuilderConstructor.visit(typeId);
        BuilderCtor.prototype._setValue = setVisitor.getVisitFn(typeId);
    });

(Utf8Builder.prototype as any)._setValue = setVisitor.visitBinary;
