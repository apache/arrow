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

export { Builder, BuilderOptions } from './base';
export { BinaryBuilder } from './binary';
export { BoolBuilder } from './bool';
export { DateBuilder, DateDayBuilder, DateMillisecondBuilder } from './date';
export { DecimalBuilder } from './decimal';
export { DictionaryBuilder } from './dictionary';
export { FixedSizeBinaryBuilder } from './fixedsizebinary';
export { FixedSizeListBuilder } from './fixedsizelist';
export { FloatBuilder, Float16Builder, Float32Builder, Float64Builder } from './float';
export { IntervalBuilder, IntervalDayTimeBuilder, IntervalYearMonthBuilder } from './interval';
export { IntBuilder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, Uint8Builder, Uint16Builder, Uint32Builder, Uint64Builder } from './int';
export { ListBuilder } from './list';
export { MapBuilder } from './map';
export { NullBuilder } from './null';
export { StructBuilder } from './struct';
export { TimestampBuilder, TimestampSecondBuilder, TimestampMillisecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder } from './timestamp';
export { TimeBuilder, TimeSecondBuilder, TimeMillisecondBuilder, TimeMicrosecondBuilder, TimeNanosecondBuilder } from './time';
export { UnionBuilder, DenseUnionBuilder, SparseUnionBuilder } from './union';
export { Utf8Builder } from './utf8';

import { Type } from '../enum';
import { Builder } from './base';
import { DataType } from '../type';
import { Utf8Builder } from './utf8';
import { BuilderOptions } from './base';
import { Builder as B } from '../interfaces';
import { instance as setVisitor } from '../visitor/set';
import { instance as getBuilderConstructor } from '../visitor/builderctor';

declare module './base' {
    namespace Builder {
        export { newBuilder as new };
    }
}

/** @nocollapse */
Builder.new = newBuilder;

/** @ignore */
function newBuilder<T extends DataType = any, TNull = any>(options: BuilderOptions<T, TNull>): B<T, TNull> {
    return new (getBuilderConstructor.getVisitFn<T>(options.type)())(options) as B<T, TNull>;
}

(Object.keys(Type) as any[])
    .map((T: any) => Type[T] as any)
    .filter((T: any): T is Type => typeof T === 'number')
    .filter((typeId) => typeId !== Type.NONE)
    .forEach((typeId) => {
        const BuilderCtor = getBuilderConstructor.visit(typeId);
        BuilderCtor.prototype._setValue = setVisitor.getVisitFn(typeId);
    });

(Utf8Builder.prototype as any)._setValue = setVisitor.getVisitFn(Type.Binary);
