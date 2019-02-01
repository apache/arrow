import { DataBuilder } from './base';
import { BoolBuilder } from './bool';
import { IntBuilder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, Uint8Builder, Uint16Builder, Uint32Builder, Uint64Builder } from './int';
import { FloatBuilder, Float16Builder, Float32Builder, Float64Builder } from './float';
import { Utf8Builder } from './utf8';
import { BinaryBuilder } from './binary';
import { FixedSizeBinaryBuilder } from './fixedsizebinary';
import { DateBuilder, DateDayBuilder, DateMillisecondBuilder } from './date';
import { TimestampBuilder, TimestampSecondBuilder, TimestampMillisecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder } from './timestamp';
import { TimeBuilder, TimeSecondBuilder, TimeMillisecondBuilder, TimeMicrosecondBuilder, TimeNanosecondBuilder } from './time';
import { DecimalBuilder } from './decimal';
import { DictionaryBuilder } from './dictionary';
// import { ListBuilder } from './list';
// import { StructBuilder } from './struct';
// import { UnionBuilder, DenseUnionBuilder, SparseUnionBuilder } from './union';
import { IntervalBuilder, IntervalDayTimeBuilder, IntervalYearMonthBuilder } from './interval';
// import { FixedSizeListBuilder } from './fixedsizelist';
// import { MapBuilder } from './map';

export { DataBuilder };
export { BoolBuilder };
export { IntBuilder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, Uint8Builder, Uint16Builder, Uint32Builder, Uint64Builder };
export { FloatBuilder, Float16Builder, Float32Builder, Float64Builder };
export { Utf8Builder };
export { BinaryBuilder };
export { FixedSizeBinaryBuilder };
export { DateBuilder, DateDayBuilder, DateMillisecondBuilder };
export { TimestampBuilder, TimestampSecondBuilder, TimestampMillisecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder };
export { TimeBuilder, TimeSecondBuilder, TimeMillisecondBuilder, TimeMicrosecondBuilder, TimeNanosecondBuilder };
export { DecimalBuilder };
export { DictionaryBuilder };
// export { ListBuilder };
// export { StructBuilder };
// export { UnionBuilder, DenseUnionBuilder, SparseUnionBuilder };
export { IntervalBuilder, IntervalDayTimeBuilder, IntervalYearMonthBuilder };
// export { FixedSizeListBuilder };
// export { MapBuilder };

import { SetVisitor } from '../visitor/set';

(                 IntBuilder.prototype as any)._setValue = SetVisitor.prototype.visitInt;
(                Int8Builder.prototype as any)._setValue = SetVisitor.prototype.visitInt8;
(               Int16Builder.prototype as any)._setValue = SetVisitor.prototype.visitInt16;
(               Int32Builder.prototype as any)._setValue = SetVisitor.prototype.visitInt32;
(               Int64Builder.prototype as any)._setValue = SetVisitor.prototype.visitInt64;
(               Uint8Builder.prototype as any)._setValue = SetVisitor.prototype.visitUint8;
(              Uint16Builder.prototype as any)._setValue = SetVisitor.prototype.visitUint16;
(              Uint32Builder.prototype as any)._setValue = SetVisitor.prototype.visitUint32;
(              Uint64Builder.prototype as any)._setValue = SetVisitor.prototype.visitUint64;
(               FloatBuilder.prototype as any)._setValue = SetVisitor.prototype.visitFloat;
(             Float16Builder.prototype as any)._setValue = SetVisitor.prototype.visitFloat16;
(             Float32Builder.prototype as any)._setValue = SetVisitor.prototype.visitFloat32;
(             Float64Builder.prototype as any)._setValue = SetVisitor.prototype.visitFloat64;
(                Utf8Builder.prototype as any)._setValue = SetVisitor.prototype.visitBinary;// visitUtf8;
(              BinaryBuilder.prototype as any)._setValue = SetVisitor.prototype.visitBinary;
(     FixedSizeBinaryBuilder.prototype as any)._setValue = SetVisitor.prototype.visitFixedSizeBinary;
(                DateBuilder.prototype as any)._setValue = SetVisitor.prototype.visitDate;
(             DateDayBuilder.prototype as any)._setValue = SetVisitor.prototype.visitDateDay;
(     DateMillisecondBuilder.prototype as any)._setValue = SetVisitor.prototype.visitDateMillisecond;
(           TimestampBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTimestamp;
(     TimestampSecondBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTimestampSecond;
(TimestampMillisecondBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTimestampMillisecond;
(TimestampMicrosecondBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTimestampMicrosecond;
( TimestampNanosecondBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTimestampNanosecond;
(                TimeBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTime;
(          TimeSecondBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTimeSecond;
(     TimeMillisecondBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTimeMillisecond;
(     TimeMicrosecondBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTimeMicrosecond;
(      TimeNanosecondBuilder.prototype as any)._setValue = SetVisitor.prototype.visitTimeNanosecond;
(             DecimalBuilder.prototype as any)._setValue = SetVisitor.prototype.visitDecimal;
// (                ListBuilder.prototype as any)._setValue = SetVisitor.prototype.visitList;
// (              StructBuilder.prototype as any)._setValue = SetVisitor.prototype.visitStruct;
// (               UnionBuilder.prototype as any)._setValue = SetVisitor.prototype.visitUnion;
// (          DenseUnionBuilder.prototype as any)._setValue = SetVisitor.prototype.visitDenseUnion;
// (         SparseUnionBuilder.prototype as any)._setValue = SetVisitor.prototype.visitSparseUnion;
(            IntervalBuilder.prototype as any)._setValue = SetVisitor.prototype.visitInterval;
(     IntervalDayTimeBuilder.prototype as any)._setValue = SetVisitor.prototype.visitIntervalDayTime;
(   IntervalYearMonthBuilder.prototype as any)._setValue = SetVisitor.prototype.visitIntervalYearMonth;
// (       FixedSizeListBuilder.prototype as any)._setValue = SetVisitor.prototype.visitFixedSizeList;
// (                 MapBuilder.prototype as any)._setValue = SetVisitor.prototype.visitMap;

import { Type, Precision, TimeUnit, DateUnit, IntervalUnit, /* UnionMode */ } from '../enum';
import { DataType, Float, Int, Date_, Interval, Time, Timestamp } from '../type';

export function createBuilderFromType(type: any, nullValues?: any[], chunkSize?: number) {
    switch ((type as DataType).typeId) {
        // case Type.Null: return Type.Null;
        case Type.Int:
            const { bitWidth, isSigned } = (type as any as Int);
            switch (bitWidth) {
                case  8: return isSigned ? new Int8Builder(nullValues, chunkSize)  : new Uint8Builder(nullValues, chunkSize);
                case 16: return isSigned ? new Int16Builder(nullValues, chunkSize) : new Uint16Builder(nullValues, chunkSize);
                case 32: return isSigned ? new Int32Builder(nullValues, chunkSize) : new Uint32Builder(nullValues, chunkSize);
                case 64: return isSigned ? new Int64Builder(nullValues, chunkSize) : new Uint64Builder(nullValues, chunkSize);
            }
            return Type.Int;
        case Type.Float:
            switch((type as any as Float).precision) {
                case Precision.HALF: return new Float16Builder(nullValues, chunkSize);
                case Precision.SINGLE: return new Float32Builder(nullValues, chunkSize);
                case Precision.DOUBLE: return new Float64Builder(nullValues, chunkSize);
            }
            return Type.Float;
        case Type.Binary: return new BinaryBuilder(nullValues, chunkSize);
        case Type.Utf8: return new Utf8Builder(nullValues, chunkSize);
        case Type.Bool: return new BoolBuilder(nullValues, chunkSize);
        case Type.Decimal: return new DecimalBuilder(type, nullValues, chunkSize);
        case Type.Time:
            switch ((type as any as Time).unit) {
                case TimeUnit.SECOND: return new TimeSecondBuilder(nullValues, chunkSize);
                case TimeUnit.MILLISECOND: return new TimeMillisecondBuilder(nullValues, chunkSize);
                case TimeUnit.MICROSECOND: return new TimeMicrosecondBuilder(nullValues, chunkSize);
                case TimeUnit.NANOSECOND: return new TimeNanosecondBuilder(nullValues, chunkSize);
            }
            return Type.Time;
        case Type.Timestamp:
            switch ((type as any as Timestamp).unit) {
                case TimeUnit.SECOND: return new TimestampSecondBuilder(nullValues, chunkSize);
                case TimeUnit.MILLISECOND: return new TimestampMillisecondBuilder(nullValues, chunkSize);
                case TimeUnit.MICROSECOND: return new TimestampMicrosecondBuilder(nullValues, chunkSize);
                case TimeUnit.NANOSECOND: return new TimestampNanosecondBuilder(nullValues, chunkSize);
            }
            return Type.Timestamp;
        case Type.Date:
            switch ((type as any as Date_).unit) {
                case DateUnit.DAY: return new DateDayBuilder(nullValues, chunkSize);
                case DateUnit.MILLISECOND: return new DateMillisecondBuilder(nullValues, chunkSize);
            }
            return Type.Date;
        case Type.Interval:
            switch ((type as any as Interval).unit) {
                case IntervalUnit.DAY_TIME: return new IntervalDayTimeBuilder(nullValues, chunkSize);
                case IntervalUnit.YEAR_MONTH: return new IntervalYearMonthBuilder(nullValues, chunkSize);
            }
            return Type.Interval;
        // case Type.Map: return new MapBuilder(type, nullValues, chunkSize);
        // case Type.List: return new ListBuilder(type, nullValues, chunkSize);
        // case Type.Struct: return new StructBuilder(type, nullValues, chunkSize);
        // case Type.Union:
        //     switch ((type as any as Union).mode) {
        //         case UnionMode.Dense: return new DenseUnionBuilder(type, nullValues, chunkSize);
        //         case UnionMode.Sparse: return new SparseUnionBuilder(type, nullValues, chunkSize);
        //     }
        //     return Type.Union;
        // case Type.FixedSizeBinary: return new FixedSizeBinaryBuilder(type, nullValues, chunkSize);
        // case Type.FixedSizeList: return new FixedSizeListBuilder(type, nullValues, chunkSize);
        case Type.Dictionary: return new DictionaryBuilder(type, nullValues, chunkSize);
    }
    throw new Error(`Builder for ${Type[type.typeId]} not implemented yet`);
}
