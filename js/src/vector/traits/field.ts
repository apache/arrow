import { Vector } from '../vector';
import * as vectors from './vectors';
import { fieldMixin } from './mixins';
import { FieldBuilder, FieldNodeBuilder } from '../../format/arrow';

export { Vector, FieldBuilder, FieldNodeBuilder };
import * as Schema_ from '../../format/fb/Schema';
import * as Message_ from '../../format/fb/Message';
export import Field = Schema_.org.apache.arrow.flatbuf.Field;
export import FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;

export const FieldListVector = fieldMixin(vectors.ListVector);
export class ListVector extends FieldListVector {}
export const FieldBinaryVector = fieldMixin(vectors.BinaryVector);
export class BinaryVector extends FieldBinaryVector {}
export const FieldUtf8Vector = fieldMixin(vectors.Utf8Vector);
export class Utf8Vector extends FieldUtf8Vector {}
export const FieldBoolVector = fieldMixin(vectors.BoolVector);
export class BoolVector extends FieldBoolVector {}
export const FieldInt8Vector = fieldMixin(vectors.Int8Vector);
export class Int8Vector extends FieldInt8Vector {}
export const FieldInt16Vector = fieldMixin(vectors.Int16Vector);
export class Int16Vector extends FieldInt16Vector {}
export const FieldInt32Vector = fieldMixin(vectors.Int32Vector);
export class Int32Vector extends FieldInt32Vector {}
export const FieldInt64Vector = fieldMixin(vectors.Int64Vector);
export class Int64Vector extends FieldInt64Vector {}
export const FieldUint8Vector = fieldMixin(vectors.Uint8Vector);
export class Uint8Vector extends FieldUint8Vector {}
export const FieldUint16Vector = fieldMixin(vectors.Uint16Vector);
export class Uint16Vector extends FieldUint16Vector {}
export const FieldUint32Vector = fieldMixin(vectors.Uint32Vector);
export class Uint32Vector extends FieldUint32Vector {}
export const FieldUint64Vector = fieldMixin(vectors.Uint64Vector);
export class Uint64Vector extends FieldUint64Vector {}
export const FieldDate32Vector = fieldMixin(vectors.Date32Vector);
export class Date32Vector extends FieldDate32Vector {}
export const FieldDate64Vector = fieldMixin(vectors.Date64Vector);
export class Date64Vector extends FieldDate64Vector {}
export const FieldTime32Vector = fieldMixin(vectors.Time32Vector);
export class Time32Vector extends FieldTime32Vector {}
export const FieldTime64Vector = fieldMixin(vectors.Time64Vector);
export class Time64Vector extends FieldTime64Vector {}
export const FieldFloat16Vector = fieldMixin(vectors.Float16Vector);
export class Float16Vector extends FieldFloat16Vector {}
export const FieldFloat32Vector = fieldMixin(vectors.Float32Vector);
export class Float32Vector extends FieldFloat32Vector {}
export const FieldFloat64Vector = fieldMixin(vectors.Float64Vector);
export class Float64Vector extends FieldFloat64Vector {}
export const FieldStructVector = fieldMixin(vectors.StructVector);
export class StructVector extends FieldStructVector {}
export const FieldDecimalVector = fieldMixin(vectors.DecimalVector);
export class DecimalVector extends FieldDecimalVector {}
export const FieldTimestampVector = fieldMixin(vectors.TimestampVector);
export class TimestampVector extends FieldTimestampVector {}
export const FieldDictionaryVector = fieldMixin(vectors.DictionaryVector);
export class DictionaryVector extends FieldDictionaryVector {}
export const FieldFixedSizeListVector = fieldMixin(vectors.FixedSizeListVector);
export class FixedSizeListVector extends FieldFixedSizeListVector {}