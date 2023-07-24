// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ipc
{
    internal class MessageSerializer
    {
        public const int IpcContinuationToken = -1;

        public static Types.NumberType GetNumberType(int bitWidth, bool signed)
        {
            if (signed)
            {
                if (bitWidth == 8)
                    return Types.Int8Type.Default;
                if (bitWidth == 16)
                    return Types.Int16Type.Default;
                if (bitWidth == 32)
                    return Types.Int32Type.Default;
                if (bitWidth == 64)
                    return Types.Int64Type.Default;
            }
            else
            {
                if (bitWidth == 8)
                    return Types.UInt8Type.Default;
                if (bitWidth == 16)
                    return Types.UInt16Type.Default;
                if (bitWidth == 32)
                    return Types.UInt32Type.Default;
                if (bitWidth == 64)
                    return Types.UInt64Type.Default;
            }
            throw new Exception($"Unexpected bit width of {bitWidth} for " +
                                $"{(signed ? "signed " : "unsigned")} integer.");
        }

        internal static Schema GetSchema(Flatbuf.Schema schema, ref DictionaryMemo dictionaryMemo)
        {
            List<Field> fields = new List<Field>();
            for (int i = 0; i < schema.FieldsLength; i++)
            {
                Flatbuf.Field field = schema.Fields(i).GetValueOrDefault();
                fields.Add(FieldFromFlatbuffer(field, ref dictionaryMemo));
            }

            Dictionary<string, string> metadata = schema.CustomMetadataLength > 0 ? new Dictionary<string, string>() : null;
            for (int i = 0; i < schema.CustomMetadataLength; i++)
            {
                Flatbuf.KeyValue keyValue = schema.CustomMetadata(i).GetValueOrDefault();

                metadata[keyValue.Key] = keyValue.Value;
            }

            return new Schema(fields, metadata, copyCollections: false);
        }

        private static Field FieldFromFlatbuffer(Flatbuf.Field flatbufField, ref DictionaryMemo dictionaryMemo)
        {
            Field[] childFields = flatbufField.ChildrenLength > 0 ? new Field[flatbufField.ChildrenLength] : null;
            for (int i = 0; i < flatbufField.ChildrenLength; i++)
            {
                Flatbuf.Field? childFlatbufField = flatbufField.Children(i);
                childFields[i] = FieldFromFlatbuffer(childFlatbufField.Value, ref dictionaryMemo);
            }

            Flatbuf.DictionaryEncoding? dictionaryEncoding = flatbufField.Dictionary;
            IArrowType type = GetFieldArrowType(flatbufField, childFields);

            if (dictionaryEncoding.HasValue)
            {
                Flatbuf.Int? indexTypeAsInt = dictionaryEncoding.Value.IndexType;
                IArrowType indexType = indexTypeAsInt.HasValue ?
                    GetNumberType(indexTypeAsInt.Value.BitWidth, indexTypeAsInt.Value.IsSigned) :
                    GetNumberType(Int32Type.Default.BitWidth, Int32Type.Default.IsSigned);

                type = new DictionaryType(indexType, type, dictionaryEncoding.Value.IsOrdered);
            }

            Dictionary<string, string> metadata = flatbufField.CustomMetadataLength > 0 ? new Dictionary<string, string>() : null;
            for (int i = 0; i < flatbufField.CustomMetadataLength; i++)
            {
                Flatbuf.KeyValue keyValue = flatbufField.CustomMetadata(i).GetValueOrDefault();

                metadata[keyValue.Key] = keyValue.Value;
            }

            var arrowField = new Field(flatbufField.Name, type, flatbufField.Nullable, metadata, copyCollections: false);

            if (dictionaryEncoding.HasValue)
            {
                dictionaryMemo ??= new DictionaryMemo();
                dictionaryMemo.AddField(dictionaryEncoding.Value.Id, arrowField);
            }

            return arrowField;
        }

        private static Types.IArrowType GetFieldArrowType(Flatbuf.Field field, Field[] childFields = null)
        {
            switch (field.TypeType)
            {
                case Flatbuf.Type.Null:
                    return Types.NullType.Default;
                case Flatbuf.Type.Int:
                    Flatbuf.Int intMetaData = field.Type<Flatbuf.Int>().Value;
                    return MessageSerializer.GetNumberType(intMetaData.BitWidth, intMetaData.IsSigned);
                case Flatbuf.Type.FloatingPoint:
                    Flatbuf.FloatingPoint floatingPointTypeMetadata = field.Type<Flatbuf.FloatingPoint>().Value;
                    switch (floatingPointTypeMetadata.Precision)
                    {
                        case Flatbuf.Precision.SINGLE:
                            return Types.FloatType.Default;
                        case Flatbuf.Precision.DOUBLE:
                            return Types.DoubleType.Default;
                        case Flatbuf.Precision.HALF:
                            return Types.HalfFloatType.Default;
                        default:
                            throw new InvalidDataException("Unsupported floating point precision");
                    }
                case Flatbuf.Type.Bool:
                    return Types.BooleanType.Default;
                case Flatbuf.Type.Decimal:
                    Flatbuf.Decimal decMeta = field.Type<Flatbuf.Decimal>().Value;
                    switch (decMeta.BitWidth)
                    {
                        case 128:
                            return new Types.Decimal128Type(decMeta.Precision, decMeta.Scale);
                        case 256:
                            return new Types.Decimal256Type(decMeta.Precision, decMeta.Scale);
                        default:
                            throw new InvalidDataException("Unsupported decimal bit width " + decMeta.BitWidth);
                    }
                case Flatbuf.Type.Date:
                    Flatbuf.Date dateMeta = field.Type<Flatbuf.Date>().Value;
                    switch (dateMeta.Unit)
                    {
                        case Flatbuf.DateUnit.DAY:
                            return Types.Date32Type.Default;
                        case Flatbuf.DateUnit.MILLISECOND:
                            return Types.Date64Type.Default;
                        default:
                            throw new InvalidDataException("Unsupported date unit");
                    }
                case Flatbuf.Type.Time:
                    Flatbuf.Time timeMeta = field.Type<Flatbuf.Time>().Value;
                    switch (timeMeta.BitWidth)
                    {
                        case 32:
                            return new Types.Time32Type(timeMeta.Unit.ToArrow());
                        case 64:
                            return new Types.Time64Type(timeMeta.Unit.ToArrow());
                        default:
                            throw new InvalidDataException("Unsupported time bit width");
                    }
                case Flatbuf.Type.Timestamp:
                    Flatbuf.Timestamp timestampTypeMetadata = field.Type<Flatbuf.Timestamp>().Value;
                    Types.TimeUnit unit = timestampTypeMetadata.Unit.ToArrow();
                    string timezone = timestampTypeMetadata.Timezone;
                    return new Types.TimestampType(unit, timezone);
                case Flatbuf.Type.Interval:
                    Flatbuf.Interval intervalMetadata = field.Type<Flatbuf.Interval>().Value;
                    return new Types.IntervalType(intervalMetadata.Unit.ToArrow());
                case Flatbuf.Type.Utf8:
                    return Types.StringType.Default;
                case Flatbuf.Type.FixedSizeBinary:
                    Flatbuf.FixedSizeBinary fixedSizeBinaryMetadata = field.Type<Flatbuf.FixedSizeBinary>().Value;
                    return new Types.FixedSizeBinaryType(fixedSizeBinaryMetadata.ByteWidth);
                case Flatbuf.Type.Binary:
                    return Types.BinaryType.Default;
                case Flatbuf.Type.List:
                    if (childFields == null || childFields.Length != 1)
                    {
                        throw new InvalidDataException($"List type must have exactly one child.");
                    }
                    return new Types.ListType(childFields[0]);
                case Flatbuf.Type.Struct_:
                    Debug.Assert(childFields != null);
                    return new Types.StructType(childFields);
                default:
                    throw new InvalidDataException($"Arrow primitive '{field.TypeType}' is unsupported.");
            }
        }
    }
}
