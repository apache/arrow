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
using System.IO;

namespace Apache.Arrow.Ipc
{
    internal class MessageSerializer
    {

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

        internal static Schema GetSchema(Flatbuf.Schema schema)
        {
            var schemaBuilder = new Schema.Builder();

            for (var i = 0; i < schema.FieldsLength; i++)
            {
                var field = schema.Fields(i).GetValueOrDefault();

                schemaBuilder.Field(
                    new Field(field.Name, GetFieldArrowType(field), field.Nullable));
            }

            return schemaBuilder.Build();
        }


        private static Types.IArrowType GetFieldArrowType(Flatbuf.Field field)
        {
            switch (field.TypeType)
            {
                case Flatbuf.Type.Int:
                    var intMetaData = field.Type<Flatbuf.Int>().Value;
                    return MessageSerializer.GetNumberType(intMetaData.BitWidth, intMetaData.IsSigned);
                case Flatbuf.Type.FloatingPoint:
                    var floatingPointTypeMetadta = field.Type<Flatbuf.FloatingPoint>().Value;
                    switch (floatingPointTypeMetadta.Precision)
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
                    return new Types.BooleanType();
                case Flatbuf.Type.Decimal:
                    var decMeta = field.Type<Flatbuf.Decimal>().Value;
                    return new Types.DecimalType(decMeta.Precision, decMeta.Scale);
                case Flatbuf.Type.Date:
                    var dateMeta = field.Type<Flatbuf.Date>().Value;
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
                    var timeMeta = field.Type<Flatbuf.Time>().Value;
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
                    var timestampTypeMetadata = field.Type<Flatbuf.Timestamp>().Value;
                    var unit = timestampTypeMetadata.Unit.ToArrow();
                    var timezone = timestampTypeMetadata.Timezone;
                    return new Types.TimestampType(unit, timezone);
                case Flatbuf.Type.Interval:
                    var intervalMetadata = field.Type<Flatbuf.Interval>().Value;
                    return new Types.IntervalType(intervalMetadata.Unit.ToArrow());
                case Flatbuf.Type.Utf8:
                    return new Types.StringType();
                case Flatbuf.Type.Binary:
                    return Types.BinaryType.Default;
                default:
                    throw new InvalidDataException($"Arrow primitive '{field.TypeType}' is unsupported.");
            }
        }
    }
}