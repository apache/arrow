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
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Apache.Arrow.Types;

namespace Apache.Arrow.C
{
    public sealed class CArrowSchemaExporter
    {
        public static unsafe void ExportType(IArrowType datatype, CArrowSchema* schema)
        {
            if (schema == null)
            {
                throw new Exception("Cannot export schema to a null pointer.");
            }
            if (schema->release != null)
            {
                throw new Exception("Cannot export schema to a struct that is already initialized.");
            }

            schema->format = StringUtil.ToCStringUtf8(GetFormat(datatype));
            schema->name = null;
            schema->metadata = null;
            schema->flags = GetFlags(datatype);

            schema->children = ConstructChildren(datatype, out var numChildren);
            schema->n_children = numChildren;

            schema->dictionary = ConstructDictionary(datatype);

            schema->release = (delegate* unmanaged[Stdcall]<CArrowSchema*, void>)Marshal.GetFunctionPointerForDelegate(
                ReleaseCArrowSchema);

            schema->private_data = null;
        }

        public static unsafe void ExportField(Field field, CArrowSchema* schema)
        {
            ExportType(field.DataType, schema);
            schema->name = StringUtil.ToCStringUtf8(field.Name);
            // TODO: field metadata
            schema->metadata = null;
            schema->flags = GetFlags(field.DataType, field.IsNullable);
        }

        public static unsafe void ExportSchema(Schema schema, CArrowSchema* out_schema)
        {
            var structType = new StructType(schema.Fields.Values.ToList());
            // TODO: top-level metadata
            ExportType(structType, out_schema);
        }



        private static char FormatTimeUnit(TimeUnit unit) => unit switch
        {
            TimeUnit.Second => 's',
            TimeUnit.Millisecond => 'm',
            TimeUnit.Microsecond => 'u',
            TimeUnit.Nanosecond => 'n',
            _ => throw new InvalidDataException($"Unsupported time unit for export: {unit}"),
        };

        private static string GetFormat(IArrowType datatype)
        {
            switch (datatype)
            {
                case NullType _: return "n";
                case BooleanType _: return "b";
                // Integers
                case Int8Type _: return "c";
                case UInt8Type _: return "C";
                case Int16Type _: return "s";
                case UInt16Type _: return "S";
                case Int32Type _: return "i";
                case UInt32Type _: return "I";
                case Int64Type _: return "l";
                case UInt64Type _: return "L";
                // Floats
                case HalfFloatType _: return "e";
                case FloatType _: return "f";
                case DoubleType _: return "g";
                // Decimal
                case Decimal128Type decimalType:
                    return $"d:{decimalType.Precision},{decimalType.Scale}";
                case Decimal256Type decimalType:
                    return $"d:{decimalType.Precision},{decimalType.Scale},256";
                // Binary
                case BinaryType _: return "z";
                case StringType _: return "u";
                case FixedSizeBinaryType binaryType:
                    return $"w:{binaryType.ByteWidth}";
                // Date
                case Date32Type _: return "tdD";
                case Date64Type _: return "tdm";
                // Time
                case Time32Type timeType:
                    return String.Format("tt{0}", FormatTimeUnit(timeType.Unit));
                case Time64Type timeType:
                    // Same prefix as Time32, but allowed time units are different.
                    return String.Format("tt{0}", FormatTimeUnit(timeType.Unit));
                // Timestamp
                case TimestampType timestampType:
                    return String.Format("ts{0}:{1}", FormatTimeUnit(timestampType.Unit), timestampType.Timezone);
                // Nested
                case ListType _: return "+l";
                case StructType _: return "+s";
                // Dictionary
                case DictionaryType dictionaryType:
                    return GetFormat(dictionaryType.IndexType);
                default: throw new NotImplementedException($"Exporting {datatype.Name} not implemented");
            };
        }

        private static long GetFlags(IArrowType datatype, bool nullable = true)
        {
            long flags = 0;

            if (nullable)
            {
                flags |= CArrowSchema.ArrowFlagNullable;
            }

            if (datatype is DictionaryType dictionaryType)
            {
                if (dictionaryType.Ordered)
                {
                    flags |= CArrowSchema.ArrowFlagDictionaryOrdered;
                }
            }

            // TODO: when we implement MapType, make sure to set the KEYS_SORTED flag.
            return flags;
        }

        private static unsafe CArrowSchema** ConstructChildren(IArrowType datatype, out long numChildren)
        {
            if (datatype is NestedType nestedType)
            {
                IReadOnlyList<Field> fields = nestedType.Fields;
                int numFields = fields.Count;
                numChildren = numFields;

                unsafe
                {
                    var pointerList = (CArrowSchema**)Marshal.AllocHGlobal(numFields * IntPtr.Size);

                    for (var i = 0; i < numChildren; i++)
                    {
                        CArrowSchema* cSchema = CArrowSchema.New();
                        ExportField(fields[i], cSchema);
                        pointerList[i] = cSchema;
                    }

                    return pointerList;
                }

            }
            else
            {
                numChildren = 0;
                return null;
            }
        }

        private static unsafe CArrowSchema* ConstructDictionary(IArrowType datatype)
        {
            if (datatype is DictionaryType dictType)
            {
                CArrowSchema* cSchema = CArrowSchema.New();
                ExportType(dictType.ValueType, cSchema);
                return cSchema;
            }
            else
            {
                return null;
            }
        }

        private static unsafe void ReleaseCArrowSchema(CArrowSchema* schema)
        {
            if (schema == null) return;
            if (schema->release == null) return;

            Marshal.FreeHGlobal((IntPtr)schema->format);
            Marshal.FreeHGlobal((IntPtr)schema->name);
            Marshal.FreeHGlobal((IntPtr)schema->metadata);
            schema->format = null;
            schema->name = null;
            schema->metadata = null;

            if (schema->n_children > 0)
            {
                for (int i = 0; i < schema->n_children; i++)
                {
                    CArrowSchema.Free(schema->GetChild(i));
                }
                Marshal.FreeHGlobal((IntPtr)schema->children);
            }

            if (schema->dictionary != null)
            {
                CArrowSchema.Free(schema->dictionary);
            }

            schema->flags = 0;
            schema->n_children = 0;
            schema->dictionary = null;
            schema->children = null;
            schema->release = null;
        }
    }
}