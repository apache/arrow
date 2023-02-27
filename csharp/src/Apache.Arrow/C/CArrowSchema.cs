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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Apache.Arrow.Types;

[UnmanagedFunctionPointer(CallingConvention.StdCall)]
public delegate void ReleaseCArrowSchema(IntPtr schema);

namespace Apache.Arrow.C
{
    /// <summary>
    /// An Arrow C Data Interface Schema, which represents a type, field, or schema.
    /// </summary>
    /// 
    /// <remarks>
    /// This is used to export <see cref="ArrowType"/>, <see cref="Field"/>, or
    /// <see cref="Schema"/> to other languages. It matches the layout of the
    /// ArrowSchema struct described in https://github.com/apache/arrow/blob/main/cpp/src/arrow/c/abi.h.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    unsafe public struct CArrowSchema
    {
        [MarshalAs(UnmanagedType.LPStr)]
        public string format;
        [MarshalAs(UnmanagedType.LPStr)]
        public string name;
        [MarshalAs(UnmanagedType.LPStr)]
        public string metadata;
        public long flags;
        public long n_children;
        public IntPtr* children;
        public IntPtr dictionary;
        [MarshalAs(UnmanagedType.FunctionPtr)]

        public ReleaseCArrowSchema release;
        // Check this out: https://github.com/G-Research/ParquetSharp/blob/386d91bd5e6fe6cb81583803447023c1359957c8/csharp/ParquetHandle.cs#L8
        public IntPtr private_data;

        private static string GetFormat(IArrowType datatype)
        {
            TypeFormatter formatter = new TypeFormatter();
            datatype.Accept(formatter);
            return formatter.formatString;
        }

        private static long GetFlags(IArrowType datatype, bool nullable = true)
        {
            long flags = 0;

            if (nullable)
            {
                flags |= ArrowFlagNullable;
            }

            if (datatype is DictionaryType)
            {
                if (((DictionaryType)datatype).Ordered)
                {
                    flags |= ArrowFlagDictionaryOrdered;
                }
            }

            // TODO: when we implement MapType, make sure to set the KEYS_SORTED flag.
            return flags;
        }

        private static IntPtr* ConstructChildren(IArrowType datatype)
        {
            if (datatype is NestedType)
            {
                IReadOnlyList<Field> fields = ((NestedType)datatype).Fields;
                int numFields = fields.Count;

                IntPtr* pointerList = (IntPtr*)Marshal.AllocHGlobal(numFields * sizeof(IntPtr));

                for (var i = 0; i < numFields; i++)
                {
                    var cSchema = new CArrowSchema();
                    CArrowSchema.ExportField(fields[i], out cSchema);
                    IntPtr exportedSchema = cSchema.AllocateAsPtr();
                    pointerList[i] = exportedSchema;
                }

                return pointerList;
            }
            else
            {
                return (IntPtr*)IntPtr.Zero;
            }
        }

        private static IntPtr ConstructDictionary(IArrowType datatype)
        {
            if (datatype is DictionaryType)
            {
                var cSchema = new CArrowSchema();
                IArrowType valueType = ((DictionaryType)datatype).ValueType;
                CArrowSchema.ExportDataType(valueType, out cSchema);
                return cSchema.AllocateAsPtr();
            }
            else
            {
                return IntPtr.Zero;
            }
        }

        /// <summary>
        /// Initialize the exported C schema as an Arrow type.
        /// </summary>
        /// <param name="datatype">The Arrow type to export.</param>
        /// <param name="schema">An uninitialized CArrowSchema.</param>
        public static void ExportDataType(IArrowType datatype, out CArrowSchema schema)
        {
            schema.format = GetFormat(datatype);
            schema.name = null;
            schema.metadata = null;
            schema.flags = GetFlags(datatype);

            schema.children = ConstructChildren(datatype);
            schema.n_children = datatype is NestedType ? ((NestedType)datatype).Fields.Count : 0;

            schema.dictionary = ConstructDictionary(datatype);

            schema.release = (IntPtr self) =>
            {
                var schema = Marshal.PtrToStructure<CArrowSchema>(self);
                if (schema.n_children > 0)
                {
                    for (int i = 0; i < schema.n_children; i++)
                    {
                        FreePtr(schema.children[i]);
                    }
                    Marshal.FreeHGlobal((IntPtr)schema.children);
                }

                if (schema.dictionary != IntPtr.Zero)
                {
                    FreePtr(schema.dictionary);
                }
                Marshal.DestroyStructure<CArrowSchema>(self);
            };

            schema.private_data = IntPtr.Zero;
        }

        /// <summary>
        /// Initialize the exported C schema as a field.
        /// </summary>
        /// <param name="field">Field to export.</param>
        /// <param name="schema">An uninitialized CArrowSchema.</param>
        public static void ExportField(Field field, out CArrowSchema schema)
        {
            ExportDataType(field.DataType, out schema);
            schema.name = field.Name;
            // TODO: field metadata
            schema.metadata = null;
            schema.flags = GetFlags(field.DataType, field.IsNullable);
        }

        /// <summary>
        /// Initialize the exported C schema as a schema.
        /// </summary>
        /// <param name="schema">Schema to export.</param>
        /// <param name="outSchema">An uninitialized CArrowSchema</param>
        public static void ExportSchema(Schema schema, out CArrowSchema outSchema)
        {
            // TODO: top-level metadata
            var structType = new StructType(schema.Fields.Values.ToList());
            ExportDataType(structType, out outSchema);
        }

        /// <summary>
        /// Allocate an unmanaged pointer and copy this instances data to it.
        /// </summary>
        /// <remarks>
        /// To avoid a memory leak, you must call <see cref="FreePtr"/> on this
        /// pointer when done using it.
        /// </remarks>
        public IntPtr AllocateAsPtr()
        {
            IntPtr ptr = Marshal.AllocHGlobal(Marshal.SizeOf(this));
            Marshal.StructureToPtr<CArrowSchema>(this, ptr, false);
            return ptr;
        }

        /// <summary>
        /// Free a pointer that was allocated in <see cref="AllocateAsPtr"/>.
        /// </summary>
        /// <remarks>
        /// Do not call this on a pointer that was allocated elsewhere.
        /// </remarks>
        public static void FreePtr(IntPtr ptr)
        {
            var schema = Marshal.PtrToStructure<CArrowSchema>(ptr);
            if (schema.release != null)
            {
                // Call release if not already called.
                schema.release(ptr);
            }
            Marshal.FreeHGlobal(ptr);
        }

        /// <summary>
        /// Export to an existing pointer
        /// </summary>
        /// <param name="ptr">An allocated but uninitialized pointer.</param>
        public IntPtr Export(IntPtr ptr)
        {
            Marshal.StructureToPtr<CArrowSchema>(this, ptr, false);
            return ptr;
        }

        public const int ArrowFlagDictionaryOrdered = 1;
        public const int ArrowFlagNullable = 2;
        public const int ArrowFlagMapKeysSorted = 4;

        private class TypeFormatter :
        IArrowTypeVisitor<NullType>,
        IArrowTypeVisitor<BooleanType>,
        IArrowTypeVisitor<Int8Type>,
        IArrowTypeVisitor<Int16Type>,
        IArrowTypeVisitor<Int32Type>,
        IArrowTypeVisitor<Int64Type>,
        IArrowTypeVisitor<UInt8Type>,
        IArrowTypeVisitor<UInt16Type>,
        IArrowTypeVisitor<UInt32Type>,
        IArrowTypeVisitor<UInt64Type>,
        IArrowTypeVisitor<HalfFloatType>,
        IArrowTypeVisitor<FloatType>,
        IArrowTypeVisitor<DoubleType>,
        IArrowTypeVisitor<Decimal128Type>,
        IArrowTypeVisitor<Decimal256Type>,
        IArrowTypeVisitor<Date32Type>,
        IArrowTypeVisitor<Date64Type>,
        IArrowTypeVisitor<Time32Type>,
        IArrowTypeVisitor<Time64Type>,
        IArrowTypeVisitor<TimestampType>,
        IArrowTypeVisitor<StringType>,
        IArrowTypeVisitor<BinaryType>,
        IArrowTypeVisitor<FixedSizeBinaryType>,
        IArrowTypeVisitor<ListType>,
        IArrowTypeVisitor<StructType>,
        IArrowTypeVisitor<DictionaryType>
        {
            public string formatString;
            public void Visit(NullType _) => formatString = "n";
            public void Visit(BooleanType _) => formatString = "b";
            // Integers
            public void Visit(Int8Type _) => formatString = "c";
            public void Visit(UInt8Type _) => formatString = "C";
            public void Visit(Int16Type _) => formatString = "s";
            public void Visit(UInt16Type _) => formatString = "S";
            public void Visit(Int32Type _) => formatString = "i";
            public void Visit(UInt32Type _) => formatString = "I";
            public void Visit(Int64Type _) => formatString = "l";
            public void Visit(UInt64Type _) => formatString = "L";
            // Floats
            public void Visit(HalfFloatType _) => formatString = "e";
            public void Visit(FloatType _) => formatString = "f";
            public void Visit(DoubleType _) => formatString = "g";
            // Binary
            public void Visit(BinaryType _) => formatString = "z";
            public void Visit(StringType _) => formatString = "u";
            public void Visit(FixedSizeBinaryType datatype)
            {
                formatString = $"w:{datatype.ByteWidth}";
            }
            // Decimal
            public void Visit(Decimal128Type datatype)
            {
                formatString = $"d:{datatype.Precision},{datatype.Scale}";
            }
            public void Visit(Decimal256Type datatype)
            {
                formatString = $"w:{datatype.Precision},{datatype.Scale},256";
            }
            // Date
            public void Visit(Date32Type _) => formatString = "tdD";
            public void Visit(Date64Type _) => formatString = "tdm";

            private char TimeUnitComponent(TimeUnit unit) => unit switch
            {
                TimeUnit.Second => 's',
                TimeUnit.Millisecond => 'm',
                TimeUnit.Microsecond => 'u',
                TimeUnit.Nanosecond => 'n',
                _ => throw new InvalidDataException($"Unsupported time unit for export: {unit}"),
            };
            // Time
            public void Visit(Time32Type datatype)
            {
                formatString = String.Format("tt{0}", TimeUnitComponent(datatype.Unit));
            }
            public void Visit(Time64Type datatype)
            {
                formatString = String.Format("tt{0}", TimeUnitComponent(datatype.Unit));
            }
            // Timestamp type
            public void Visit(TimestampType datatype)
            {
                formatString = String.Format("ts{0}:{1}", TimeUnitComponent(datatype.Unit), datatype.Timezone);
            }
            // Nested
            public void Visit(ListType _) => formatString = "+l";
            public void Visit(StructType _) => formatString = "+s";
            // Dictionary
            public void Visit(DictionaryType datatype)
            {
                // format string is that of the indices
                datatype.IndexType.Accept(this);
            }

            // Unsupported
            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"Exporting {type.Name} not implemented");
            }
        }
    }
}
