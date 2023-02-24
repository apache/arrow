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
    /// </remarks
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
            return formatter.format_string;
        }

        private static long GetFlags(IArrowType datatype, bool nullable = true)
        {
            long flags = 0;

            if (nullable)
            {
                flags |= ARROW_FLAG_NULLABLE;
            }

            if (datatype is DictionaryType)
            {
                if (((DictionaryType)datatype).Ordered)
                {
                    flags |= ARROW_FLAG_DICTIONARY_ORDERED;
                }
            }

            // TODO: when we implement MapType, make sure to set the KEYS_SORTED flag.
            return flags;
        }

        private static IntPtr* ConstructChildren(IArrowType datatype)
        {
            if (datatype is NestedType)
            {
                var fields = ((NestedType)datatype).Fields;
                int n_fields = fields.Count;

                IntPtr* pointer_list = (IntPtr*)Marshal.AllocHGlobal(n_fields * sizeof(IntPtr));

                for (var i = 0; i < n_fields; i++)
                {
                    var c_schema = new CArrowSchema();
                    CArrowSchema.ExportField(fields[i], out c_schema);
                    IntPtr exported_schema = c_schema.AllocateAsPtr();
                    pointer_list[i] = exported_schema;
                }

                return pointer_list;
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
                var c_schema = new CArrowSchema();
                var value_type = ((DictionaryType)datatype).ValueType;
                CArrowSchema.ExportDataType(value_type, out c_schema);
                return c_schema.AllocateAsPtr();
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
        /// <param name="out_schema">An uninitialized CArrowSchema</param>
        public static void ExportSchema(Schema schema, out CArrowSchema out_schema)
        {
            // TODO: top-level metadata
            var struct_type = new StructType(schema.Fields.Values.ToList());
            ExportDataType(struct_type, out out_schema);
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
        /// <param name="ptr"></param>
        /// <returns></returns>
        public IntPtr Export(IntPtr ptr)
        {
            Marshal.StructureToPtr<CArrowSchema>(this, ptr, false);
            return ptr;
        }

        public const int ARROW_FLAG_DICTIONARY_ORDERED = 1;
        public const int ARROW_FLAG_NULLABLE = 2;
        public const int ARROW_FLAG_MAP_KEYS_SORTED = 4;

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
            public string format_string;
            public void Visit(NullType _) => format_string = "n";
            public void Visit(BooleanType _) => format_string = "b";
            // Integers
            public void Visit(Int8Type _) => format_string = "c";
            public void Visit(UInt8Type _) => format_string = "C";
            public void Visit(Int16Type _) => format_string = "s";
            public void Visit(UInt16Type _) => format_string = "S";
            public void Visit(Int32Type _) => format_string = "i";
            public void Visit(UInt32Type _) => format_string = "I";
            public void Visit(Int64Type _) => format_string = "l";
            public void Visit(UInt64Type _) => format_string = "L";
            // Floats
            public void Visit(HalfFloatType _) => format_string = "e";
            public void Visit(FloatType _) => format_string = "f";
            public void Visit(DoubleType _) => format_string = "g";
            // Binary
            public void Visit(BinaryType _) => format_string = "z";
            public void Visit(StringType _) => format_string = "u";
            public void Visit(FixedSizeBinaryType datatype)
            {
                format_string = $"w:{datatype.ByteWidth}";
            }
            // Decimal
            public void Visit(Decimal128Type datatype)
            {
                format_string = $"d:{datatype.Precision},{datatype.Scale}";
            }
            public void Visit(Decimal256Type datatype)
            {
                format_string = $"w:{datatype.Precision},{datatype.Scale},256";
            }
            // Date
            public void Visit(Date32Type _) => format_string = "tdD";
            public void Visit(Date64Type _) => format_string = "tdm";

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
                format_string = String.Format("tt{0}", TimeUnitComponent(datatype.Unit));
            }
            public void Visit(Time64Type datatype)
            {
                format_string = String.Format("tt{0}", TimeUnitComponent(datatype.Unit));
            }
            // Timestamp type
            public void Visit(TimestampType datatype)
            {
                format_string = String.Format("ts{0}:{1}", TimeUnitComponent(datatype.Unit), datatype.Timezone);
            }
            // Nested
            public void Visit(ListType _) => format_string = "+l";
            public void Visit(StructType _) => format_string = "+s";
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

    /// <summary>
    /// A <see cref="CArrowSchema"/> imported from somewhere else.
    /// </summary>
    ///
    /// <example>
    /// Typically, when importing a schema we will allocate an uninitialized 
    /// <see cref="CArrowSchema"/>, pass the pointer to the foreign function,
    /// then construct this class with the initialized pointer.
    /// 
    /// <code>
    /// var c_schema = new CArrowSchema();
    /// IntPtr imported_ptr = c_schema.AllocateAsPtr();
    /// foreign_export_function(imported_ptr);
    /// var imported_type = new ImportedArrowSchema(imported_ptr);
    /// ArrowType arrow_type = imported_type.GetAsType();
    /// <code>
    /// </example>
    public sealed class ImportedArrowSchema : IDisposable
    {
        private CArrowSchema _data;
        private IntPtr _handle;
        private bool _is_root;

        public ImportedArrowSchema(IntPtr handle)
        {
            _data = Marshal.PtrToStructure<CArrowSchema>(handle);
            if (_data.release == null)
            {
                throw new Exception("Tried to import a schema that has already been released.");
            }
            _handle = handle;
            _is_root = true;
        }

        private ImportedArrowSchema(IntPtr handle, bool is_root) : this(handle)
        {
            _is_root = is_root;
        }

        public void Dispose()
        {
            // We only call release on a root-level schema, not child ones.
            if (_is_root)
            {
                _data.release(_handle);
            }
        }

        public ArrowType GetAsType()
        {
            if (_data.dictionary != IntPtr.Zero)
            {
                ArrowType indices_type = _data.format switch
                {
                    "c" => new Int8Type(),
                    "C" => new UInt8Type(),
                    "s" => new Int16Type(),
                    "S" => new UInt16Type(),
                    "i" => new Int32Type(),
                    "I" => new UInt32Type(),
                    "l" => new Int64Type(),
                    "L" => new UInt64Type(),
                    _ => throw new InvalidDataException($"Indices must be an integer, but got format string {_data.format}"),
                };

                var dictionary_schema = new ImportedArrowSchema(_data.dictionary, /*is_root*/ false);
                var dictionary_type = dictionary_schema.GetAsType();

                bool ordered = (_data.flags & CArrowSchema.ARROW_FLAG_NULLABLE) == CArrowSchema.ARROW_FLAG_NULLABLE;

                return new DictionaryType(indices_type, dictionary_type, ordered);
            }

            // Special handling for nested types
            if (_data.format == "+l")
            {
                if (_data.n_children != 1)
                {
                    throw new Exception("Expected list type to have exactly one child.");
                }
                ImportedArrowSchema child_schema;
                unsafe
                {
                    if (_data.children[0] == null)
                    {
                        throw new Exception("Expected list type child to be non-null.");
                    }
                    child_schema = new ImportedArrowSchema(_data.children[0]);
                }

                var child_field = child_schema.GetAsField();

                return new ListType(child_field);
            }
            else if (_data.format == "+s")
            {
                var child_schemas = new ImportedArrowSchema[_data.n_children];
                unsafe
                {
                    for (int i = 0; i < _data.n_children; i++)
                    {
                        if (_data.children[i] == null)
                        {
                            throw new Exception("Expected struct type child to be non-null.");
                        }
                        child_schemas[i] = new ImportedArrowSchema(_data.children[i]);
                    }

                }

                var child_fields = child_schemas.Select(schema => schema.GetAsField()).ToList();

                return new StructType(child_fields);
            }
            // TODO: Map type and large list type

            return _data.format switch
            {
                // Primitives
                "n" => new NullType(),
                "b" => new BooleanType(),
                "c" => new Int8Type(),
                "C" => new UInt8Type(),
                "s" => new Int16Type(),
                "S" => new UInt16Type(),
                "i" => new Int32Type(),
                "I" => new UInt32Type(),
                "l" => new Int64Type(),
                "L" => new UInt64Type(),
                "e" => new HalfFloatType(),
                "f" => new FloatType(),
                "g" => new DoubleType(),
                // Binary data
                "z" => new BinaryType(),
                //"Z" => new LargeBinaryType() // Not yet implemented
                "u" => new StringType(),
                //"U" => new LargeStringType(), // Not yet implemented
                // TODO: decimal
                // TODO: fixed-width binary
                // Date and time
                "tdD" => new Date32Type(),
                "tdm" => new Date64Type(),
                "tts" => new Time32Type(TimeUnit.Second),
                "ttm" => new Time32Type(TimeUnit.Millisecond),
                "ttu" => new Time64Type(TimeUnit.Microsecond),
                "ttn" => new Time64Type(TimeUnit.Nanosecond),
                // TODO: timestamp with timezone,
                // TODO: duration not yet implemented
                "tiM" => new IntervalType(IntervalUnit.YearMonth),
                "tiD" => new IntervalType(IntervalUnit.DayTime),
                //"tin" => new IntervalType(IntervalUnit.MonthDayNanosecond), // Not yet implemented
                _ => throw new NotSupportedException("Data type is not yet supported in import.")
            };
        }

        public Field GetAsField()
        {
            string field_name = string.IsNullOrEmpty(_data.name) ? "" : _data.name;

            bool nullable = (_data.flags & CArrowSchema.ARROW_FLAG_NULLABLE) == CArrowSchema.ARROW_FLAG_NULLABLE;

            return new Field(field_name, GetAsType(), nullable);
        }

        public Schema GetAsSchema()
        {
            ArrowType full_type = GetAsType();
            if (full_type is StructType)
            {
                StructType struct_type = (StructType)full_type;
                return new Schema(struct_type.Fields, default);
            }
            else
            {
                throw new Exception("Imported type is not a struct type, so it cannot be converted to a schema.");
            }
        }
    }
}
