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
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.C
{
    public static class CArrowArrayImporter
    {
        /// <summary>
        /// Import C pointer as an <see cref="IArrowArray"/>.
        /// </summary>
        /// <remarks>
        /// This will call the release callback once all of the buffers in the returned
        /// IArrowArray are disposed.
        /// </remarks>
        /// <examples>
        /// Typically, you will allocate an uninitialized CArrowArray pointer,
        /// pass that to external function, and then use this method to import
        /// the result.
        /// 
        /// <code>
        /// CArrowArray* importedPtr = CArrowArray.Create();
        /// foreign_export_function(importedPtr);
        /// IArrowArray importedArray = CArrowArrayImporter.ImportArray(importedPtr);
        /// </code>
        /// </examples>
        /// <param name="ptr">The pointer to the array being imported</param>
        /// <param name="type">The type of the array being imported</param>
        /// <returns>The imported C# array</returns>
        public static unsafe IArrowArray ImportArray(CArrowArray* ptr, IArrowType type)
        {
            ImportedArrowArray importedArray = null;
            try
            {
                importedArray = new ImportedArrowArray(ptr);
                return importedArray.GetAsArray(type);
            }
            finally
            {
                importedArray?.Release();
            }
        }

        /// <summary>
        /// Import C pointer as a <see cref="RecordBatch"/>.
        /// </summary>
        /// <remarks>
        /// This will call the release callback once all of the buffers in the returned
        /// RecordBatch are disposed.
        /// </remarks>
        /// <examples>
        /// Typically, you will allocate an uninitialized CArrowArray pointer,
        /// pass that to external function, and then use this method to import
        /// the result.
        /// 
        /// <code>
        /// CArrowArray* importedPtr = CArrowArray.Create();
        /// foreign_export_function(importedPtr);
        /// RecordBatch batch = CArrowArrayImporter.ImportRecordBatch(importedPtr, schema);
        /// </code>
        /// </examples>
        /// <param name="ptr">The pointer to the record batch being imported</param>
        /// <param name="schema">The schema type of the record batch being imported</param>
        /// <returns>The imported C# record batch</returns>
        public static unsafe RecordBatch ImportRecordBatch(CArrowArray* ptr, Schema schema)
        {
            ImportedArrowArray importedArray = null;
            try
            {
                importedArray = new ImportedArrowArray(ptr);
                return importedArray.GetAsRecordBatch(schema);
            }
            finally
            {
                importedArray?.Release();
            }
        }

        private sealed unsafe class ImportedArrowArray : ImportedAllocationOwner
        {
            private readonly CArrowArray _cArray;

            public ImportedArrowArray(CArrowArray* cArray)
            {
                if (cArray == null)
                {
                    throw new ArgumentNullException(nameof(cArray));
                }
                if (cArray->release == default)
                {
                    throw new ArgumentException("Tried to import an array that has already been released.", nameof(cArray));
                }
                _cArray = *cArray;
                cArray->release = default;
            }

            protected override void FinalRelease()
            {
                if (_cArray.release != default)
                {
                    fixed (CArrowArray* cArray = &_cArray)
                    {
#if NET5_0_OR_GREATER
                        cArray->release(cArray);
#else
                        Marshal.GetDelegateForFunctionPointer<CArrowArrayExporter.ReleaseArrowArray>(cArray->release)(cArray);
#endif
                    }
                }
            }

            public IArrowArray GetAsArray(IArrowType type)
            {
                fixed (CArrowArray* cArray = &_cArray)
                {
                    return ArrowArrayFactory.BuildArray(GetAsArrayData(cArray, type));
                }
            }

            public RecordBatch GetAsRecordBatch(Schema schema)
            {
                IArrowArray[] arrays = new IArrowArray[schema.FieldsList.Count];
                for (int i = 0; i < _cArray.n_children; i++)
                {
                    arrays[i] = ArrowArrayFactory.BuildArray(GetAsArrayData(_cArray.children[i], schema.FieldsList[i].DataType));
                }
                return new RecordBatch(schema, arrays, checked((int)_cArray.length));
            }

            private ArrayData GetAsArrayData(CArrowArray* cArray, IArrowType type)
            {
                ArrayData[] children = null;
                ArrowBuffer[] buffers = null;
                ArrayData dictionary = null;
                switch (type.TypeId)
                {
                    case ArrowTypeId.String:
                    case ArrowTypeId.Binary:
                        buffers = ImportByteArrayBuffers(cArray);
                        break;
                    case ArrowTypeId.StringView:
                    case ArrowTypeId.BinaryView:
                        buffers = ImportByteArrayViewBuffers(cArray);
                        break;
                    case ArrowTypeId.List:
                        children = ProcessListChildren(cArray, ((ListType)type).ValueDataType);
                        buffers = ImportListBuffers(cArray);
                        break;
                    case ArrowTypeId.ListView:
                        children = ProcessListChildren(cArray, ((ListViewType)type).ValueDataType);
                        buffers = ImportListViewBuffers(cArray);
                        break;
                    case ArrowTypeId.FixedSizeList:
                        children = ProcessListChildren(cArray, ((FixedSizeListType)type).ValueDataType);
                        buffers = ImportFixedSizeListBuffers(cArray);
                        break;
                    case ArrowTypeId.Struct:
                        children = ProcessStructChildren(cArray, ((StructType)type).Fields);
                        buffers = new ArrowBuffer[] { ImportValidityBuffer(cArray) };
                        break;
                    case ArrowTypeId.Union:
                        UnionType unionType = (UnionType)type;
                        children = ProcessStructChildren(cArray, unionType.Fields);
                        buffers = unionType.Mode switch
                        {
                            UnionMode.Dense => ImportDenseUnionBuffers(cArray),
                            UnionMode.Sparse => ImportSparseUnionBuffers(cArray),
                            _ => throw new InvalidOperationException("unknown union mode in import")
                        }; ;
                        break;
                    case ArrowTypeId.Map:
                        MapType mapType = (MapType)type;
                        children = ProcessListChildren(cArray, mapType.Fields[0].DataType);
                        buffers = ImportListBuffers(cArray);
                        break;
                    case ArrowTypeId.Null:
                        buffers = System.Array.Empty<ArrowBuffer>();
                        break;
                    case ArrowTypeId.Dictionary:
                        DictionaryType dictionaryType = (DictionaryType)type;
                        dictionary = GetAsArrayData(cArray->dictionary, dictionaryType.ValueType);
                        goto default; // Fall through to get the validity and index data
                    default:
                        if (type is FixedWidthType fixedWidthType)
                        {
                            buffers = ImportFixedWidthBuffers(cArray, fixedWidthType.BitWidth);
                        }
                        break;
                }

                if (buffers == null)
                {
                    throw new NotSupportedException("Data type is not yet supported in import.");
                }

                return new ArrayData(
                    type,
                    checked((int)cArray->length),
                    checked((int)cArray->null_count),
                    checked((int)cArray->offset),
                    buffers,
                    children,
                    dictionary);
            }

            private ArrayData[] ProcessListChildren(CArrowArray* cArray, IArrowType type)
            {
                if (cArray->n_children != 1)
                {
                    throw new InvalidOperationException("Lists are expected to have exactly one child array");
                }

                ArrayData[] children = new ArrayData[1];
                children[0] = GetAsArrayData(cArray->children[0], type);
                return children;
            }

            private ArrayData[] ProcessStructChildren(CArrowArray* cArray, IReadOnlyList<Field> fields)
            {
                if (cArray->n_children != fields.Count)
                {
                    throw new InvalidOperationException("Struct child count does not match schema");
                }

                ArrayData[] children = new ArrayData[fields.Count];
                for (int i = 0; i < fields.Count; i++)
                {
                    children[i] = GetAsArrayData(cArray->children[i], fields[i].DataType);
                }
                return children;
            }

            private ArrowBuffer ImportValidityBuffer(CArrowArray* cArray)
            {
                int length = checked((int)cArray->length);
                int validityLength = checked((int)BitUtility.RoundUpToMultipleOf8(length) / 8);
                return (cArray->buffers[0] == null) ? ArrowBuffer.Empty : new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[0], 0, validityLength));
            }

            private ArrowBuffer[] ImportByteArrayBuffers(CArrowArray* cArray)
            {
                if (cArray->n_buffers != 3)
                {
                    throw new InvalidOperationException("Byte arrays are expected to have exactly three buffers");
                }

                int length = checked((int)cArray->length);
                int offsetsLength = (length + 1) * 4;
                int* offsets = (int*)cArray->buffers[1];
                int valuesLength = offsets[length];

                ArrowBuffer[] buffers = new ArrowBuffer[3];
                buffers[0] = ImportValidityBuffer(cArray);
                buffers[1] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[1], 0, offsetsLength));
                buffers[2] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[2], 0, valuesLength));

                return buffers;
            }

            private ArrowBuffer[] ImportByteArrayViewBuffers(CArrowArray* cArray)
            {
                if (cArray->n_buffers < 3)
                {
                    throw new InvalidOperationException("Byte array views are expected to have at least three buffers");
                }

                int length = checked((int)cArray->length);
                int viewsLength = length * 16;

                long* bufferLengths = (long*)cArray->buffers[cArray->n_buffers - 1];
                ArrowBuffer[] buffers = new ArrowBuffer[cArray->n_buffers - 1];
                buffers[0] = ImportValidityBuffer(cArray);
                buffers[1] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[1], 0, viewsLength));
                for (int i = 2; i < buffers.Length; i++)
                {
                    buffers[i] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[i], 0, checked((int)bufferLengths[i - 2])));
                }

                return buffers;
            }

            private ArrowBuffer[] ImportListBuffers(CArrowArray* cArray)
            {
                if (cArray->n_buffers != 2)
                {
                    throw new InvalidOperationException("List arrays are expected to have exactly two buffers");
                }

                int length = checked((int)cArray->length);
                int offsetsLength = (length + 1) * 4;

                ArrowBuffer[] buffers = new ArrowBuffer[2];
                buffers[0] = ImportValidityBuffer(cArray);
                buffers[1] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[1], 0, offsetsLength));

                return buffers;
            }

            private ArrowBuffer[] ImportListViewBuffers(CArrowArray* cArray)
            {
                if (cArray->n_buffers != 3)
                {
                    throw new InvalidOperationException("List view arrays are expected to have exactly three buffers");
                }

                int length = checked((int)cArray->length);
                int offsetsLength = length * 4;

                ArrowBuffer[] buffers = new ArrowBuffer[3];
                buffers[0] = ImportValidityBuffer(cArray);
                buffers[1] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[1], 0, offsetsLength));
                buffers[2] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[2], 0, offsetsLength));

                return buffers;
            }

            private ArrowBuffer[] ImportFixedSizeListBuffers(CArrowArray* cArray)
            {
                if (cArray->n_buffers != 1)
                {
                    throw new InvalidOperationException("Fixed-size list arrays are expected to have exactly one buffer");
                }

                ArrowBuffer[] buffers = new ArrowBuffer[1];
                buffers[0] = ImportValidityBuffer(cArray);

                return buffers;
            }

            private ArrowBuffer[] ImportDenseUnionBuffers(CArrowArray* cArray)
            {
                if (cArray->n_buffers != 2)
                {
                    throw new InvalidOperationException("Dense union arrays are expected to have exactly two children");
                }
                int length = checked((int)cArray->length);
                int offsetsLength = length * 4;

                ArrowBuffer[] buffers = new ArrowBuffer[2];
                buffers[0] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[0], 0, length));
                buffers[1] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[1], 0, offsetsLength));

                return buffers;
            }

            private ArrowBuffer[] ImportSparseUnionBuffers(CArrowArray* cArray)
            {
                if (cArray->n_buffers != 1)
                {
                    throw new InvalidOperationException("Sparse union arrays are expected to have exactly one child");
                }

                ArrowBuffer[] buffers = new ArrowBuffer[1];
                buffers[0] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[0], 0, checked((int)cArray->length)));

                return buffers;
            }

            private ArrowBuffer[] ImportFixedWidthBuffers(CArrowArray* cArray, int bitWidth)
            {
                if (cArray->n_buffers != 2)
                {
                    throw new InvalidOperationException("Arrays of fixed-width type are expected to have exactly two buffers");
                }

                // validity, data
                int length = checked((int)cArray->length);
                int valuesLength;
                if (bitWidth >= 8)
                    valuesLength = checked((int)(cArray->length * bitWidth / 8));
                else
                    valuesLength = checked((int)BitUtility.RoundUpToMultipleOf8(length) / 8);

                ArrowBuffer[] buffers = new ArrowBuffer[2];
                buffers[0] = ImportValidityBuffer(cArray);
                buffers[1] = new ArrowBuffer(AddMemory((IntPtr)cArray->buffers[1], 0, valuesLength));

                return buffers;
            }
        }
    }
}
