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

using Apache.Arrow.Types;

namespace Apache.Arrow
{
    static class ArrayDataTypeComparer
    {
        internal static bool Compare(IArrowType expectedType, IArrowType actualType)
        {
            if (expectedType.TypeId != actualType.TypeId)
            {
                return false;
            }

            if (actualType.TypeId == ArrowTypeId.FixedSizedBinary)
            {
                var expectedFixedSizeBinaryType = (FixedSizeBinaryType)expectedType;
                var actualFixedSizeBinaryType = (FixedSizeBinaryType)actualType;

                if (expectedFixedSizeBinaryType.ByteWidth != actualFixedSizeBinaryType.ByteWidth)
                {
                    return false;
                }

                return true;
            }

            if (actualType.TypeId == ArrowTypeId.List)
            {
                var expectedListType = (ListType)expectedType;
                var actualListType = (ListType)actualType;

                CompareNested(expectedListType, actualListType);
            }

            if (actualType.TypeId == ArrowTypeId.Struct)
            {
                var expectedStructType = (StructType)expectedType;
                var actualStructType = (StructType)actualType;

                CompareNested(expectedStructType, actualStructType);
            }

            return true;
        }

        private static bool CompareNested(NestedType expectedType, NestedType actualType)
        {
            if (expectedType.Fields.Count != actualType.Fields.Count)
            {
                return false;
            }

            for (int i = 0; i < expectedType.Fields.Count; i++)
            {
                Compare(expectedType.Fields[i].DataType, actualType.Fields[i].DataType);
            }

            return true;
        }
    }
}
