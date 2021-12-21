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

using System.Linq;

namespace Apache.Arrow
{
    internal static class FieldComparer
    {
        public static bool Compare(Field expected, Field actual)
        {
            if (ReferenceEquals(expected, actual))
            {
                return true;
            }

            if (expected.Name != actual.Name || expected.IsNullable != actual.IsNullable ||
                expected.HasMetadata != actual.HasMetadata)
            {
                return false;
            }

            if (expected.HasMetadata)
            {
                if (expected.Metadata.Count != actual.Metadata.Count)
                {
                    return false;
                }

                if (!expected.Metadata.Keys.All(k => actual.Metadata.ContainsKey(k) && expected.Metadata[k] == actual.Metadata[k]))
                {
                    return false;
                }
            }

            var dataTypeComparer = new ArrayDataTypeComparer(expected.DataType);

            actual.DataType.Accept(dataTypeComparer);

            if (!dataTypeComparer.DataTypeMatch)
            {
                return false;
            }

            return true;
        }
    }
}
