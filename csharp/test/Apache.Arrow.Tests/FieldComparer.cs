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

namespace Apache.Arrow.Tests
{
    public class FieldComparer
    {
        public static bool Equals(Field f1, Field f2)
        {
            if (ReferenceEquals(f1, f2))
            {
                return true;
            }
            if (f2 != null && f1 != null && f1.Name == f2.Name && f1.IsNullable == f2.IsNullable &&
                f1.DataType.TypeId == f2.DataType.TypeId && f1.HasMetadata == f2.HasMetadata)
            {
                if (f1.HasMetadata && f2.HasMetadata)
                {
                    return f1.Metadata.Keys.Count() == f2.Metadata.Keys.Count() &&
                           f1.Metadata.Keys.All(k => f2.Metadata.ContainsKey(k) && f1.Metadata[k] == f2.Metadata[k]) &&
                           f2.Metadata.Keys.All(k => f1.Metadata.ContainsKey(k) && f2.Metadata[k] == f1.Metadata[k]);
                }
                return true;
            }
            return false;
        }
    }
}
