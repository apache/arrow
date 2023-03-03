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
using Xunit;

namespace Apache.Arrow.Tests
{
    public static class SchemaComparer
    {
        public static void Compare(Schema expected, Schema actual)
        {
            if (ReferenceEquals(expected, actual))
            {
                return;
            }

            Assert.Equal(expected.HasMetadata, actual.HasMetadata);
            if (expected.HasMetadata)
            {
                Assert.Equal(expected.Metadata.Keys.Count(), actual.Metadata.Keys.Count());
                Assert.True(expected.Metadata.Keys.All(k => actual.Metadata.ContainsKey(k) && expected.Metadata[k] == actual.Metadata[k]));
                Assert.True(actual.Metadata.Keys.All(k => expected.Metadata.ContainsKey(k) && actual.Metadata[k] == expected.Metadata[k]));
            }

            Assert.Equal(expected.FieldsList.Count, actual.FieldsList.Count);
            Assert.True(expected.FieldsList.All(f1 => actual.FieldsList.Any(f2 => f2.Name == f1.Name)));
            foreach (string name in expected.FieldsList.Select(f => f.Name))
            {
                FieldComparer.Compare(expected.GetFieldByName(name), actual.GetFieldByName(name));
            }
        }
    }
}
