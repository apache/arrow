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

using Apache.Arrow;
using Apache.Arrow.Types;
using System;
using System.Collections.Generic;
using Xunit;

namespace Apache.Arrow.Tests;

public class SchemaTests
{
    [Fact]
    public void ThrowsWhenFieldsAreNull()
    {
        Assert.Throws<ArgumentNullException>(() => new Schema(null, null));
    }

    [Theory]
    [MemberData(nameof(StringComparers))]
    public void CanRetrieveFieldIndexByName(StringComparer comparer)
    {
        var field0 = new Field("f0", Int32Type.Default, true);
        var field1 = new Field("f1", Int64Type.Default, true);
        var schema = new Schema([field0, field1], null);

        Assert.Equal(0, schema.GetFieldIndex("f0", comparer));
        Assert.Equal(1, schema.GetFieldIndex("f1", comparer));
        Assert.Throws<InvalidOperationException>(() => schema.GetFieldIndex("nonexistent", comparer));
    }

    [Theory]
    [MemberData(nameof(StringComparers))]
    public void CanRetrieveFieldIndexByNonUniqueName(StringComparer comparer)
    {
        var field0 = new Field("f0", Int32Type.Default, true);
        var field1 = new Field("f1", Int64Type.Default, true);

        // Repeat fields in the list
        var schema = new Schema([field0, field1, field0, field1], null);

        Assert.Equal(0, schema.GetFieldIndex("f0", comparer));
        Assert.Equal(1, schema.GetFieldIndex("f1", comparer));
        Assert.Throws<InvalidOperationException>(() => schema.GetFieldIndex("nonexistent", comparer));
    }

    public static IEnumerable<object[]> StringComparers() =>
        new List<object[]>
        {
            new object[] {null},
            new object[] {StringComparer.Ordinal},
            new object[] {StringComparer.OrdinalIgnoreCase},
            new object[] {StringComparer.CurrentCulture},
        };
}
