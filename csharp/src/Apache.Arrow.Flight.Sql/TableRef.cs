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

namespace Apache.Arrow.Flight.Sql;

public class TableRef
{
    public string? Catalog { get; }
    public string DbSchema { get; }
    public string Table { get; }

    public TableRef(string dbSchema, string table)
    {
        DbSchema = dbSchema ?? throw new ArgumentNullException(nameof(dbSchema));
        Table = table ?? throw new ArgumentNullException(nameof(table));
    }

    public TableRef(string? catalog, string dbSchema, string table)
    {
        Catalog = catalog;
        DbSchema = dbSchema ?? throw new ArgumentNullException(nameof(dbSchema));
        Table = table ?? throw new ArgumentNullException(nameof(table));
    }
}
