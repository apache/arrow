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
using System.Text;

namespace Apache.Arrow.Acero.Tests
{
    public static class TableExtensions
    {
        public static string PrettyPrint(this Table table, int columnPadding = 10)
        {
            var sb = new StringBuilder();

            for (int i = 0; i < table.ColumnCount; i++)
            {
                sb.Append(table.Column(i).Name.PadRight(columnPadding) + " | ");
            }

            sb.AppendLine();

            for (int i = 0; i < table.RowCount; i++)
            {
                for (int j = 0; j < table.ColumnCount; j++)
                {
                    Column sliced = table.Column(j).Slice(i, 1);

                    for (int k = 0; k < sliced.Data.ArrayCount; k++)
                    {
                        if (sliced.Data.Array(k).Length == 0)
                            continue;

                        Array data = sliced.Data.Array(k);
                        string value;

                        switch (data)
                        {
                            case StringArray stringArray:
                                value = stringArray.GetString(0);
                                break;

                            case Int32Array int32Array:
                                value = int32Array.GetValue(0).ToString();
                                break;

                            default:
                                throw new InvalidOperationException("Array type not supported");
                        }

                        sb.Append(value.PadRight(columnPadding) + " | ");
                    }
                }
                sb.AppendLine();
            }

            return sb.ToString().Trim('\n', '\r');
        }
    }
}
