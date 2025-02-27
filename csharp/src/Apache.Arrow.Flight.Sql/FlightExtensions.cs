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
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Apache.Arrow.Flight.Sql;

internal static class FlightExtensions
{
    public static byte[] PackAndSerialize(this IMessage command) => Any.Pack(command).ToByteArray();

    public static T ParseAndUnpack<T>(this ByteString source) where T : IMessage<T>, new() =>
        Any.Parser.ParseFrom(source).Unpack<T>();

    public static int ExtractRowCount(this RecordBatch batch)
    {
        if (batch.ColumnCount == 0) return 0;
        int length = batch.Column(0).Length;
        foreach (var column in batch.Arrays)
        {
            if (column.Length != length)
                throw new InvalidOperationException("Inconsistent column lengths in RecordBatch.");
        }

        return length;
    }
}