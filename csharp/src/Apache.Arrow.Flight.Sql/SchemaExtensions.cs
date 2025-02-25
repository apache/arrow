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
using System.IO;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Flight.Sql;

public static class SchemaExtensions
{
    /// <summary>
    /// Deserializes a schema from a byte array.
    /// </summary>
    /// <param name="serializedSchema">The byte array representing the serialized schema.</param>
    /// <returns>The deserialized Schema object.</returns>
    public static Schema DeserializeSchema(ReadOnlyMemory<byte> serializedSchema)
    {
        if (serializedSchema.IsEmpty)
        {
            throw new ArgumentException("Invalid serialized schema", nameof(serializedSchema));
        }
        using var reader = new ArrowStreamReader(serializedSchema);
        return reader.Schema;
    }
    
    /// <summary>
    /// Serializes the provided schema to a byte array.
    /// </summary>
    public static byte[] SerializeSchema(Schema schema)
    {
        using var memoryStream = new MemoryStream();
        using var writer = new ArrowStreamWriter(memoryStream, schema);
        writer.WriteStart(); 
        writer.WriteEnd();
        return memoryStream.ToArray();
    }
}