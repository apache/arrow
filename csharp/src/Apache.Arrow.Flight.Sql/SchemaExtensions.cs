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
    public static Schema DeserializeSchema(byte[] serializedSchema)
    {
        if (serializedSchema == null || serializedSchema.Length == 0)
        {
            throw new ArgumentException("Invalid serialized schema");
        }

        using var stream = new MemoryStream(serializedSchema);
        var reader = new ArrowStreamReader(stream);
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