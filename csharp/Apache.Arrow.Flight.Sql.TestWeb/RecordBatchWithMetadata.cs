using Google.Protobuf;

namespace Apache.Arrow.Flight.Sql.TestWeb;

public class RecordBatchWithMetadata
{
    public RecordBatch RecordBatch { get; }
    public ByteString Metadata { get; }

    public RecordBatchWithMetadata(RecordBatch recordBatch, ByteString metadata = null)
    {
        RecordBatch = recordBatch;
        Metadata = metadata;
    }
}
