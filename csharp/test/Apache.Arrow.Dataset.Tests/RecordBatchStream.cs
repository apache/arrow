using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Dataset.Tests;

/// <summary>
/// An IArrowArrayStream that uses in-memory record batches
/// </summary>
public class RecordBatchStream : IArrowArrayStream
{
    public RecordBatchStream(Schema schema, IReadOnlyList<RecordBatch> batches)
    {
        Schema = schema;
        _batches = batches;
    }

    public Schema Schema { get; }

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        if (_index < 0)
        {
            throw new ObjectDisposedException(nameof(RecordBatchStream));
        }

        RecordBatch? result = _index < _batches.Count ? _batches[_index++] : null;
        return new ValueTask<RecordBatch?>(result);
    }

    public void Dispose()
    {
        _index = -1;
    }

    private readonly IReadOnlyList<RecordBatch> _batches;
    private int _index = 0;
}
