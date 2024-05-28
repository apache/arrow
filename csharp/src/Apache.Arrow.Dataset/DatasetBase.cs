using System;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Dataset;

/// <summary>
/// Base class for all Dataset implementations
/// </summary>
public class DatasetBase : IDisposable
{
    protected DatasetBase(GLibBindings.Dataset gLibDataset)
    {
        GObj = gLibDataset;
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            GObj.Dispose();
        }
    }

    public unsafe IArrowArrayStream ToRecordBatchReader()
    {
        using var gLibReader = GObj.ToRecordBatchReader();
        var arrayStreamPtr = gLibReader!.Export();
        try
        {
            return CArrowArrayStreamImporter.ImportArrayStream((CArrowArrayStream*)arrayStreamPtr);
        }
        finally
        {
            if (arrayStreamPtr != IntPtr.Zero)
            {
                GLib.Functions.Free(arrayStreamPtr);
            }
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    internal readonly GLibBindings.Dataset GObj;
}
