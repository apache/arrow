using System;

namespace Apache.Arrow.Dataset;

public abstract class Partitioning : IDisposable
{
    protected Partitioning(GLibBindings.Partitioning gLibPartitioning)
    {
        GObj = gLibPartitioning;
    }

    internal static Partitioning FromGObject(GLibBindings.Partitioning partitioning)
    {
        return partitioning switch
        {
            GLibBindings.HivePartitioning hivePartitioning => throw new NotImplementedException(),
            GLibBindings.DirectoryPartitioning directoryPartitioning =>
                new DirectoryPartitioning(directoryPartitioning),
            GLibBindings.KeyValuePartitioning keyValuePartitioning => throw new NotImplementedException(),
            _ => throw new ArgumentOutOfRangeException(nameof(partitioning))
        };
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            GObj.Dispose();
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    internal readonly GLibBindings.Partitioning GObj;
}
