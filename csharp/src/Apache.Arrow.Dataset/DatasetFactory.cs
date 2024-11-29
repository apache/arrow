using System;

namespace Apache.Arrow.Dataset;

/// <summary>
/// Base class for all Dataset factories
/// </summary>
public abstract class DatasetFactory : IDisposable
{
    protected DatasetFactory(GLibBindings.DatasetFactory gObj)
    {
        GObj = gObj;
    }

    public abstract DatasetBase Finish();

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

    internal readonly GLibBindings.DatasetFactory GObj;
}
