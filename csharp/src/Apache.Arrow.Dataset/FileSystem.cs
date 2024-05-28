using System;

namespace Apache.Arrow.Dataset;

/// <summary>
/// Base class for all file system implementations
/// </summary>
public class FileSystem : IDisposable
{
    protected FileSystem(Apache.Arrow.GLibBindings.FileSystem gObj)
    {
        GObj = gObj;
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

    internal readonly Apache.Arrow.GLibBindings.FileSystem GObj;
}
