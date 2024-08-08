using System;

namespace Apache.Arrow.Dataset;

/// <summary>
/// Base class for all file system implementations
/// </summary>
public abstract class FileSystem : IDisposable
{
    protected FileSystem(Apache.Arrow.GLibBindings.FileSystem gObj)
    {
        GObj = gObj;
    }

    internal static FileSystem FromGObject(Apache.Arrow.GLibBindings.FileSystem gObj)
    {
        return gObj switch
        {
            Arrow.GLibBindings.GCSFileSystem gcsFileSystem => throw new NotImplementedException(),
            Arrow.GLibBindings.HDFSFileSystem hdfsFileSystem => throw new NotImplementedException(),
            Arrow.GLibBindings.LocalFileSystem localFileSystem => new LocalFileSystem(localFileSystem),
            Arrow.GLibBindings.MockFileSystem mockFileSystem => throw new NotImplementedException(),
            Arrow.GLibBindings.S3FileSystem s3FileSystem => throw new NotImplementedException(),
            Arrow.GLibBindings.SlowFileSystem slowFileSystem => throw new NotImplementedException(),
            Arrow.GLibBindings.SubTreeFileSystem subTreeFileSystem => throw new NotImplementedException(),
            _ => throw new ArgumentOutOfRangeException(nameof(gObj))
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

    internal readonly Apache.Arrow.GLibBindings.FileSystem GObj;
}
