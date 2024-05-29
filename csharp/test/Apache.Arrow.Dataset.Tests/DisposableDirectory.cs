using System;
using System.IO;

namespace Apache.Arrow.Dataset.Tests;

/// <summary>
/// Creates a temporary directory that is deleted on dispose
/// </summary>
internal sealed class DisposableDirectory : IDisposable
{
    public DisposableDirectory()
    {
        _directoryPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directoryPath);
    }

    public void Dispose()
    {
        Directory.Delete(_directoryPath, recursive: true);
    }

    public string DirectoryPath => _directoryPath;

    private readonly string _directoryPath;
}
