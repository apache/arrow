using System;

namespace Apache.Arrow.Dataset;

public sealed class FileSystemDatasetWriteOptions : IDisposable
{
    public string? BaseDir
    {
        get => GObj.BaseDir;
        set => GObj.BaseDir = value;
    }

    public string? BaseNameTemplate
    {
        get => GObj.BaseNameTemplate;
        set => GObj.BaseNameTemplate = value;
    }

    public FileWriteOptions? FileWriteOptions
    {
        get
        {
            var gObjectFileWriteOptions = GObj.FileWriteOptions;
            return gObjectFileWriteOptions == null ? null : new FileWriteOptions(gObjectFileWriteOptions);
        }
        set => GObj.FileWriteOptions = value?.GObj;
    }

    public FileSystem? FileSystem
    {
        get
        {
            var gObjectFileSystem = GObj.FileSystem;
            return gObjectFileSystem == null ? null : FileSystem.FromGObject(gObjectFileSystem);
        }
        set => GObj.FileSystem = value?.GObj;
    }

    public Partitioning? Partitioning
    {
        get
        {
            var gObjectPartitioning = GObj.Partitioning;
            if (gObjectPartitioning == null)
            {
                return null;
            }

            return Partitioning.FromGObject(gObjectPartitioning);
        }
        set => GObj.Partitioning = value?.GObj;
    }

    public uint MaxPartitions
    {
        get => GObj.MaxPartitions;
        set => GObj.MaxPartitions = value;
    }

    public void Dispose()
    {
        GObj.Dispose();
    }

    internal readonly GLibBindings.FileSystemDatasetWriteOptions GObj = GLibBindings.FileSystemDatasetWriteOptions.New();
}
