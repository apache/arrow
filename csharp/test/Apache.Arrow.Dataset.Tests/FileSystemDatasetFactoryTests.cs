using System;
using System.Runtime.Versioning;
using System.Threading.Tasks;
using Apache.Arrow.C;
using Apache.Arrow.Dataset.GLibBindings;
using Apache.Arrow.GLibBindings;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Dataset.Tests;

public class FileSystemDatasetFactoryTests
{
    public FileSystemDatasetFactoryTests(ITestOutputHelper testOutputHelper)
    {
        Apache.Arrow.Dataset.GLibBindings.Module.Initialize();
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    [UnsupportedOSPlatform("OSX")]
    [UnsupportedOSPlatform("Windows")]
    public async Task CreateParquetFileSystemDataset()
    {
        var fileFormat = ParquetFileFormat.New();
        using var datasetFactory = FileSystemDatasetFactory.New(fileFormat);
        datasetFactory.SetFileSystem(LocalFileSystem.New(LocalFileSystemOptions.New()));
        datasetFactory.AddPath("/home/adam/dev/gross/parquet-issues/hive-partitioning/dataset");
        using var dataset = datasetFactory.Finish(null);

        Assert.NotNull(dataset);

        using var glibReader = dataset.ToRecordBatchReader();
        using var reader = ImportRecordBatchReader(glibReader);
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            _testOutputHelper.WriteLine(batch.ToString());
        }
    }

    [UnsupportedOSPlatform("OSX")]
    [UnsupportedOSPlatform("Windows")]
    private static unsafe IArrowArrayStream ImportRecordBatchReader(RecordBatchReader glibReader)
    {
        var arrayStreamPtr = glibReader.Export();
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

    private readonly ITestOutputHelper _testOutputHelper;
}
