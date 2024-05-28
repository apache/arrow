using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Dataset.Tests;

public class FileSystemDatasetFactoryTests
{
    public FileSystemDatasetFactoryTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task CreateParquetFileSystemDataset()
    {
        using var fileFormat = new ParquetFileFormat();
        using var datasetFactory = new FileSystemDatasetFactory(fileFormat);
        using var dataset = datasetFactory
            .SetFileSystem(new LocalFileSystem(new LocalFileSystemOptions { UseMmap = true}))
            .AddPath("/home/adam/dev/gross/parquet-issues/hive-partitioning/dataset")
            .Finish();

        Assert.NotNull(dataset);

        using var reader = dataset.ToRecordBatchReader();
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            _testOutputHelper.WriteLine(batch.ToString());
        }
    }

    private readonly ITestOutputHelper _testOutputHelper;
}
