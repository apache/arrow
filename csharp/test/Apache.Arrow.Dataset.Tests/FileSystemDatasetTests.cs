using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Dataset.Tests;

public class FileSystemDatasetTests
{
    [Fact]
    public async Task RoundTripParquetFileSystemDataset()
    {
        const int rowsPerBatch = 100;
        const int numBatches = 10;
        var recordBatchStream = GetTestData(rowsPerBatch, numBatches);

        using var fileSystem = new LocalFileSystem(new LocalFileSystemOptions { UseMmap = true});
        using var fileFormat = new ParquetFileFormat();
        using var dataDirectory = new DisposableDirectory();

        var partitioningSchema = new Schema.Builder()
            .Field(new Field("label", new StringType(), false))
            .Build();
        var partitioning = new DirectoryPartitioning(partitioningSchema);

        WriteDataset(recordBatchStream, fileSystem, dataDirectory.DirectoryPath, fileFormat, partitioning);

        using var datasetFactory = new FileSystemDatasetFactory(fileFormat);
        using var dataset = datasetFactory
            .SetFileSystem(fileSystem)
            .SetPartitioning(partitioning)
            .AddPath(dataDirectory.DirectoryPath)
            .Finish();

        using var reader = dataset.ToRecordBatchReader();
        var rowsRead = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            Assert.Equal(3, batch.ColumnCount);
            rowsRead += batch.Length;
        }

        Assert.Equal(rowsPerBatch * numBatches, rowsRead);
    }

    private static IArrowArrayStream GetTestData(int rowsPerBatch, int numBatches)
    {
        var recordBatches = new RecordBatch[numBatches];
        var stringValues = new[] { "abc", "def", "ghi" };
        for (int batchIdx = 0; batchIdx < numBatches; ++batchIdx)
        {
            recordBatches[batchIdx] = new RecordBatch.Builder()
                .Append("label", false,
                    builder => builder.String(stringBuilder =>
                        stringBuilder.AppendRange(Enumerable.Range(0, rowsPerBatch).Select(i => stringValues[i % stringValues.Length]))))
                .Append("x", false,
                    builder => builder.Int32(intBuilder =>
                        intBuilder.AppendRange(Enumerable.Range(batchIdx * rowsPerBatch, rowsPerBatch))))
                .Append("y", false,
                    builder => builder.Float(floatBuilder =>
                        floatBuilder.AppendRange(Enumerable.Range(batchIdx * rowsPerBatch, rowsPerBatch).Select(i => 0.1f * i))))
                .Build();
        }

        return new RecordBatchStream(recordBatches[0].Schema, recordBatches);
    }

    private static void WriteDataset(
        IArrowArrayStream recordBatchStream,
        FileSystem fileSystem,
        string baseDirectory,
        FileFormat fileFormat,
        Partitioning partitioning)
    {
        using var datasetWriteOptions = new FileSystemDatasetWriteOptions();
        datasetWriteOptions.BaseDir = baseDirectory;
        datasetWriteOptions.FileWriteOptions = fileFormat.DefaultFileWriteOptions();
        datasetWriteOptions.FileSystem = fileSystem;
        datasetWriteOptions.Partitioning = partitioning;
        datasetWriteOptions.BaseNameTemplate = "data_{i}.parquet";

        FileSystemDataset.WriteArrayStream(recordBatchStream, datasetWriteOptions);
    }
}
