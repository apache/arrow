using System;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Dataset;

/// <summary>
/// Dataset that reads from data files stored in a file system
/// </summary>
public class FileSystemDataset : DatasetBase
{
    internal FileSystemDataset(GLibBindings.FileSystemDataset glibFileSystemDataset)
        : base(glibFileSystemDataset)
    {
    }

    public static void WriteArrayStream(IArrowArrayStream arrayStream, FileSystemDatasetWriteOptions options)
    {
        using var reader = ExportArrayStream(arrayStream);
        using var scanner = GLibBindings.ScannerBuilder.NewRecordBatchReader(reader)!.Finish()!;
        GLibBindings.FileSystemDataset.WriteScanner(scanner, options.GObj);
    }

    private static unsafe Apache.Arrow.GLibBindings.RecordBatchReader ExportArrayStream(IArrowArrayStream arrayStream)
    {
        var cStream = new CArrowArrayStream();
        CArrowArrayStreamExporter.ExportArrayStream(arrayStream, &cStream);
        return Apache.Arrow.GLibBindings.RecordBatchReader.Import(new IntPtr(&cStream))!;
    }
}
