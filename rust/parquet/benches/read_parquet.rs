use std::{fs::File, path::PathBuf};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use parquet::errors::Result;

use parquet::file::serialized_reader::SerializedFileReader;
use parquet::*;
use parquet::arrow::*;

fn read_decompressed_pages(size: usize, column: usize) -> Result<()> {
    let dir = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(dir).join(format!("fixtures/pyarrow3/basic_nulls_{}.parquet", size));
    let mut file = File::open(path).unwrap();

    let reader =
            SerializedFileReader::new(file).expect("Failed to create serialized reader");

    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));

    let mut record_batch_reader = arrow_reader
        .get_record_reader_by_columns(vec![column], size)
        .expect("Failed to read into array!");

    let _ = record_batch_reader.collect::<Vec<_>>();
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    c.bench_function("read u64 10000", |b| {
        b.iter(|| read_decompressed_pages(10000, 0))
    });
    c.bench_function("read u64 100000", |b| {
        b.iter(|| read_decompressed_pages(100000, 0))
    });
    c.bench_function("read utf8 10000", |b| {
        b.iter(|| read_decompressed_pages(10000, 2))
    });
    c.bench_function("read utf8 100000", |b| {
        b.iter(|| read_decompressed_pages(100000, 2))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
