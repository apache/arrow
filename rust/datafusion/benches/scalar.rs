use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::scalar::ScalarValue;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("to_array_of_size 100000", |b| {
        let scalar = ScalarValue::Int32(Some(100));

        b.iter(|| assert_eq!(scalar.to_array_of_size(100000).null_count(), 0))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
