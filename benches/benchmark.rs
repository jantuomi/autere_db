mod utils;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use log_db::*;
use tempfile;
use utils::*;

#[derive(Eq, PartialEq, Clone, Debug)]
enum Field {
    Id,
    Name,
    Data,
}

pub fn upsert_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("db.upsert");

    for size in [0, 10, 100, 1000, 10000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");
        let mut db = DB::configure()
            .data_dir(&data_dir)
            .fields(&vec![
                (Field::Id, RecordFieldType::Int),
                (Field::Name, RecordFieldType::String),
                (Field::Data, RecordFieldType::Bytes),
            ])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to initialize DB");
        prefill_db_with_n_records(&mut db, size).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let record = random_record();
                let _ = db.upsert(black_box(&record));
            });
        });
    }
}

pub fn get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("db.get");

    for size in [0, 10, 100, 1000, 10000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");
        let mut db = DB::configure()
            .data_dir(&data_dir)
            .fields(&vec![
                (Field::Id, RecordFieldType::Int),
                (Field::Name, RecordFieldType::String),
                (Field::Data, RecordFieldType::Bytes),
            ])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to initialize DB");
        prefill_db_with_n_records(&mut db, size).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let id = random_int();
                let _ = db.get(black_box(&RecordValue::Int(id)));
            });
        });
    }
}

// Register the benchmark group
criterion_group!(benches, upsert_benchmark, get_benchmark);
criterion_main!(benches);
