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

pub fn upsert_various_initial_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_various_initial_sizes");

    for size in [100, 1000, 10000, 100_000, 1_000_000, 10_000_000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");
        let mut db = DB::configure()
            .data_dir(&data_dir)
            .fields(&[
                (Field::Id, RecordField::int()),
                (Field::Name, RecordField::string()),
                (Field::Data, RecordField::bytes()),
            ])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to initialize DB");

        prefill_db(&mut db, size, false).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let record = random_record(0, size as i64 + 1);
                let _ = db.upsert(black_box(&record));
            });
        });
    }
}

pub fn upsert_various_initial_sizes_compacted(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_various_initial_sizes_compacted");

    for size in [100, 1000, 10000, 100_000, 1_000_000, 10_000_000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");

        let sample_record = random_record(0, 1);
        let record_length = sample_record.serialize().len();

        let mut db = DB::configure()
            .data_dir(&data_dir)
            .fields(&[
                (Field::Id, RecordField::int()),
                (Field::Name, RecordField::string()),
                (Field::Data, RecordField::bytes()),
            ])
            .segment_size(1000 * record_length)
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to initialize DB");

        prefill_db(&mut db, size, true).expect("Failed to prefill DB");
        db.do_maintenance_tasks()
            .expect("Failed to do maintenance tasks");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let record = random_record(0, size as i64 + 1);
                let _ = db.upsert(black_box(&record));
                db.do_maintenance_tasks()
                    .expect("Failed to do maintenance tasks");
            });
        });
    }
}

pub fn upsert_write_durability(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_write_durability");

    for mode in [WriteDurability::Flush, WriteDurability::FlushSync] {
        group.bench_with_input(BenchmarkId::from_parameter(&mode), &mode, |b, _mode| {
            let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
            let data_dir = &data_dir_obj
                .path()
                .to_str()
                .expect("Failed to convert tmpdir path to str");
            let mut db = DB::configure()
                .data_dir(&data_dir)
                .fields(&[
                    (Field::Id, RecordField::int()),
                    (Field::Name, RecordField::string()),
                    (Field::Data, RecordField::bytes()),
                ])
                .write_durability(mode.clone())
                .primary_key(Field::Id)
                .initialize()
                .expect("Failed to initialize DB");

            b.iter(|| {
                let record = random_record(0, 1000);
                let _ = db.upsert(black_box(&record));
            });
        });
    }
}

pub fn get_from_disk_various_initial_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_from_disk_various_initial_sizes");

    for size in [0, 10, 100, 1000, 3300, 6700, 10000, 50000, 100_000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");
        let mut db = DB::configure()
            .data_dir(&data_dir)
            .fields(&[
                (Field::Id, RecordField::int()),
                (Field::Name, RecordField::string()),
                (Field::Data, RecordField::bytes()),
            ])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to initialize DB");
        prefill_db(&mut db, size, false).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let id = random_int(0, size as i64 + 1);
                let _ = db.get(black_box(&Value::Int(id)));
            });
        });
    }
}

pub fn get_from_disk_various_initial_sizes_compacted(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_from_disk_various_initial_sizes_compacted");

    for size in [0, 10, 100, 1000, 3300, 6700, 10000, 50000, 100_000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");
        let mut db = DB::configure()
            .data_dir(&data_dir)
            .fields(&[
                (Field::Id, RecordField::int()),
                (Field::Name, RecordField::string()),
                (Field::Data, RecordField::bytes()),
            ])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to initialize DB");

        prefill_db(&mut db, size, true).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let id = random_int(0, size as i64 + 1);
                let _ = db.get(black_box(&Value::Int(id)));
            });
        });
    }
}

// Register the benchmark group
criterion_group!(
    benches,
    upsert_various_initial_sizes,
    upsert_various_initial_sizes_compacted,
    upsert_write_durability,
    get_from_disk_various_initial_sizes,
    get_from_disk_various_initial_sizes_compacted,
);
criterion_main!(benches);
