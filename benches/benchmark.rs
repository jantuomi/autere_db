mod utils;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use log_db::*;
use std::fs::OpenOptions;
use std::path::Path;
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
        prefill_db(&mut db, size).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let record = random_record(0, size as i64);
                let _ = db.upsert(black_box(&record));
            });
        });
    }
}

pub fn upsert_write_durability(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_write_durability");

    for mode in [
        WriteDurability::Async,
        WriteDurability::Flush,
        WriteDurability::FlushSync,
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(&mode), &mode, |b, _mode| {
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
            .memtable_capacity(0)
            .fields(&vec![
                (Field::Id, RecordFieldType::Int),
                (Field::Name, RecordFieldType::String),
                (Field::Data, RecordFieldType::Bytes),
            ])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to initialize DB");
        prefill_db(&mut db, size).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let id = random_int(0, size as i64 + 1);
                let _ = db.get(black_box(&RecordValue::Int(id)));
            });
        });
    }
}

pub fn get_various_memtable_capacities(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_various_memtable_capacities");

    const PREFILL_N: usize = 10000;
    let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
    let data_dir = &data_dir_obj
        .path()
        .to_str()
        .expect("Failed to convert tmpdir path to str");

    // Create a db instance for prefilling
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

    prefill_db(&mut db, PREFILL_N).expect("Failed to prefill DB");
    drop(db);

    // prefill_db generates IDs between 0..1000, so having memtable_capacity = 1000
    // effectively indexes the whole DB
    for size in (0..).map(|x| x * 100).take_while(|&x| x <= 1000) {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            let mut db = DB::configure()
                .data_dir(&data_dir)
                .fields(&vec![
                    (Field::Id, RecordFieldType::Int),
                    (Field::Name, RecordFieldType::String),
                    (Field::Data, RecordFieldType::Bytes),
                ])
                .memtable_capacity(size)
                .primary_key(Field::Id)
                .initialize()
                .expect("Failed to initialize DB");

            b.iter(|| {
                let id = random_int(0, 1000 + 1);
                let _ = db.get(black_box(&RecordValue::Int(id)));
            });
        });
    }
}

fn reverse_read_file_with_various_buffer_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("reverse_read_file_with_various_buffer_sizes");
    group.sample_size(50);

    // odd powers of 2
    let buffer_sizes = [128, 512, 2048, 8192, 32768, 131_072, 524_288];
    const PREFILL_N: usize = 100_000;

    let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
    let data_dir = &data_dir_obj
        .path()
        .to_str()
        .expect("Failed to convert tmpdir path to str");

    // Create a db instance for prefilling
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

    prefill_db(&mut db, PREFILL_N).expect("Failed to prefill DB");
    drop(db);

    for size in buffer_sizes {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            let mut file = OpenOptions::new()
                .read(true)
                .open(Path::new(data_dir).join("db"))
                .expect("Failed to open log file");

            b.iter(|| {
                let mut rev_reader = ReverseLogReader::new_with_size(&mut file, size)
                    .expect("Failed to create ReverseLogReader");

                // This is to avoid optimizing out the loop
                let mut i = 0;
                for _ in &mut rev_reader {
                    i += 1;
                }

                i
            });
        });
    }
}

// Register the benchmark group
criterion_group!(
    benches,
    upsert_various_initial_sizes,
    upsert_write_durability,
    get_from_disk_various_initial_sizes,
    get_various_memtable_capacities,
    reverse_read_file_with_various_buffer_sizes,
);
criterion_main!(benches);
