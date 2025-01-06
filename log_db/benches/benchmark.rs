mod utils;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use log_db::*;
use tempfile;
use utils::*;

pub fn upsert_various_initial_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_various_initial_sizes");

    for size in [0, 1000, 10_000, 100_000, 1_000_000, 10_000_000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");
        let mut db = DB::<Inst>::configure()
            .data_dir(&data_dir)
            .initialize()
            .expect("Failed to initialize DB");

        prefill_db(&mut db, size, false).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let inst = random_inst(0, size as i64 + 1);
                let _ = db.upsert(black_box(inst));
            });
        });
    }
}

pub fn upsert_various_initial_sizes_compacted(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_various_initial_sizes_compacted");

    for size in [0, 1000, 10_000, 100_000, 1_000_000, 10_000_000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");

        let record_length = 1 + // tombstone tag
            1 + 8 +     // int tag + int value
            1 + 8 + 5 + // string tag + string length + string value
            1 + 8 + 10; // bytes tag + bytes length + bytes value

        let mut db = DB::<Inst>::configure()
            .data_dir(&data_dir)
            .segment_size(1000 * record_length)
            .initialize()
            .expect("Failed to initialize DB");

        prefill_db(&mut db, size, true).expect("Failed to prefill DB");
        db.do_maintenance_tasks()
            .expect("Failed to do maintenance tasks");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            let mut i = 0;
            b.iter(|| {
                let inst = random_inst(0, size as i64 + 1);
                let _ = db.upsert(black_box(inst));

                if i % 100 == 0 {
                    db.do_maintenance_tasks()
                        .expect("Failed to do maintenance tasks");
                }
                i += 1;
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
            let mut db = DB::<Inst>::configure()
                .data_dir(&data_dir)
                .write_durability(mode.clone())
                .initialize()
                .expect("Failed to initialize DB");

            b.iter(|| {
                let inst = random_inst(0, 1000);
                let _ = db.upsert(black_box(inst));
            });
        });
    }
}

pub fn get_from_disk_various_initial_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_from_disk_various_initial_sizes");

    for size in [0, 1000, 10_000, 100_000, 1_000_000, 10_000_000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");
        let mut db = DB::<Inst>::configure()
            .data_dir(&data_dir)
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

    for size in [0, 1000, 10_000, 100_000, 1_000_000, 10_000_000] {
        let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
        let data_dir = &data_dir_obj
            .path()
            .to_str()
            .expect("Failed to convert tmpdir path to str");
        let mut db = DB::<Inst>::configure()
            .data_dir(&data_dir)
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
