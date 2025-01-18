mod utils;

use std::collections::HashSet;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use log_db::*;
use tempfile;
use utils::*;

pub fn upsert_compacted(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_compacted");
    let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
    let data_dir = &data_dir_obj
        .path()
        .to_str()
        .expect("Failed to convert tmpdir path to str");
    let mut db = DB::<Inst>::configure()
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB");

    let mut insts = Vec::new();
    for size in [100_000, 1_000_000, 10_000_000] {
        println!("Prefilling DB to {} entries", size);
        prefill_db(&mut db, &mut insts, size, true).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let inst = random_inst(0, size as i64 + 1);
                let result = db.upsert(black_box(inst)).unwrap();
                assert!(result == ());
            });
        });
    }
}

pub fn delete_existing_compacted(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_existing_compacted");
    let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
    let data_dir = &data_dir_obj
        .path()
        .to_str()
        .expect("Failed to convert tmpdir path to str");
    let mut db = DB::<Inst>::configure()
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB");

    let mut insts = Vec::new();
    for size in [100_000, 1_000_000, 10_000_000] {
        println!("Prefilling DB to {} entries", size);
        prefill_db(&mut db, &mut insts, size, true).expect("Failed to prefill DB");

        let pk_set: HashSet<i64> = insts.iter().map(|inst| inst.id).collect();
        let mut pk_set_it = pk_set.into_iter();

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let result = db
                    .delete(black_box(&Value::Int(pk_set_it.next().unwrap())))
                    .unwrap();
                assert!(result.is_some());
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
                let result = db.upsert(black_box(inst)).unwrap();
                assert!(result == ());
            });
        });
    }
}

pub fn get_existing_compacted(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_existing_compacted");

    let data_dir_obj = tempfile::tempdir().expect("Failed to get tmpdir");
    let data_dir = &data_dir_obj
        .path()
        .to_str()
        .expect("Failed to convert tmpdir path to str");
    let mut db = DB::<Inst>::configure()
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB");

    let mut insts = Vec::new();
    let mut inst_index = 0;
    for size in [100_000, 1_000_000, 10_000_000] {
        println!("Prefilling DB to {} entries", size);
        prefill_db(&mut db, &mut insts, size, true).expect("Failed to prefill DB");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let id = insts[inst_index].id;
                let result = db.get(black_box(&Value::Int(id))).unwrap();
                assert!(result.is_some());
                inst_index = (inst_index + 1) % insts.len();
            });
        });
    }
}

pub fn find_by_existing_compacted(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_by_existing_compacted");

    let data_dir_path = tempfile::tempdir()
        .expect("Failed to get tmpdir")
        .into_path();
    let data_dir = data_dir_path
        .to_str()
        .expect("Failed to convert tmpdir path to str");
    let mut db = DB::<Inst>::configure()
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB");

    let mut insts = Vec::new();
    let mut inst_index = 0;
    for size in [100_000, 1_000_000, 10_000_000] {
        println!("Prefilling DB to {} entries", size);
        prefill_db(&mut db, &mut insts, size, true).expect("Failed to prefill DB");
        println!("DB prefilling done, insts.len = {}", insts.len());

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let name = insts[inst_index].name.clone();
                let result = db
                    .find_by(black_box(&Field::Name), black_box(&Value::String(name)))
                    .unwrap();
                assert!(result.len() > 0);
                inst_index = (inst_index + 1) % insts.len();
            });
        });
    }
}

// Register the benchmark group
criterion_group!(
    benches,
    upsert_compacted,
    delete_existing_compacted,
    upsert_write_durability,
    get_existing_compacted,
    find_by_existing_compacted,
);
criterion_main!(benches);
