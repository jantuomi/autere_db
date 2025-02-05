use super::*;

pub struct Engine<T, F: Eq + Clone> {
    pub config: Config<T, F>,
    pub lock_manager: LockManager,

    data_dir_path: PathBuf,
    primary_key_index: usize,
    refresh_next_logkey: LogKey,

    pub tx_active: bool,
    pub tx_log: Vec<TxEntry>,

    active_metadata_file: fs::File,
    active_data_file: fs::File,

    // TODO: these could be made private. Currently they are public for testing in lib.rs.
    pub primary_memtable: PrimaryMemtable,
    pub secondary_memtables: Vec<SecondaryMemtable>,
}

impl<T, F: Eq + Clone> Engine<T, F> {
    pub fn initialize(config: Config<T, F>) -> DBResult<Engine<T, F>> {
        info!("Initializing DB...");
        // If data_dir does not exist or is empty, create it and any necessary files.
        // After creation, the directory should always be in a complete state without missing files.

        // Ensure the data directory exists
        let data_dir_path = Path::new(&config.data_dir).to_path_buf();
        match fs::create_dir(&data_dir_path) {
            Ok(_) => {}
            Err(e) => {
                if e.kind() != io::ErrorKind::AlreadyExists {
                    return Err(DBError::IOError(e));
                }
            }
        }

        // Create the lock file first to prevent multiple concurrent initializations
        let mut lock_manager = LockManager::new(data_dir_path.clone())?;
        lock_manager.lock_exclusive()?;

        // We have acquired the lock, check if the data directory is in a complete state
        // If not, initialize it, otherwise skip.
        if !fs::exists(data_dir_path.join(INITIALIZED_FILENAME))? {
            // Delete all files except the lock files to ensure a clean state
            for entry in fs::read_dir(&data_dir_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file()
                    && path.file_name().unwrap() != LOCK_FILENAME
                    && path.file_name().unwrap() != EXCL_LOCK_REQ_FILENAME
                {
                    fs::remove_file(&path)?;
                }
            }

            // Create the initial segment files
            let (segment_uuid, _) = create_segment_data_file(&data_dir_path)?;
            let (segment_num, _) = create_segment_metadata_file(&data_dir_path, &segment_uuid)?;
            set_active_segment(&data_dir_path, segment_num)?;

            // Create the initialized file to indicate that the directory is in a complete state
            fs::File::create(data_dir_path.join(INITIALIZED_FILENAME))?;
        }

        // Calculate the index of the primary value in a record
        let primary_key_index = config
            .schema
            .iter()
            .position(|(field, _)| field == &config.primary_key)
            .ok_or(DBError::ValidationError(
                "Primary key not found in schema after initialize".to_owned(),
            ))?;

        // Join primary key and secondary keys vec into a single vec
        let mut all_keys = vec![&config.primary_key];
        all_keys.extend(&config.secondary_keys);

        // If any of the keys is not in the schema or
        // is not an IndexableValue, return an error
        for &key in &all_keys {
            let (_, value_type) = config.schema.iter().find(|(field, _)| field == key).ok_or(
                DBError::ValidationError("Key must be present in the field schema".to_owned()),
            )?;

            match value_type.primitive {
                PrimitiveType::Int | PrimitiveType::String => {}
                _ => return Err(DBError::ValidationError("Key must be indexable".to_owned())),
            }
        }
        let primary_memtable = PrimaryMemtable::new();
        let secondary_memtables = config
            .secondary_keys
            .iter()
            .map(|_| SecondaryMemtable::new())
            .collect();

        let active_symlink = Path::new(&config.data_dir).join(ACTIVE_SYMLINK_FILENAME);

        let active_target = fs::read_link(&active_symlink)?;
        let active_metadata_path = Path::new(&config.data_dir).join(active_target);
        let mut active_metadata_file = APPEND_MODE.open(&active_metadata_path)?;

        let active_metadata_header = read_metadata_header(&mut active_metadata_file)?;
        validate_metadata_header(&active_metadata_header)?;

        let active_data_path =
            Path::new(&config.data_dir).join(active_metadata_header.uuid.to_string());
        let active_data_file = APPEND_MODE.open(&active_data_path)?;

        let mut engine = Engine::<T, F> {
            config,
            lock_manager,
            data_dir_path,
            primary_key_index,
            primary_memtable,
            secondary_memtables,
            active_metadata_file,
            active_data_file,
            refresh_next_logkey: LogKey::new(1, 0),
            tx_active: false,
            tx_log: vec![],
        };

        info!("Rebuilding memtable indexes...");
        engine.refresh_indexes()?;

        info!("Database ready.");

        engine.lock_manager.unlock()?;
        Ok(engine)
    }

    pub fn refresh_indexes(&mut self) -> DBResult<()> {
        let active_symlink_path = self.data_dir_path.join(ACTIVE_SYMLINK_FILENAME);
        let active_target = fs::read_link(active_symlink_path)?;
        let active_metadata_path = self.data_dir_path.join(active_target);

        let to_segnum = parse_segment_number(&active_metadata_path)?;
        let from_segnum = self.refresh_next_logkey.segment_num();
        let mut from_index = self.refresh_next_logkey.index();

        for segnum in from_segnum..=to_segnum {
            let metadata_path = self.data_dir_path.join(metadata_filename(segnum));
            let mut metadata_file = READ_MODE.open(&metadata_path)?;

            let metadata_len = metadata_file.seek(SeekFrom::End(0))?;
            if (metadata_len - METADATA_FILE_HEADER_SIZE as u64) % METADATA_ROW_LENGTH as u64 != 0 {
                return Err(DBError::ConsistencyError(format!(
                    "Metadata file {} has invalid size: {}",
                    metadata_path.display(),
                    metadata_len
                )));
            }

            let metadata_header = read_metadata_header(&mut metadata_file)?;
            validate_metadata_header(&metadata_header)?;

            let data_path = self.data_dir_path.join(metadata_header.uuid.to_string());
            let data_file = READ_MODE.open(data_path)?;

            for ForwardLogReaderItem { record, index } in
                ForwardLogReader::new_with_index(metadata_file, data_file, from_index)
            {
                // Validate that the values in the record are compatible with the schema
                record.validate(&self.config.schema)?;

                let log_key = LogKey::new(segnum, index);

                if record.tombstone {
                    self.remove_record_from_memtables(&record);
                } else {
                    self.insert_record_to_memtables(log_key, record);
                }

                // Update from_index in case this is the last iteration: we need to know the next
                // index that should be read on later invocations of refresh_indexes.
                from_index = index + 1
            }

            // If there are still segments to read, set from_index to zero to read them
            // from beginning. Otherwise we leave from_index as the index of the next record to read.
            if segnum != to_segnum {
                from_index = 0
            }
        }

        self.refresh_next_logkey = LogKey::new(to_segnum, from_index);

        Ok(())
    }

    fn insert_record_to_memtables(&mut self, log_key: LogKey, record: Record) {
        for (sk_index, sk_field) in self.config.secondary_keys.iter().enumerate() {
            let secondary_memtable = &mut self.secondary_memtables[sk_index];
            let sk_field_index = self
                .config
                .schema
                .iter()
                .position(|(f, _)| sk_field == f)
                .unwrap();
            let sk = record.at(sk_field_index).as_indexable().unwrap();

            secondary_memtable.set(sk, log_key.clone());
        }

        // Doing this last because this moves log_key
        let pk = record.at(self.primary_key_index).as_indexable().unwrap();
        self.primary_memtable.set(pk, log_key);
    }

    fn remove_record_from_memtables(&mut self, record: &Record) {
        let pk = record.at(self.primary_key_index).as_indexable().unwrap();

        if let Some(plk) = self.primary_memtable.remove(&pk) {
            // TODO this does not work (test_delete_by_multiple_indexes)
            for (sk_index, sk_field) in self.config.secondary_keys.iter_mut().enumerate() {
                let secondary_memtable = &mut self.secondary_memtables[sk_index];
                let sk_field_index = self
                    .config
                    .schema
                    .iter()
                    .position(|(f, _)| sk_field == f)
                    .unwrap();
                let sk = record.at(sk_field_index).as_indexable().unwrap();

                secondary_memtable.remove(&sk, &plk);
            }
        }
    }

    pub fn upsert_record(&mut self, record: Record) -> DBResult<()> {
        debug!("Opening file in append mode...");

        if !self.ensure_metadata_file_is_active()?
            || !ensure_active_metadata_is_valid(
                &self.data_dir_path,
                &mut self.active_metadata_file,
            )?
        {
            // The log file has been rotated, so we must try again
            return self.upsert_record(record);
        }

        self.tx_log.push(TxEntry::Upsert { record });

        if !self.tx_active {
            self.commit_transaction()?;
            self.tx_log.clear();
        }

        Ok(())
    }

    pub fn batch_find_by_records<'a>(
        &mut self,
        field: &F,
        values: impl Iterator<Item = &'a Value>,
    ) -> DBResult<Vec<(usize, Record)>> {
        let field_type = self.get_field_type(field).ok_or(DBError::ValidationError(
            "Field not found in schema".to_owned(),
        ))?;

        let indexables = values
            .map(|value| {
                if type_check(&value, &field_type) {
                    value.as_indexable().ok_or(DBError::ValidationError(
                        "Queried value must be indexable".to_owned(),
                    ))
                } else {
                    Err(DBError::ValidationError(format!(
                        "Queried value {:?} does not match key type: {:?}",
                        value, field_type
                    )))
                }
            })
            .collect::<DBResult<Vec<IndexableValue>>>()?;

        // Otherwise, continue with querying secondary indexes.
        debug!("Finding all records with matching fields");

        if self.config.read_consistency == ReadConsistency::Strong {
            self.refresh_indexes()?;
        }

        let log_key_batches = indexables
            .into_iter()
            .map(|query_key| {
                if field == &self.config.primary_key {
                    let opt = self.primary_memtable.get(&query_key);
                    let log_keys = match opt {
                        Some(log_key) => vec![log_key],
                        None => vec![],
                    };
                    Ok(log_keys)
                } else {
                    let smemtable_index = match get_secondary_memtable_index_by_field(
                        &self.config.secondary_keys,
                        field,
                    ) {
                        Some(index) => index,
                        None => {
                            return Err(DBError::ValidationError(
                                "Cannot find_by by non-indexed key".to_owned(),
                            ))
                        }
                    };

                    let log_keys = self.secondary_memtables[smemtable_index]
                        .find_by(&query_key)
                        .into_iter()
                        .collect();
                    Ok(log_keys)
                }
            })
            .collect::<DBResult<Vec<Vec<&LogKey>>>>()?;

        debug!("Found log keys in memtable: {:?}", log_key_batches);

        let mut tagged = vec![];
        for (tag, batch) in log_key_batches.into_iter().enumerate() {
            let mapped = batch.into_iter().map(|log_key| (tag, log_key));
            tagged.extend(mapped);
        }

        let tagged_records = self.read_tagged_log_keys(tagged.into_iter())?;

        debug!("Read {} records", tagged_records.len());

        Ok(tagged_records)
    }

    /// Read records from segment files based on log keys.
    /// The log keys are accompanied by an integer tag that can be used to identify and group them later.
    fn read_tagged_log_keys<'a>(
        &self,
        log_keys: impl Iterator<Item = (usize, &'a LogKey)>,
    ) -> DBResult<Vec<(usize, Record)>> {
        let mut records = vec![];
        let mut log_keys_map = BTreeMap::new();

        for (tag, log_key) in log_keys {
            if !log_keys_map.contains_key(&log_key.segment_num()) {
                log_keys_map.insert(log_key.segment_num(), vec![(tag, log_key.index())]);
            } else {
                log_keys_map
                    .get_mut(&log_key.segment_num())
                    .unwrap()
                    .push((tag, log_key.index()));
            }
        }

        for (segment_num, mut segment_indexes) in log_keys_map {
            segment_indexes.sort_unstable();

            let metadata_path = &self.data_dir_path.join(metadata_filename(segment_num));
            let mut metadata_file = READ_MODE.open(&metadata_path)?;

            let metadata_header = read_metadata_header(&mut metadata_file)?;

            let data_path = &self.data_dir_path.join(metadata_header.uuid.to_string());
            let mut data_file = READ_MODE.open(&data_path)?;

            let header_size = METADATA_FILE_HEADER_SIZE as i64;
            let row_length = METADATA_ROW_LENGTH as i64;
            let mut current_metadata_offset = header_size;
            for (tag, segment_index) in segment_indexes {
                let new_metadata_offset = header_size + segment_index as i64 * row_length;
                metadata_file.seek_relative(new_metadata_offset - current_metadata_offset)?;

                let mut metadata_buf = [0; METADATA_ROW_LENGTH];
                metadata_file.read_exact(&mut metadata_buf)?;

                let data_offset = u64::from_be_bytes(metadata_buf[0..8].try_into().unwrap());
                let data_length = u64::from_be_bytes(metadata_buf[8..16].try_into().unwrap());
                assert!(data_length > 0);

                data_file.seek(SeekFrom::Start(data_offset))?;

                let mut data_buf = vec![0; data_length as usize];
                data_file.read_exact(&mut data_buf)?;

                let record = Record::deserialize(&data_buf);
                records.push((tag, record));

                current_metadata_offset = new_metadata_offset + row_length;
            }
        }

        Ok(records)
    }

    pub fn range_by_records<B: RangeBounds<Value>>(
        &mut self,
        field: &F,
        range: B,
    ) -> DBResult<Vec<Record>> {
        fn range_bound_to_indexable(
            bound: Bound<&Value>,
            field_type: &Type,
        ) -> DBResult<Bound<IndexableValue>> {
            fn convert(value: &Value, field_type: &Type) -> DBResult<IndexableValue> {
                if !type_check(&value, field_type) {
                    return Err(DBError::ValidationError(format!(
                        "Queried value does not match type: {:?}",
                        field_type
                    )));
                }
                value.as_indexable().ok_or(DBError::ValidationError(
                    "Queried value must be indexable".to_owned(),
                ))
            }

            match bound {
                Bound::Included(value) => convert(value, field_type).map(Bound::Included),
                Bound::Excluded(value) => convert(value, field_type).map(Bound::Excluded),
                Bound::Unbounded => Ok(Bound::Unbounded),
            }
        }

        let field_type = self.get_field_type(field).ok_or(DBError::ValidationError(
            "Field not found in schema".to_owned(),
        ))?;

        let start_indexable = range_bound_to_indexable(range.start_bound(), field_type)?;
        let end_indexable = range_bound_to_indexable(range.end_bound(), field_type)?;

        let indexable_bounds = OwnedBounds::new(start_indexable, end_indexable);

        if self.config.read_consistency == ReadConsistency::Strong {
            self.refresh_indexes()?;
        }

        let log_keys = if field == &self.config.primary_key {
            self.primary_memtable.range(indexable_bounds)
        } else {
            let index = get_secondary_memtable_index_by_field(&self.config.secondary_keys, field)
                .ok_or_else(|| {
                DBError::ValidationError("Cannot range_by by non-indexed key".to_owned())
            })?;

            self.secondary_memtables[index].range(indexable_bounds)
        };

        let log_key_batches = log_keys.into_iter().map(|log_key| (0, log_key));

        let tagged_records = self.read_tagged_log_keys(log_key_batches);

        Ok(tagged_records?.into_iter().map(|(_, rec)| rec).collect())
    }

    /// Ensures that the `self.metadata_file` and `self.data_file` handles are still pointing to the correct files.
    /// If the segment has been rotated, the handle will be closed and reopened.
    /// Returns `false` if the file has been rotated and the handle has been reopened, `true` otherwise.
    fn ensure_metadata_file_is_active(&mut self) -> DBResult<bool> {
        let active_target = fs::read_link(&self.data_dir_path.join(ACTIVE_SYMLINK_FILENAME))?;
        let active_metadata_path = &self.data_dir_path.join(active_target);

        let correct = is_file_same_as_path(&self.active_metadata_file, &active_metadata_path)?;
        if !correct {
            debug!("Metadata file has been rotated. Reopening...");
            let metadata_file = APPEND_MODE.open(&active_metadata_path)?;

            let metadata_header = read_metadata_header(&mut self.active_metadata_file)?;

            validate_metadata_header(&metadata_header)?;

            let data_file_path = &self.data_dir_path.join(metadata_header.uuid.to_string());

            self.active_metadata_file = metadata_file;
            self.active_data_file = APPEND_MODE.open(&data_file_path)?;

            return Ok(false);
        } else {
            return Ok(true);
        }
    }

    pub fn delete_by_field(&mut self, field: &F, value: &Value) -> DBResult<Vec<Record>> {
        let recs: Vec<Record> = self
            .batch_find_by_records(field, std::iter::once(value))?
            .into_iter()
            .map(|(_, mut rec)| {
                rec.tombstone = true;
                rec
            })
            .collect();

        // TODO: refactor the clone out of here
        for record in &recs {
            self.tx_log.push(TxEntry::Delete {
                record: record.clone(),
            });
        }

        if !self.tx_active {
            self.commit_transaction()?;
            self.tx_log.clear();
        }

        debug!("Records deleted");

        Ok(recs)
    }

    pub fn commit_transaction(&mut self) -> DBResult<()> {
        let active_symlink_path = self.data_dir_path.join(ACTIVE_SYMLINK_FILENAME);
        let active_target = fs::read_link(active_symlink_path)?;
        let segment_num = parse_segment_number(&active_target)?;

        let initial_data_offset = self.active_data_file.seek(SeekFrom::End(0))?;
        let initial_metadata_offset = self.active_metadata_file.seek(SeekFrom::End(0))?;
        let mut serialized_data: Vec<u8> = vec![];
        let mut serialized_metadata: Vec<u8> = Vec::with_capacity(self.tx_log.len() * 16);
        let mut pending_memtable_ops: Vec<(LogKey, TxEntry)> = vec![];

        let mut metadata_buf = [0u8; 16];

        debug!("Serializing tx_log to byte arrays");
        for tx_entry in &self.tx_log {
            let record = match tx_entry {
                TxEntry::Upsert { record } => record,
                TxEntry::Delete { record } => record,
            };

            let serialized = record.serialize();
            let record_offset = initial_data_offset + serialized_data.len() as u64;
            let record_length = serialized.len() as u64;
            assert!(record_length > 0);

            serialized_data.extend(serialized);

            let metadata_pos = initial_metadata_offset + serialized_metadata.len() as u64;
            let metadata_index =
                (metadata_pos - METADATA_FILE_HEADER_SIZE as u64) / METADATA_ROW_LENGTH as u64;

            // Write the record metadata to the fixed-size metadata buffer
            metadata_buf[..8].copy_from_slice(&record_offset.to_be_bytes());
            metadata_buf[8..].copy_from_slice(&record_length.to_be_bytes());

            serialized_metadata.extend_from_slice(&metadata_buf);

            let log_key = LogKey::new(segment_num, metadata_index);
            pending_memtable_ops.push((log_key, tx_entry.clone()));
        }

        debug!("Writing serialized bytearrays to log files");
        self.active_data_file.write_all(&serialized_data)?;
        self.active_metadata_file.write_all(&serialized_metadata)?;

        // Flush and sync data and metadata to disk
        if self.config.write_durability == WriteDurability::Flush {
            self.active_data_file.flush()?;
            self.active_metadata_file.flush()?;
        } else if self.config.write_durability == WriteDurability::FlushSync {
            self.active_data_file.flush()?;
            self.active_data_file.sync_all()?;
            self.active_metadata_file.flush()?;
            self.active_metadata_file.sync_all()?;
        }

        debug!("Updating memtables");
        for (log_key, tx_entry) in pending_memtable_ops {
            match tx_entry {
                TxEntry::Upsert { record } => self.insert_record_to_memtables(log_key, record),
                TxEntry::Delete { record } => self.remove_record_from_memtables(&record),
            }
        }
        debug!("Commit done");

        Ok(())
    }

    pub fn do_maintenance_tasks(&mut self) -> DBResult<()> {
        ensure_active_metadata_is_valid(&self.data_dir_path, &mut self.active_metadata_file)?;

        let metadata_size = self.active_metadata_file.seek(SeekFrom::End(0))?;
        if metadata_size >= self.config.segment_size as u64 {
            self.rotate_and_compact()?;
        }

        Ok(())
    }

    fn rotate_and_compact(&mut self) -> DBResult<()> {
        debug!("Active log size exceeds threshold, starting rotation and compaction...");

        let original_data_len = self.active_data_file.seek(SeekFrom::End(0))?;

        let active_target = fs::read_link(&self.data_dir_path.join(ACTIVE_SYMLINK_FILENAME))?;
        let active_num = parse_segment_number(&active_target)?;

        debug!("Reading segment data into a BTreeMap");
        let mut pk_to_item_map: BTreeMap<&IndexableValue, &Record> = BTreeMap::new();
        let forward_read_items: Vec<(IndexableValue, Record)> = ForwardLogReader::new(
            self.active_metadata_file.try_clone()?,
            self.active_data_file.try_clone()?,
        )
        .map(|item| {
            (
                item.record
                    .at(self.primary_key_index)
                    .as_indexable()
                    .expect("Primary key was not indexable"),
                item.record,
            )
        })
        .collect();

        for (pk, record) in forward_read_items.iter() {
            pk_to_item_map.insert(pk, record);
        }

        debug!(
            "Read {} records, out of which {} were unique",
            forward_read_items.len(),
            pk_to_item_map.len()
        );

        // Create a new log data file and write it
        debug!("Opening new data file and writing compacted data");
        let (new_data_uuid, new_data_path) = create_segment_data_file(&self.data_dir_path)?;
        let mut new_data_file = APPEND_MODE.open(&new_data_path)?;

        let mut pk_to_data_map = BTreeMap::new();
        let mut offset = 0u64;
        for (pk, record) in pk_to_item_map.into_iter() {
            let serialized = record.serialize();
            let len = serialized.len() as u64;
            new_data_file.write_all(&serialized)?;

            pk_to_data_map.insert(pk, (offset, len));
            offset += len;
        }

        // Sync the data file to disk.
        // This is fine to do without consulting WriteDurability because this is a one-off
        // operation that is not part of the normal write path.
        new_data_file.flush()?;
        new_data_file.sync_all()?;

        let final_data_len = new_data_file.seek(io::SeekFrom::End(0))?;
        debug!(
            "Wrote compacted data, reduced data size: {} -> {}",
            original_data_len, final_data_len
        );

        // Create a new log metadata file and write it
        debug!("Opening temp metadata file and writing pointers to compacted data file");
        let temp_metadata_file = tempfile::NamedTempFile::new()?;
        let temp_metadata_path = temp_metadata_file.as_ref();
        let mut temp_metadata_file = WRITE_MODE.open(temp_metadata_path)?;

        let metadata_header = MetadataHeader {
            version: 1,
            uuid: new_data_uuid,
        };

        temp_metadata_file.write_all(&metadata_header.serialize())?;

        let mut metadata_buf = [0u8; 16];
        for (pk, _) in forward_read_items.iter() {
            let (offset, len) = pk_to_data_map.get(&pk).unwrap();

            metadata_buf[..8].copy_from_slice(&offset.to_be_bytes());
            metadata_buf[8..].copy_from_slice(&len.to_be_bytes());

            temp_metadata_file.write_all(&metadata_buf)?;
        }

        // Sync the metadata file to disk, see comment above about sync.
        temp_metadata_file.flush()?;
        temp_metadata_file.sync_all()?;

        debug!("Moving temporary files to their final locations");
        let new_data_path = &self.data_dir_path.join(new_data_uuid.to_string());
        let active_metadata_path = &self.data_dir_path.join(metadata_filename(active_num)); // overwrite active

        fs::rename(&temp_metadata_path, &active_metadata_path)?;

        debug!("Compaction complete, creating new segment");

        let new_segment_num = active_num + 1;
        let new_metadata_path = self.data_dir_path.join(metadata_filename(new_segment_num));
        let mut new_metadata_file = APPEND_MODE.clone().create(true).open(&new_metadata_path)?;

        let new_metadata_header = MetadataHeader {
            version: 1,
            uuid: new_data_uuid,
        };

        new_metadata_file.write_all(&new_metadata_header.serialize())?;

        set_active_segment(&self.data_dir_path, new_segment_num)?;

        self.active_metadata_file = APPEND_MODE.open(&new_metadata_path)?;
        self.active_data_file = APPEND_MODE.open(&new_data_path)?;

        debug!(
            "Active log file {} rotated and compacted, new segment: {}",
            active_num, new_segment_num
        );

        Ok(())
    }

    #[inline]
    fn get_field_type(&self, field: &F) -> Option<&Type> {
        self.config
            .schema
            .iter()
            .find(|(f, _)| f == field)
            .map(|(_, t)| t)
    }

    #[inline]
    pub fn with_exclusive_lock<A>(
        &mut self,
        f: impl FnOnce(&mut Self) -> DBResult<A>,
    ) -> DBResult<A> {
        // No need to acquire a lock if a transaction is already active
        // because the lock is already held.
        if !self.tx_active {
            self.lock_manager.lock_exclusive()?;
        }
        let result = f(self);
        if !self.tx_active {
            self.lock_manager.unlock()?;
        }
        result
    }

    #[inline]
    pub fn with_shared_lock<A>(&mut self, f: impl FnOnce(&mut Self) -> DBResult<A>) -> DBResult<A> {
        // No need to acquire a lock if a transaction is already active
        // because the lock is already held.
        if !self.tx_active {
            self.lock_manager.lock_shared()?;
        }
        let result = f(self);
        if !self.tx_active {
            self.lock_manager.unlock()?;
        }

        result
    }
}
