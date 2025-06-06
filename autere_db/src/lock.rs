use super::*;

pub struct LockManager {
    lock_file: fs::File,
    excl_lock_file: fs::File,

    state: LockState,
}

#[derive(Debug, PartialEq, Eq)]
enum LockState {
    NotLocked,
    Shared,
    Exclusive,
}

impl LockManager {
    pub fn new(data_dir_path: PathBuf) -> DBResult<LockManager> {
        let lock_file = fs::File::create(data_dir_path.join(LOCK_FILENAME))?;
        let excl_lock_file = fs::File::create(data_dir_path.join(EXCL_LOCK_REQ_FILENAME))?;

        Ok(LockManager {
            lock_file,
            excl_lock_file,
            state: LockState::NotLocked,
        })
    }

    fn is_exclusive_lock_requested(&self) -> DBResult<bool> {
        // Attempt to acquire a shared lock on the lock request file
        // If the file is already locked, return false
        match fs2::FileExt::try_lock_shared(&self.excl_lock_file) {
            Err(e) => {
                if e.kind() == fs2::lock_contended_error().kind() {
                    return Ok(true);
                }
                return Err(DBError::IOError(e));
            }

            Ok(_) => {
                fs2::FileExt::unlock(&self.excl_lock_file)?;
                return Ok(false);
            }
        }
    }

    pub fn lock_shared(&mut self) -> DBResult<()> {
        if self.state == LockState::Shared {
            return Err(DBError::LockRequestError(
                "Already holding a shared lock".to_owned(),
            ));
        } else if self.state == LockState::Exclusive {
            return Err(DBError::LockRequestError(
                "Cannot acquire shared lock while holding an exclusive lock".to_owned(),
            ));
        }

        let mut timeout = 5;
        loop {
            if self.is_exclusive_lock_requested()? {
                debug!(
                    "Exclusive lock requested, waiting for {}ms before requesting a shared lock again",
                    timeout
                );
                thread::sleep(std::time::Duration::from_millis(timeout));
                timeout *= 2;

                if timeout > LOCK_WAIT_MAX_MS {
                    return Err(DBError::LockRequestError(
                        "Acquisition of shared lock timed out after {LOCK_WAIT_MAX_MS}".to_owned(),
                    ));
                }
            } else {
                fs2::FileExt::lock_shared(&self.lock_file)?;
                self.state = LockState::Shared;
                return Ok(());
            }
        }
    }

    pub fn lock_exclusive(&mut self) -> DBResult<()> {
        if self.state == LockState::Exclusive {
            return Err(DBError::LockRequestError(
                "Already holding an exclusive lock".to_owned(),
            ));
        } else if self.state == LockState::Shared {
            return Err(DBError::LockRequestError(
                "Cannot acquire exclusive lock while holding a shared lock".to_owned(),
            ));
        }

        // Create a lock on the exclusive lock request file to signal to readers that they should wait
        // This will block until the lock is acquired
        fs2::FileExt::lock_exclusive(&self.excl_lock_file)?;

        // Acquire an exclusive lock on the actual lock files
        fs2::FileExt::lock_exclusive(&self.lock_file)?;
        self.state = LockState::Exclusive;

        // Unlock the request file
        fs2::FileExt::unlock(&self.excl_lock_file)?;

        Ok(())
    }

    pub fn unlock(&mut self) -> DBResult<()> {
        if self.state == LockState::NotLocked {
            return Err(DBError::LockRequestError(
                "Not holding any locks".to_owned(),
            ));
        }

        fs2::FileExt::unlock(&self.lock_file)?;
        self.state = LockState::NotLocked;
        Ok(())
    }
}
