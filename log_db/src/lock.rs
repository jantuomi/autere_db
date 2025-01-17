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
    ManualExclusive,
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
        match self.excl_lock_file.try_lock_shared() {
            Err(e) => {
                if e.kind() == lock_contended_error().kind() {
                    return Ok(true);
                }
                return Err(DBError::IOError(e));
            }

            Ok(_) => {
                self.excl_lock_file.unlock()?;
                return Ok(false);
            }
        }
    }

    pub fn lock_shared(&mut self) -> DBResult<()> {
        if self.state == LockState::Shared {
            return Ok(());
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
                self.lock_file.lock_shared()?;
                self.state = LockState::Shared;
                return Ok(());
            }
        }
    }

    pub fn lock_exclusive(&mut self) -> DBResult<()> {
        if self.state == LockState::Exclusive || self.state == LockState::ManualExclusive {
            return Ok(());
        }

        // Create a lock on the exclusive lock request file to signal to readers that they should wait
        // This will block until the lock is acquired
        self.excl_lock_file.lock_exclusive()?;

        // Acquire an exclusive lock on the actual lock files
        self.lock_file.lock_exclusive()?;
        self.state = LockState::Exclusive;

        // Unlock the request file
        self.excl_lock_file.unlock()?;

        Ok(())
    }

    pub fn unlock(&mut self) -> DBResult<()> {
        self.lock_file.unlock()?;
        self.state = LockState::NotLocked;
        Ok(())
    }
}
