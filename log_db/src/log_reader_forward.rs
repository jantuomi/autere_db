use super::common::*;
use std::fs::{self};
use std::io::{self, Read, Seek};

pub struct ForwardLogReader {
    metadata_reader: io::BufReader<fs::File>,
    data_reader: io::BufReader<fs::File>,
}

impl<'a> ForwardLogReader {
    pub fn new(metadata_file: fs::File, data_file: fs::File) -> ForwardLogReader {
        let mut ret = ForwardLogReader {
            metadata_reader: io::BufReader::new(metadata_file),
            data_reader: io::BufReader::new(data_file),
        };

        ret.metadata_reader
            .seek(io::SeekFrom::Start(METADATA_FILE_HEADER_SIZE as u64))
            .expect("Seek failed");

        ret
    }

    fn read_record(&mut self) -> Result<Option<Record>, io::Error> {
        let mut metadata_entry_buf = vec![0; 16]; // 2x u64
        if let Err(e) = self.metadata_reader.read_exact(&mut metadata_entry_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
            } else {
                return Err(e);
            }
        }

        // First u64 is the offset of the record in the data file, second is the length of the record
        let entry_offset = u64::from_be_bytes(metadata_entry_buf[0..8].try_into().unwrap());
        let entry_length = u64::from_be_bytes(metadata_entry_buf[8..16].try_into().unwrap());

        // Use .seek_relative instead of .seek to avoid dropping the BufReader internal buffer when
        // the seek distance is small
        let seek_distance = entry_offset as i64 - self.data_reader.stream_position()? as i64;
        self.data_reader.seek_relative(seek_distance)?;

        let mut result_buf = vec![0; entry_length as usize];
        self.data_reader.read_exact(&mut result_buf)?;

        let record = Record::deserialize(&result_buf);
        Ok(Some(record))
    }
}

impl Iterator for ForwardLogReader {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_record() {
            Ok(Some(record)) => Some(record),
            Ok(None) => None,
            Err(err) => panic!("Error reading record: {:?}", err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_forward_log_reader_fixture_db1() {
        let _ = env_logger::builder().is_test(true).try_init();
        let metadata_path = Path::new(TEST_RESOURCES_DIR).join("test_metadata_1");
        let data_path = Path::new(TEST_RESOURCES_DIR).join("test_data_1");
        let metadata_file = fs::OpenOptions::new()
            .read(true)
            .open(&metadata_path)
            .expect("Failed to open metadata file");
        let data_file = fs::OpenOptions::new()
            .read(true)
            .open(&data_path)
            .expect("Failed to open data file");

        let mut forward_log_reader = ForwardLogReader::new(metadata_file, data_file);

        // There are two records in the log with "schema" with one field: Bytes

        let first_record = forward_log_reader
            .next()
            .expect("Failed to read the first record");
        assert!(match first_record.values() {
            [Value::Bytes(bytes)] => bytes.len() == 256,
            _ => false,
        });

        assert!(forward_log_reader.next().is_none());
    }
}
