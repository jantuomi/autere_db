use super::common::*;
use std::fs::{self};
use std::io::{self, Read, Seek};

pub struct ReverseLogReader {
    /// The contents of the metadata file are read into this buffer in one go.
    metadata_buf: Vec<u8>,

    /// The data log file that is read based on offset + length information from the metadata file.
    data_reader: io::BufReader<fs::File>,

    /// The current position in metadata_buf.
    metadata_pos: usize,
}

impl ReverseLogReader {
    pub fn new(
        mut metadata_file: fs::File,
        data_file: fs::File,
    ) -> Result<ReverseLogReader, io::Error> {
        let mut metadata_buf = vec![];
        metadata_file.seek(io::SeekFrom::Start(0))?;
        metadata_file.read_to_end(&mut metadata_buf)?;
        let len = metadata_buf.len();

        let ret = ReverseLogReader {
            metadata_buf,
            data_reader: io::BufReader::new(data_file),
            metadata_pos: len,
        };
        Ok(ret)
    }

    fn read_record(&mut self) -> Result<Option<Record>, io::Error> {
        assert!(
            (self.metadata_pos - METADATA_FILE_HEADER_SIZE) % 16 == 0,
            "metadata_pos is not aligned"
        );

        // Return None if we have read the entire metadata file and
        // reached the end of the header
        if self.metadata_pos == METADATA_FILE_HEADER_SIZE {
            return Ok(None);
        }

        self.metadata_pos -= 16;
        let i = self.metadata_pos; // shorter alias

        // First u64 is the offset of the record in the data file, second is the length of the record
        let entry_offset = u64::from_be_bytes(self.metadata_buf[i..i + 8].try_into().unwrap());
        let entry_length = u64::from_be_bytes(self.metadata_buf[i + 8..i + 16].try_into().unwrap());

        self.data_reader.seek(io::SeekFrom::Start(entry_offset))?;
        let mut result_buf = vec![0; entry_length as usize];
        self.data_reader.read_exact(&mut result_buf)?;

        let record = Record::deserialize(&result_buf);

        assert!(
            (self.metadata_pos - METADATA_FILE_HEADER_SIZE) % 16 == 0,
            "metadata_pos is not aligned"
        );
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_reverse_log_reader_fixture_db1() {
        let _ = env_logger::builder().is_test(true).try_init();
        let metadata_path = Path::new(TEST_RESOURCES_DIR).join("test_metadata_1");
        let metadata_file = fs::OpenOptions::new()
            .read(true)
            .open(&metadata_path)
            .expect("Failed to open file");
        let data_file = fs::OpenOptions::new()
            .read(true)
            .open(Path::new(TEST_RESOURCES_DIR).join("test_data_1"))
            .expect("Failed to open file");

        let mut reverse_log_reader = ReverseLogReader::new(metadata_file, data_file).unwrap();

        // There are two records in the log with "schema": Int

        let last_record = reverse_log_reader
            .next()
            .expect("Failed to read the last record");
        assert!(match last_record.values.as_slice() {
            [RecordValue::Bytes(bytes)] => bytes.len() == 256,
            _ => false,
        });

        assert!(reverse_log_reader.next().is_none());
    }
}

impl Iterator for ReverseLogReader {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_record() {
            Ok(Some(record)) => Some(record),
            Ok(None) => None,
            Err(err) => {
                panic!("Error reading record: {:?}", err)
            }
        }
    }
}
