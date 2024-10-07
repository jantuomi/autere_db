use super::common::*;
use std::fs::{self};
use std::io::{self, BufRead, Read};

pub struct ForwardLogReader<'a> {
    reader: io::BufReader<&'a mut fs::File>,
}

impl<'a> ForwardLogReader<'a> {
    pub fn new(file: &mut fs::File) -> ForwardLogReader {
        let reader = io::BufReader::new(file);
        ForwardLogReader { reader }
    }

    fn read_record(&mut self) -> Result<Option<Record>, io::Error> {
        // The buffer that stores the bytes read from the file.
        let mut read_buf: Vec<u8> = Vec::new();
        // The buffer that stores all the bytes of the record read so far in reverse order.
        let mut result_buf: Vec<u8> = Vec::new();

        // Try reading a byte from the file.
        // If we've reached the end of the file, return None.
        let mut peek_buf = vec![0];
        match self.reader.read_exact(&mut peek_buf) {
            Ok(_) => {
                // Go back one byte
                self.reader.seek_relative(-1)?;
            }
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e);
            }
        }

        loop {
            read_buf.clear();
            self.reader.read_until(ESCAPE_CHARACTER, &mut read_buf)?;
            self.reader.seek_relative(-1)?;
            result_buf.extend(&read_buf[..read_buf.len() - 1]);

            // Otherwise, we must have encountered an escape character.
            match self.read_special_sequence()? {
                SpecialSequence::RecordSeparator => {
                    // The record is complete, so we can break out of the loop.
                    break;
                }
                SpecialSequence::LiteralFieldSeparator => {
                    // The field separator is escaped, so we need to add it to the result buffer.
                    result_buf.push(FIELD_SEPARATOR);
                }
                SpecialSequence::LiteralEscape => {
                    // The escape character is escaped, so we need to add it to the result buffer.
                    result_buf.push(ESCAPE_CHARACTER);
                }
            }
        }

        let record = Record::deserialize(&result_buf);
        Ok(Some(record))
    }

    fn read_special_sequence(&mut self) -> Result<SpecialSequence, io::Error> {
        let mut special_buf: Vec<u8> = vec![0; SEQ_RECORD_SEP.len()];
        self.reader.read_exact(&mut special_buf)?;

        match validate_special(&special_buf.as_slice()) {
            Some(special) => Ok(special),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Not a special sequence",
            )),
        }
    }
}

impl Iterator for ForwardLogReader<'_> {
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
mod reverse_reader_tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_forward_log_reader_fixture_db1() {
        let db_path = Path::new(TEST_RESOURCES_DIR).join("test_db1");
        let mut file = fs::OpenOptions::new()
            .read(true)
            .open(&db_path)
            .expect("Failed to open file");
        let mut forward_log_reader = ForwardLogReader::new(&mut file);

        // There are two records in the log with "schema": Int, Null

        let first_record = forward_log_reader
            .next()
            .expect("Failed to read the first record");
        assert!(match first_record.values.as_slice() {
            [RecordValue::Int(0x1D), RecordValue::Null] => true,
            _ => false,
        });

        let last_record = forward_log_reader
            .next()
            .expect("Failed to read the last record");
        assert!(match last_record.values.as_slice() {
            [RecordValue::Int(10), RecordValue::Null] => true,
            _ => false,
        });

        assert!(forward_log_reader.next().is_none());
    }
}
