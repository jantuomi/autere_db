use super::common::*;
use std::fs::{self};
use std::io::{self, Read, Seek, SeekFrom};

pub struct ReverseLogReader<'a> {
    file: &'a mut fs::File,
    /// The internal buffer used to read from the file.
    /// It is populated with the last INTERNAL_BUF_SIZE bytes read from the file
    /// and is used to read records in reverse order
    internal_buf: Vec<u8>,
    /// The current position in the internal buffer. It is decremented as bytes are read
    /// from the buffer. When a read is requested and the internal position is 0, the buffer
    /// is populated with the next (= closer to the start of the file) INTERNAL_BUF_SIZE bytes from the file.
    /// Note: This is the index of the next byte to be read from the internal buffer + 1
    internal_pos: usize,
    consumed_record_sep: bool,
}

const INTERNAL_BUF_SIZE: usize = 4096;
impl<'a> ReverseLogReader<'a> {
    pub fn new(file: &mut fs::File) -> Result<ReverseLogReader, io::Error> {
        file.seek(SeekFrom::End(0))?;
        Ok(ReverseLogReader {
            file,
            internal_buf: vec![0; INTERNAL_BUF_SIZE],
            internal_pos: 0,
            consumed_record_sep: false,
        })
    }

    pub fn new_with_size(
        file: &mut fs::File,
        internal_buf_size: usize,
    ) -> Result<ReverseLogReader, io::Error> {
        file.seek(SeekFrom::End(0))?;
        Ok(ReverseLogReader {
            file,
            internal_buf: vec![0; internal_buf_size],
            internal_pos: 0,
            consumed_record_sep: false,
        })
    }

    pub fn read_record(&mut self) -> Result<Option<Record>, io::Error> {
        if self.file.stream_position()? == 0 && self.internal_pos == 0 {
            return Ok(None);
        }

        if !self.consumed_record_sep {
            // Check that record ends with a record separator
            match self.read_special_sequence()? {
                SpecialSequence::RecordSeparator => {}
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Record does not end in record separator",
                    ));
                }
            }
        }
        self.consumed_record_sep = false;

        let mut result_buf: Vec<u8> = vec![];
        let mut read_buf = Vec::with_capacity(INTERNAL_BUF_SIZE);
        loop {
            read_buf.clear();
            let read = self.read_until(ESCAPE_CHARACTER, &mut read_buf)?;

            result_buf.extend(&read_buf[..read]);

            if self.file.stream_position()? == 0 && self.internal_pos == 0 {
                // We read until the start of the file, we are done
                break;
            }

            match self.read_special_sequence()? {
                SpecialSequence::LiteralEscape => {
                    result_buf.push(ESCAPE_CHARACTER);
                }
                SpecialSequence::LiteralFieldSeparator => {
                    result_buf.push(FIELD_SEPARATOR);
                }
                SpecialSequence::RecordSeparator => {
                    self.consumed_record_sep = true;
                    break;
                }
            }
        }

        result_buf.reverse();
        Ok(Some(Record::deserialize(&result_buf)))
    }

    /// Read exactly `buf.len()` bytes from the file, return an error if the file is exhausted.
    /// The bytes are returned in start -> end order.
    /// If an error is returned, the contents of `buf` are in an undefined state.
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let mut read = 0;
        while read < buf.len() {
            if self.internal_pos == 0 {
                let populated_n = self.populate_internal_buf()?;
                if populated_n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Unexpected end of file",
                    ));
                }
            }

            let end = buf.len() - read;
            let n = std::cmp::min(self.internal_pos, end);
            buf[end - n..end]
                .copy_from_slice(&self.internal_buf[self.internal_pos - n..self.internal_pos]);
            read += n;
            self.internal_pos -= n;
        }

        Ok(read)
    }

    fn populate_internal_buf(&mut self) -> Result<usize, io::Error> {
        let current_seek_pos = self.file.stream_position()? as usize;

        // Seek back by the size of the internal buffer or to the beginning of the file
        let seek_length = if current_seek_pos > self.internal_buf.len() {
            self.internal_pos = self.internal_buf.len();
            self.internal_buf.len()
        } else {
            self.internal_buf = vec![0; current_seek_pos as usize];
            self.internal_pos = current_seek_pos as usize;
            current_seek_pos
        };

        self.file.seek_relative(-(seek_length as i64))?;
        self.file.read_exact(&mut self.internal_buf)?;
        self.file.seek_relative(-(seek_length as i64))?;

        Ok(seek_length as usize)
    }

    /// Iterate over the internal buffer with internal_pos as the index.
    /// If the byte is found or the file has been exhausted, we return the number of bytes read.
    /// The `buf` parameter is used to store the bytes read from the internal buffer, excluding the found byte,
    /// in reverse order.
    fn read_until(&mut self, byte: u8, buf: &mut Vec<u8>) -> Result<usize, io::Error> {
        // TODO: optimize this
        let mut read = 0;
        loop {
            // If we reach internal_pos == 0, we need to populate the internal buffer.
            if self.internal_pos == 0 {
                let populated_n = self.populate_internal_buf()?;
                if populated_n == 0 {
                    return Ok(read);
                }
            }

            while self.internal_pos > 0 {
                let index = self.internal_pos - 1;
                if self.internal_buf[index] == byte {
                    return Ok(read);
                }
                buf.push(self.internal_buf[index]);
                read += 1;
                self.internal_pos -= 1;
            }
        }
    }

    fn read_special_sequence(&mut self) -> Result<SpecialSequence, io::Error> {
        let mut special_buf: Vec<u8> = vec![0; SEQ_RECORD_SEP.len()];
        self.read_exact(&mut special_buf)?;

        match validate_special(&special_buf.as_slice()) {
            Some(special) => Ok(special),
            None => {
                let pos = self.file.stream_position().unwrap() + self.internal_pos as u64;

                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Not a special sequence: {:?} at pos: {:x}",
                        special_buf, pos,
                    ),
                ))
            }
        }
    }
}

#[cfg(test)]
mod reverse_reader_tests {
    use super::*;
    use std::io::Write;
    use std::path::Path;

    #[test]
    fn test_read_until_found() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"hello,world").unwrap();
        let mut reader = ReverseLogReader::new(&mut file).unwrap();
        let mut buf = vec![];
        assert_eq!(reader.read_until(b',', &mut buf).unwrap(), 5);
        assert_eq!(buf, b"dlrow");
    }

    #[test]
    fn test_read_until_not_found() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"hello,world").unwrap();
        let mut reader = ReverseLogReader::new(&mut file).unwrap();
        let mut buf = vec![];
        let read = reader.read_until(b'!', &mut buf).unwrap();
        assert_eq!(buf, b"dlrow,olleh");
        assert_eq!(read, 11);
    }

    #[test]
    fn test_read_special_sequence() {
        let mut file = tempfile::tempfile().unwrap();
        let mut buf = vec![];
        buf.extend(SEQ_RECORD_SEP);
        buf.extend(SEQ_LIT_ESCAPE);
        buf.extend(SEQ_LIT_FIELD_SEP);
        // Note: written in start -> end order, read end -> start
        file.write_all(&buf).unwrap();

        let mut reader = ReverseLogReader::new(&mut file).unwrap();
        assert_eq!(
            reader.read_special_sequence().unwrap(),
            SpecialSequence::LiteralFieldSeparator
        );
        assert_eq!(
            reader.read_special_sequence().unwrap(),
            SpecialSequence::LiteralEscape
        );
        assert_eq!(
            reader.read_special_sequence().unwrap(),
            SpecialSequence::RecordSeparator
        );
    }

    #[test]
    fn test_populate_internal_buf() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"hello,world").unwrap();
        let mut reader = ReverseLogReader::new_with_size(&mut file, 3).unwrap();

        reader.populate_internal_buf().unwrap();
        assert_eq!(
            String::from_utf8(reader.internal_buf.clone()).unwrap(),
            "rld".to_string()
        );

        reader.populate_internal_buf().unwrap();
        assert_eq!(
            String::from_utf8(reader.internal_buf.clone()).unwrap(),
            ",wo".to_string()
        );

        reader.populate_internal_buf().unwrap();
        assert_eq!(
            String::from_utf8(reader.internal_buf.clone()).unwrap(),
            "llo".to_string()
        );

        reader.populate_internal_buf().unwrap();
        assert_eq!(
            String::from_utf8(reader.internal_buf.clone()).unwrap(),
            "he".to_string()
        );
    }

    #[test]
    fn test_reverse_log_reader_fixture_db1() {
        let db_path = Path::new(TEST_RESOURCES_DIR).join("test_db1");
        let mut file = fs::OpenOptions::new()
            .read(true)
            .open(&db_path)
            .expect("Failed to open file");
        let mut reverse_log_reader = ReverseLogReader::new(&mut file).unwrap();

        // There are two records in the log with "schema": Int, Null

        let last_record = reverse_log_reader
            .next()
            .expect("Failed to read the last record");
        assert!(match last_record.values.as_slice() {
            [RecordValue::Int(10), RecordValue::Null] => true,
            _ => false,
        });

        let first_record = reverse_log_reader
            .next()
            .expect("Failed to read the first record");
        assert!(match first_record.values.as_slice() {
            // Note: the int value is equal to the escape byte
            [RecordValue::Int(0x1D), RecordValue::Null] => true,
            _ => false,
        });

        assert!(reverse_log_reader.next().is_none());
    }

    #[test]
    fn test_read_exact() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"hello,world").unwrap();
        let mut reader = ReverseLogReader::new(&mut file).unwrap();
        let mut buf = vec![0; 3];

        let read = reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"rld");
        assert_eq!(read, 3);

        let read = reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b",wo");
        assert_eq!(read, 3);

        let read = reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"llo");
        assert_eq!(read, 3);

        assert!(reader.read_exact(&mut buf).unwrap_err().kind() == io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_read_exact_insufficient_bytes() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"hello").unwrap();
        let mut reader = ReverseLogReader::new(&mut file).unwrap();
        let mut buf = vec![0; 10];
        assert!(reader.read_exact(&mut buf).unwrap_err().kind() == io::ErrorKind::UnexpectedEof);
    }
}

impl Iterator for ReverseLogReader<'_> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_record() {
            Ok(Some(record)) => Some(record),
            Ok(None) => None,
            Err(err) => {
                panic!("Error reading record: {:?}", err,)
            }
        }
    }
}
