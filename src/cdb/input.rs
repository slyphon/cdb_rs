use bytes::*;

use std::error;
use std::fs::File;
use std::io;
use std::io::{BufReader,BufWriter};
use std::io::prelude::*;
use std::num::ParseIntError;
use std::str;
use std::str::Utf8Error;
use std::fmt;

use super::KV;
use super::errors::WriterError;

enum BufReaders {
    FileBacked(BufReader<File>),
    BufBacked()
}

struct KVSizes(usize, usize);

const PLUS: u8  = 0x2b; // ASCII '+'
const COMMA: u8 = 0x2c; // ASCII ','
const COLON: u8 = 0x3a; // ASCII ':'
const DASH: u8  = 0x2a; // ASCII '-'
const GT: u8    = 0x3e; // ASCII '>'
const NL: u8    = 0x0a; // ASCII '\n'

fn parse_digits(buf: &[u8]) -> Result<usize, WriterError> {
    str::from_utf8(&buf)
        .map_err(|err| WriterError::UTF8Error(err))
        .and_then(|str|
            str.parse::<usize>()
                .map_err(|err| WriterError::ParseError(err))
        )
}

const ARROW_BYTES: &[u8; 2] = b"->";

// format: +1,3:a->xyz\n
fn read_begin<T: Read>(input: &mut BufReader<T>) -> Result<Option<()>, WriterError> {
    let mut buf = vec![0u8; 1];

    // consume a '+'
    input.read_exact(&mut buf)?;

    match buf[0] {
        PLUS => Ok(Some(())),
        NL   => Ok(None),
        wat  => panic!("encountered unexpected char: {}", wat),
    }
}

fn read_sizes<T: Read>(input: &mut BufReader<T>) -> Result<KVSizes, WriterError> {
    let mut buf: Vec<u8> = Vec::new();

    let r = input.read_until(COMMA, &mut buf)?;

    assert!(r > 0);
    assert_eq!(COMMA, buf.pop().unwrap());
    buf.pop(); // trim the comma off the end

    let k = parse_digits(&buf)?;
    buf.clear();

    let r = input.read_until(COLON, &mut buf)?;

    assert!(r > 0);
    assert_eq!(COLON, buf.pop().unwrap());
    let v = parse_digits(&buf)?;

    Ok(KVSizes(k, v))
}

fn read_kv<T: Read>(input: &mut BufReader<T>, kvs: &KVSizes) -> Result<KV, WriterError> {
    let KVSizes(ksize, vsize) = kvs;

    let mut kbytes = vec![0u8; *ksize];
    input.read_exact(&mut kbytes)?;

    // consume the "->" between k and v
    let mut arrowbytes: [u8; 2] = [0; 2];
    input.read_exact(&mut arrowbytes)?;
    assert_eq!(arrowbytes, *ARROW_BYTES);

    let mut vbytes = Vec::with_capacity(*vsize);
    input.read_exact(&mut vbytes)?;

    Ok(KV { k: Bytes::from(kbytes), v: Bytes::from(vbytes) })
}

fn read_one_record<T: Read>(input: &mut BufReader<T>) -> Result<Option<KV>, WriterError> {
    match read_begin(input)? {
        None => Ok(None),
        Some(_) =>
            read_sizes(input)
                .and_then(|sizes| read_kv(input, &sizes))
                .map(|kv| Some(kv))
    }
}

struct IterParser<'a> {
    file: &'a mut File
}

impl<'a> Iterator for IterParser<'a> {
    type Item = Result<KV, WriterError>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let mut buf = BufReader::new(&mut self.file);

        match read_one_record(&mut buf) {
            Ok(Some(kv)) => Some(Ok(kv)),
            Ok(None)     => None,
            Err(err)     => Some(Err(err)),
        }
    }
}


// expects input in CDB format '+ks,vs:k->v\n'
pub fn parse<'a>(file: &'a mut File) -> Box<Iterator<Item=Result<KV, WriterError>> + 'a> {
   Box::new(IterParser{file})
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_read_sizes() {
        let reader = Bytes::from("+3,4:cat->ball\n\n").into_buf().reader();
        let mut br = BufReader::new(reader);
    }


    #[test]
    fn parser_read_one_record() {
        let reader = Bytes::from("+3,4:cat->ball\n\n").into_buf().reader();
        let mut br = BufReader::new(reader);

        let recs: Vec<Result<KV, WriterError>> = parse(&mut br).collect();
        assert_eq!(recs.len(), 1);

        match recs[0] {
            Ok(KV{ref k, ref v}) => {
                assert_eq!(k, "cat");
                assert_eq!(v, "ball");
            }
            Err(ref x) => panic!("should not have errored")
        }
    }
}
