#[allow(dead_code)]

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

pub struct Writer { }

struct KVSizes(usize, usize);

const PLUS: u8  = 0x2b; // ASCII '+'
const COMMA: u8 = 0x2c; // ASCII ','
const COLON: u8 = 0x3a; // ASCII ':'
const DASH: u8  = 0x2a; // ASCII '-'
const GT: u8    = 0x3e; // ASCII '>'
const NL: u8    = 0x0a; // ASCII '\n'

#[derive(Debug)]
pub enum WriterError {
    IOError(io::Error),
    UTF8Error(::std::str::Utf8Error),
    ParseError(ParseIntError)
}

impl From<ParseIntError> for WriterError {
    fn from(err: ParseIntError) -> WriterError {
        WriterError::ParseError(err)
    }
}

impl From<Utf8Error> for WriterError {
    fn from(err: Utf8Error) -> WriterError {
        WriterError::UTF8Error(err)
    }
}

impl From<io::Error> for WriterError {
    fn from(err: io::Error) -> WriterError {
        WriterError::IOError(err)
    }
}

impl error::Error for WriterError {
    fn description(&self) -> &str {
        match *self {
            WriterError::IOError(ref err) => err.description(),
            WriterError::UTF8Error(ref err) => err.description(),
            WriterError::ParseError(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            WriterError::IOError(ref err) => Some(err),
            WriterError::UTF8Error(ref err) => Some(err),
            WriterError::ParseError(ref err) => Some(err),
        }
    }
}

impl fmt::Display for WriterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            WriterError::IOError(ref err) => err.fmt(f),
            WriterError::UTF8Error(ref err) => err.fmt(f),
            WriterError::ParseError(ref err) => err.fmt(f),
        }
    }
}

struct Parser {
    eof: bool,
}

fn parse_digits(buf: &[u8]) -> Result<usize, WriterError> {
    str::from_utf8(&buf)
        .map_err(|err| WriterError::UTF8Error(err))
        .and_then(|str|
            str.parse::<usize>()
                .map_err(|err| WriterError::ParseError(err))
        )
}

const ARROW_BYTES: &[u8; 2] = b"->";

impl Parser {
    // format: +1,3:a->xyz\n
    fn read_begin<T: Read>(&mut self, input: &mut BufReader<T>) -> Result<Option<()>, WriterError> {
        let mut buf: [u8; 1] = [0; 1];

        // consume a '+'
        input.read_exact(&mut buf)?;

        match buf[0] {
            PLUS => Ok(Some(())),
            NL   => Ok(None),
            wat  => panic!("encountered unexpected char: {}", wat),
        }
    }

    fn read_sizes<T: Read>(&mut self, input: &mut BufReader<T>) -> Result<KVSizes, WriterError> {
        let mut buf: Vec<u8> = Vec::new();

        let r = input.read_until(COMMA, &mut buf)?;

        assert!(r > 0);
        assert_eq!(COMMA, buf[buf.len()-1]);
        buf.pop(); // trim the comma off the end

        let k = parse_digits(&buf)?;
        buf.clear();

        let r = input.read_until(COLON, &mut buf)?;

        assert!(r > 0);
        assert_eq!(COLON, buf.pop().unwrap());
        let v = parse_digits(&buf)?;

        Ok(KVSizes(k, v))
    }

    fn read_kv<T: Read>(&mut self, input: &mut BufReader<T>, kvs: &KVSizes) -> Result<KV, WriterError> {
        let KVSizes(ksize, vsize) = kvs;

        let mut kbytes = Vec::with_capacity(*ksize);
        input.read_exact(&mut kbytes)?;

        // consume the "->" between k and v
        let mut arrowbytes: [u8; 2] = [0; 2];
        input.read_exact(&mut arrowbytes)?;
        assert_eq!(arrowbytes, *ARROW_BYTES);

        let mut vbytes = Vec::with_capacity(*vsize);
        input.read_exact(&mut vbytes)?;

        Ok(KV { k: Bytes::from(kbytes), v: Bytes::from(vbytes) })
    }

    fn read_one_record<T: Read>(&mut self, input: &mut BufReader<T>) -> Result<Option<KV>, WriterError> {
        if self.eof {
            Ok(None)
        } else {
            match self.read_begin(input)? {
                None => {
                    self.eof = true;
                    Ok(None)
                },
                Some(_) =>
                    self.read_sizes(input)
                        .and_then(|sizes| self.read_kv(input, &sizes))
                        .map(|kv| Some(kv))
            }
        }
    }

    pub fn iter<'a, T: Read + 'a>(&'a mut self, buf: &'a mut BufReader<T>) -> IterParser<'a, T> {
        IterParser{parser: self, buf}
    }

    pub fn new() -> Parser { Parser{eof: false} }
}

struct IterParser<'a, T: Read + 'a> {
    parser: &'a mut Parser,
    buf: &'a mut BufReader<T>
}

impl<'a, T: Read + 'a> Iterator for IterParser<'a, T> {
    type Item = Result<KV, WriterError>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        match self.parser.read_one_record(self.buf) {
            Ok(Some(kv)) => Some(Ok(kv)),
            Ok(None)     => None,
            Err(err)     => Some(Err(err)),
        }
    }
}


// expects input in CDB format '+ks,vs:k->v\n'
pub fn write(input: &mut File) {
    let buf = BufReader::new(input);
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_read_one_record() {
        let buf = Bytes::from("+3,4:cat->ball\n\n").into_buf();

        let reader = buf.reader();

        let mut br = BufReader::new(reader);
        let mut parser = Parser::new();

        let recs: Vec<Result<KV, WriterError>> = parser.iter(&mut br).collect();
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
