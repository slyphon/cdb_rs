use bytes::{Bytes,BytesMut, BufMut};

use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{BufReader,BufWriter};
use std::io::prelude::*;
use std::str;
use super::KV;

pub struct Writer { }

struct KVSizes(usize, usize);

const PLUS: u8  = 0x2b; // ASCII '+'
const COMMA: u8 = 0x2c; // ASCII ','
const COLON: u8 = 0x3a; // ASCII ':'
const DASH: u8  = 0x2a; // ASCII '-'
const GT: u8    = 0x3e; // ASCII '>'

pub enum Cause {
    IOError(io::Error),
    UTF8Error(::std::str::Utf8Error),
    ParseError(::std::num::ParseIntError)
}

#[derive(Debug)]
pub struct WriterError {
    desc: String,
    underlying: Option<Cause>
}

impl Error for WriterError {
    fn description(&self) -> &str { self.desc.as_ref() }

    fn cause(&self) -> Option<&Error> {
        unimplemented!()
    }
}

fn parse_digits(buf: &[u8]) -> Result<usize, Error> {
    str::from_utf8(&buf)
        .map_err(|err| Error::UTF8Error(err))
        .and_then(|str|
            str.parse::<usize>()
                .map_err(|err| Error::ParseError(err))
        )
}


// format: +1,3:a->xyz\n
fn read_sizes<R: Read>(input: &mut BufReader<R>) -> Result<KVSizes, Error> {
    let mut buf: Vec<u8> = Vec::new();

    let wrap = |err| { Error::IOError(err) };

    // consume a '+'
    let r = input.read_until(PLUS, &mut buf).map_err(wrap)?;

    assert_eq!(r, 1);
    assert_eq!(buf[0], PLUS);
    buf.clear();

    let r = input.read_until(COMMA, &mut buf).map_err(wrap)?;

    assert!(r > 0);
    buf.pop(); // trim the comma off the end

    let k = parse_digits(&buf)?;
    buf.clear();

    let r = input.read_until(COLON, &mut buf).map_err(wrap)?;

    assert!(r > 0);
    assert_eq!(COLON, buf.pop().unwrap());
    let v = parse_digits(&buf)?;

    Ok(KVSizes(k, v))
}

fn read_kv<R: Read>(input: &mut BufReader<R>, kvs: &KVSizes) -> Result<KV, Error> {
    let KVSizes(ksize, vsize) = kvs;

    let mut kbytes = Vec::with_capacity(*ksize);
    input.read_exact(&mut kbytes).map_err(|e| Error::IOError(e))?;

    // consume the "->" between k and v
    let mut arrowbytes: [u8; 2] = [0; 2];
    input.read_exact(&mut arrowbytes).map_err()


    let mut vbytes = Vec::with_capacity(*vsize);
    input.read_exact(&mut vbytes).map_err(|e| Error::IOError(e))?;

    Ok(KV{k: Bytes::from(kbytes), v: Bytes::from(vbytes)})
}

fn read_one_record<R: Read>(input: &mut BufReader<R>) -> Result<KV, Error> {
    Ok(KV{k: Bytes::new(), v: Bytes::new()})
}

// expects input in
pub fn write(input: &mut File) {
    let buf = BufReader::new(input);


}

