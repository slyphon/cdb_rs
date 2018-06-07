use bytes::{Bytes, Buf, IntoBuf};
use std::fs::File;
use std::io;
use std::io::prelude::*;

pub const STARTING_HASH: u32 = 5381;
const MAIN_TABLE_SIZE: usize = 256;
const MAIN_TABLE_SIZE_BYTES: usize = 2048;

pub fn djb_hash(bytes: &[u8]) -> u32 {
    let mut h = STARTING_HASH;
    for b in bytes {
        h = ((h << 5) + h) ^ ((*b as u32) & 0xffffffff)
    }
    h
 }

#[derive(Debug)]
struct HashPair {
    hash: u32,
    ptr: u32,
}

#[derive(Debug)]
struct TableRec {
    ptr: u32,
    num_ents: u32,
}

pub struct CDB {
    main_table: [u32; MAIN_TABLE_SIZE],
    data: Bytes,
}

impl CDB {
    fn load_main_table(b: Bytes) -> [u32; MAIN_TABLE_SIZE] {
        let mut buf = b.into_buf();

        if buf.remaining() != MAIN_TABLE_SIZE_BYTES {
            panic!(
                "buf was not the right size, expected {} got {}",
                MAIN_TABLE_SIZE_BYTES,
                buf.remaining(),
            );
        }

        let mut table: [u32; MAIN_TABLE_SIZE] = [0; MAIN_TABLE_SIZE];

        for i in 0..MAIN_TABLE_SIZE {
            table[i] = buf.get_u32_le();
        }

        table
    }

    pub fn load(path: &str) -> io::Result<CDB> {
        let mut f = File::open(path)?;
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;

        let bytes = Bytes::from(buffer);
        let x = bytes.slice_to(MAIN_TABLE_SIZE_BYTES).clone();

        Ok(CDB { main_table: CDB::load_main_table(x), data: bytes })
    }
}
