use bytes::{Bytes, Buf, IntoBuf};
use std::fs::File;
use std::io;
use std::io::prelude::*;

pub const STARTING_HASH: u32 = 5381;
const MAIN_TABLE_SIZE: usize = 256;
const MAIN_TABLE_SIZE_BYTES: usize = 2048;
const END_TABLE_ENTRY_SIZE: usize = 8;
const DATA_HEADER_SIZE: usize = 8;

pub fn djb_hash(bytes: &[u8]) -> usize {
    let mut h = STARTING_HASH;
    for b in bytes {
        h = ((h << 5) + h) ^ ((*b as u32) & 0xffffffff)
    }
    h as usize
 }

#[derive(Copy,Clone,Debug)]
struct HashPair {
    hash: u32,
    ptr: usize,
}

#[derive(Copy,Clone,Debug)]
struct TableRec {
    ptr: usize,
    num_ents: usize,
}

impl TableRec {
    const EMPTY: TableRec = TableRec{ptr: 0, num_ents: 0};
}

struct KVLen {
    k: usize,
    v: usize,
}

struct KV {
    k: Bytes,
    v: Bytes,
}


pub struct CDB {
    main_table: [TableRec; MAIN_TABLE_SIZE],
    data: Bytes,
}

impl CDB {
    fn load_main_table(b: Bytes) -> [TableRec; MAIN_TABLE_SIZE] {
        let mut buf = b.into_buf();

        if buf.remaining() != MAIN_TABLE_SIZE_BYTES {
            panic!(
                "buf was not the right size, expected {} got {}",
                MAIN_TABLE_SIZE_BYTES,
                buf.remaining(),
            );
        }

        let mut table: [TableRec; MAIN_TABLE_SIZE] = [TableRec::EMPTY; MAIN_TABLE_SIZE];

        for i in 0..MAIN_TABLE_SIZE {
            table[i] = TableRec{ptr: buf.get_u32_le() as usize, num_ents: buf.get_u32_le() as usize};
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

    fn hash_pair_at(&self, pos: usize) -> HashPair {
        if pos < MAIN_TABLE_SIZE_BYTES {
            panic!("position {} was in the main table!", pos)
        }

        let mut b = self.data.slice(pos, pos+8).into_buf();

        let hash = b.get_u32_le();
        let ptr = b.get_u32_le() as usize;

        HashPair { hash, ptr }
    }

    fn get_kv_len(&self, posn: usize) -> KVLen {
        let mut b = self.data.slice(posn, posn + DATA_HEADER_SIZE).into_buf();
        let k = b.get_u32_le() as usize;
        let v = b.get_u32_le() as usize;
        KVLen { k, v }
    }

    fn get_kv(&self, hp: &HashPair) -> Option<KV> {
        let kvl = self.get_kv_len(hp.ptr);

        let kstart = hp.ptr + DATA_HEADER_SIZE;
        let vstart = kstart + kvl.k;

        let k = self.data.slice(kstart, kstart + kvl.k);
        let v = self.data.slice(vstart, vstart + kvl.v);

        Some(KV{k,v})
    }

    // read the end table at ptr, entry ent, looking for needle,
    // returns Some(KV) if found
    //
    fn get_kv_ent(&self, ptr: usize, ent: usize, needle: u32) -> Option<KV> {
        let hp = self.hash_pair_at(ptr + (ent * END_TABLE_ENTRY_SIZE));

        match hp.hash {
            n if n == needle => self.get_kv(&hp),
            _ => None,
        }
    }


    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let kb = key.as_bytes();
        let hash = djb_hash(kb);
        let rec = self.main_table[hash%256];

        if rec.num_ents == 0 {
            return None
        }

        let start_ent = (hash >> 8) % rec.num_ents;

        let find = |ent: usize| -> Option<Vec<u8>> {
            match self.get_kv_ent(rec.ptr, ent, hash as u32) {
                Some(ref kv) if kv.k == kb => return Some(kv.k.to_vec()),
                _ => None,
            }
        };

        for ent in start_ent..rec.num_ents {
            match find(ent) {
                Some(kv) => return Some(kv),
                None => continue
            }
        }

        for ent in 0..start_ent {
            match find(ent) {
                Some(kv) => return Some(kv),
                None => continue
            }
        }

        None
    }
}