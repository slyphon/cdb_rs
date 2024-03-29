pub mod randoread;
pub mod writer;
pub mod input;
pub mod errors;

use bytes::{Bytes, Buf, IntoBuf};
use std::fs::File;
use std::io;
use std::io::prelude::*;
use rand::{thread_rng, Rng};

pub const STARTING_HASH: u32 = 5381;
const MAIN_TABLE_SIZE: usize = 256;
const MAIN_TABLE_SIZE_BYTES: usize = 2048;
const END_TABLE_ENTRY_SIZE: usize = 8;
const DATA_HEADER_SIZE: usize = 8;

pub fn djb_hash(bytes: &[u8]) -> usize {
    let mut h = STARTING_HASH;

    for b in bytes {
        // wrapping here is explicitly for allowing overflow semantics:
        //
        //   Operations like + on u32 values is intended to never overflow,
        //   and in some debug configurations overflow is detected and results in a panic.
        //   While most arithmetic falls into this category, some code explicitly expects
        //   and relies upon modular arithmetic (e.g., hashing)
        //
        h = h.wrapping_shl(5).wrapping_add(h) ^ (*b as u32)
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

struct KVLen {
    k: usize,
    v: usize,
}

pub struct KV {
    k: Bytes,
    v: Bytes,
}

impl KV {
    fn dump(&self, w: &mut impl io::Write) -> io::Result<()> {
        write!(w, "+{},{}:", self.k.len(), self.v.len())?;
        w.write(self.k.as_ref())?;
        write!(w, "->")?;
        w.write(self.v.as_ref())?;
        write!(w, "\n")
    }
}

pub struct CDB {
    main_table: Vec<TableRec>,
    data: Bytes,
}

impl CDB {
    fn load_main_table(b: Bytes) -> Vec<TableRec> {
        let mut buf = b.into_buf();

        if buf.remaining() != MAIN_TABLE_SIZE_BYTES {
            panic!(
                "buf was not the right size, expected {} got {}",
                MAIN_TABLE_SIZE_BYTES,
                buf.remaining(),
            );
        }

        let mut table: Vec<TableRec> = Vec::new();

        for _ in 0..MAIN_TABLE_SIZE {
            table.push(TableRec{ptr: buf.get_u32_le() as usize, num_ents: buf.get_u32_le() as usize});
        }

        table.shrink_to_fit();

        eprintln!("table loaded");

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

    fn hash_pair_at(&self, pos: usize) -> Option<HashPair> {
        if pos < MAIN_TABLE_SIZE_BYTES {
            panic!("position {} was in the main table!", pos)
        }

        let mut b = self.data.slice(pos, pos+8).into_buf();
        let hash = b.get_u32_le();
        let ptr = b.get_u32_le() as usize;

        if ptr == 0 {
            None
        } else {
            Some(HashPair { hash, ptr })
        }
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
        match self.hash_pair_at(ptr + (ent * END_TABLE_ENTRY_SIZE)) {
            Some(ref hp) if hp.hash == needle => self.get_kv(hp),
            _ => None,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let hash = djb_hash(key);
        let rec = self.main_table[hash%256];

        if rec.num_ents == 0 {
            return None
        }

        let start_ent = (hash >> 8) % rec.num_ents;

        let rng_a = start_ent..rec.num_ents;
        let rng_b = 0..start_ent;

        rng_a.chain(rng_b)
            .filter_map(|ent| {
                self.get_kv_ent(rec.ptr, ent, hash as u32)
                    .iter()
                    .find(|ref kv| kv.k == key)
                    .map(|ref kv| kv.v.to_owned())
            })
            .nth(0)
    }

    // take a main_table record and return a vector of offsets to valid secondary table
    // entries.
    fn expand_table_rec_to_offsets(t_rec: &TableRec) -> Vec<usize> {
        let rng = 0..t_rec.num_ents;
        let offsets: Vec<usize> = rng.map({|j| t_rec.ptr + (j * END_TABLE_ENTRY_SIZE) }).collect();
        offsets
    }

    // read through the main table and return a vector of offsets into the secondary table
    fn end_table_entry_offsets(&self) -> Vec<usize> {
       self.main_table
           .iter()
           .flat_map(|t_rec| { CDB::expand_table_rec_to_offsets(t_rec) })
           .collect()
    }

    fn hash_pairs(&self) -> Vec<HashPair> {
        self.end_table_entry_offsets()
            .iter()
            .filter_map(|offset| self.hash_pair_at(*offset) )
            .collect()
    }

    fn kvs(&self) -> Vec<KV> {
        self.hash_pairs().iter()
            .filter_map(|hp| self.get_kv(&hp) )
            .collect()
    }

    fn kvs_iter<'a>(&'a self) -> Box<Iterator<Item=KV> + 'a> {
        Box::new(
            self.main_table.iter()
                .flat_map(|t_rec| { CDB::expand_table_rec_to_offsets(t_rec) })
                .filter_map(move |offset| self.hash_pair_at(offset))
                .filter_map(move |hp| self.get_kv(&hp))
        )
    }

    pub fn keys(&self) -> Vec<Bytes> {
        self.hash_pairs().iter()
            .filter_map(|hp| self.get_kv(&hp))
            .map(|kv| kv.k)
            .collect()
    }

    pub fn dump(&self, w: &mut impl io::Write) -> io::Result<()> {
        for kv in self.kvs_iter() {
            match kv.dump(w) {
                Err(err) => return Err(err),
                _ => continue,
            }
        }

        write!(w, "\n")?;       // need a trailing newline
        Ok(())
    }
}
