use bytes::{Buf, Bytes, IntoBuf};
use std::fmt;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::sync::Arc;

pub mod randoread;
pub mod errors;
pub mod input;
pub mod writer;

pub const STARTING_HASH: u32 = 5381;
const MAIN_TABLE_SIZE: usize = 256;
const MAIN_TABLE_SIZE_BYTES: usize = 2048;
const END_TABLE_ENTRY_SIZE: usize = 8;
const DATA_HEADER_SIZE: usize = 8;

// idea from https://raw.githubusercontent.com/jothan/cordoba/master/src/lib.rs
#[derive(Copy, Clone, Eq, PartialEq)]
struct CDBHash(u32);

impl CDBHash {
    fn new(d: &[u8]) -> Self {
        let h = d.iter().fold(STARTING_HASH, |h, &c| {
            (h << 5).wrapping_add(h) ^ u32::from(c)
        });
        CDBHash(h)
    }

    fn table(&self) -> usize {
        self.0 as usize % MAIN_TABLE_SIZE
    }

    fn slot(&self, num_ents: usize) -> usize {
        (self.0 as usize >> 8) % num_ents
    }
}

impl fmt::Debug for CDBHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CDBHash(0x{:08x})", self.0)
    }
}

impl<'a> From<&'a CDBHash> for u32 {
    fn from(h: &'a CDBHash) -> Self {
        h.0
    }
}

#[derive(Copy, Clone)]
struct Bucket {
    ptr: usize,
    num_ents: usize,
}

impl fmt::Debug for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "TableRec {{ ptr: {:>#010x}, num_ents: {:>#010x} }}",
            self.ptr, self.num_ents
        )
    }
}

impl Bucket {
    fn iter<'a>(&'a self, start: usize) -> Box<Iterator<Item=IndexEntryPos> + 'a> {
        Box::new(
            (0..self.num_ents).map(move |i| self.entry_n_pos((i + start) % self.num_ents) )
        )
    }

    // returns the offset into the db of entry n of this bucket.
    // panics if n >= num_ents
    fn entry_n_pos<'a>(&'a self, n: usize) -> IndexEntryPos {
        assert!(n < self.num_ents);
        IndexEntryPos(self.ptr + (n * END_TABLE_ENTRY_SIZE))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct IndexEntryPos(usize);

impl From<IndexEntryPos> for usize {
    fn from(n: IndexEntryPos) -> Self {
        n.0
    }
}

#[derive(Debug)]
pub struct KV {
    k: Bytes,
    v: Bytes,
}

impl KV {
    #[allow(dead_code)]
    fn dump(&self, w: &mut impl io::Write) -> io::Result<()> {
        write!(w, "+{},{}:", self.k.len(), self.v.len())?;
        w.write(self.k.as_ref())?;
        write!(w, "->")?;
        w.write(self.v.as_ref())?;
        write!(w, "\n")
    }
}

type BucketsTable = [Bucket; MAIN_TABLE_SIZE];

pub struct CDB {
    main_table: BucketsTable,
    data: Bytes,
}

struct IndexEntry {
    hash: CDBHash, // the hash of the stored key
    ptr: usize,    // pointer to the absolute position of the data in the db
    _pos: usize,   // the position of this hash pair in the db (mostly for debugging)
}

impl CDB {
    fn load_main_table(b: Bytes) -> BucketsTable {
        let mut buf = b.into_buf();

        if buf.remaining() != MAIN_TABLE_SIZE_BYTES {
            panic!(
                "buf was not the right size, expected {} got {}",
                MAIN_TABLE_SIZE_BYTES,
                buf.remaining(),
            );
        }

        let mut table = [Bucket {
            ptr: 0,
            num_ents: 0,
        }; MAIN_TABLE_SIZE];

        for i in 0..MAIN_TABLE_SIZE {
            table[i].ptr = buf.get_u32_le() as usize;
            table[i].num_ents = buf.get_u32_le() as usize;
        }

        debug!("table loaded");

        table
    }

    pub fn load(path: &str) -> io::Result<CDB> {
        let mut f = File::open(path)?;
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;

        let bytes = Bytes::from(buffer);
        let x = bytes.slice_to(MAIN_TABLE_SIZE_BYTES).clone();

        Ok(CDB {
            main_table: CDB::load_main_table(x),
            data: bytes,
        })
    }

    // returns the index entry at absolute position 'pos' in the db
    fn index_entry_at(&self, pos: IndexEntryPos) -> IndexEntry {
        let pos: usize = pos.into();

        if pos < MAIN_TABLE_SIZE_BYTES {
            panic!("position {:?} was in the main table!", pos)
        }

        let mut b = self.data.slice(pos, pos + 8).into_buf();
        let hash = CDBHash(b.get_u32_le());
        let ptr = b.get_u32_le() as usize;

        IndexEntry {
            hash,
            ptr,
            _pos: pos,
        }
    }

    fn get_kv(&self, ie: &IndexEntry) -> KV {
        let mut b = self.data.slice(ie.ptr, ie.ptr + DATA_HEADER_SIZE).into_buf();

        let ksize = b.get_u32_le() as usize;
        let vsize = b.get_u32_le() as usize;

        let kstart = ie.ptr + DATA_HEADER_SIZE;
        let vstart = kstart + ksize;

        let k = self.data.slice(kstart, kstart + ksize);
        let v = self.data.slice(vstart, vstart + vsize);

        KV { k, v }
    }

    pub fn get<'a, S>(&self, key: S) -> Option<Bytes>
        where S: Into<&'a [u8]>
    {
        let key = key.into();
        let hash = CDBHash::new(key);
        let bucket = self.main_table[hash.table()];

        if bucket.num_ents == 0 {
            trace!("bucket empty, returning none");
            return None;
        }

        for index_entry_pos in bucket.iter(hash.slot(bucket.num_ents)) {
            let idx_ent = self.index_entry_at(index_entry_pos);
            
            if idx_ent.ptr == 0 {
                return None;
            } else if idx_ent.hash == hash {
                let kv = self.get_kv(&idx_ent);
                if &kv.k[..] == key {
                    return Some(kv.v.clone());
                } else {
                    continue;
                }
            }
        }

        None
    }

    #[allow(dead_code)]
    pub fn kvs_iter<'a>(&'a self) -> Box<Iterator<Item=KV> + 'a> {
        Box::new(
            // fully qualify this call because of https://github.com/rust-lang/rust/issues/48919
            ::itertools::Itertools::flatten(
                self.main_table.iter()
                    .map(move |bucket| bucket.iter(0))
            )
                .map(move |pos| self.get_kv(&self.index_entry_at(pos)))
        )
    }
}

#[cfg(test)]
mod tests {
    use env_logger;
    use std::collections::hash_set;
    use std::fs::remove_file;
    use std::path::PathBuf;
    use super::*;
    use tempfile::NamedTempFile;
    use tinycdb::Cdb as TCDB;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::string;

    fn arb_string_slice<'a>() -> BoxedStrategy<Vec<String>> {
        let st = string::string_regex("[a-z]+").unwrap();
        vec(st, 10..1000).boxed()
    }

    struct QueryResult(String, Option<String>);

    #[allow(dead_code)]
    fn create_temp_cdb(kvs: &[(&str, &str)]) -> io::Result<CDB> {
        let path: PathBuf;

        {
            let ntf = NamedTempFile::new()?;
            remove_file(ntf.path())?;
            path = ntf.path().to_owned();
        }

        let mut dupcheck = hash_set::HashSet::new();

        TCDB::new(path.as_ref(), |c| {
            let ys = kvs.to_owned();
            for (k, v) in ys {
                let kk = k.clone();
                let vv = v.clone();

                if !dupcheck.contains(&k) {
                    dupcheck.insert(k);
                    c.add(kk.as_ref(), vv.as_ref()).unwrap();
                }
            }
        }).unwrap();

        let cdb = CDB::load(path.to_str().unwrap())?;

        Ok(cdb)
    }

    proptest! {
        #[test]
        fn qc_key_and_value_retrieval(ref xs in arb_string_slice()) {
            for QueryResult(q, r) in make_and_then_read(xs){
                prop_assert_eq!(
                    Some(q),
                    r
                );
            }
        }
    }

    type QueryResultIter<'a> = Box<Iterator<Item = QueryResult> + 'a>;

    fn read_keys<'a>(cdb: CDB, xs: &'a Vec<String>) -> QueryResultIter<'a> {
        Box::new(xs.iter().map(move |x| {
            let res = cdb.get(x.as_ref());
            QueryResult(
                x.clone(),
                res.map(|v| String::from_utf8(v.to_vec()).unwrap()),
            )
        }))
    }

    #[allow(dead_code)]
    fn make_temp_cdb_single_vals(xs: &Vec<String>) -> CDB {
        let kvs: Vec<(&str, &str)> = xs.iter().map(|k| (k.as_ref(), k.as_ref())).collect();
        create_temp_cdb(&kvs[..]).unwrap()
    }

    #[allow(dead_code)]
    fn make_and_then_read<'a>(xs: &'a Vec<String>) -> QueryResultIter<'a> {
        read_keys(make_temp_cdb_single_vals(xs), xs)
    }

    #[test]
    fn read_small_list() {
        env_logger::try_init().unwrap();

        let strings = vec![
            "shngcmfkqjtvhnbgfcvbm",
            "qjflpsvacyhsgxykbvarbvmxapufmdt",
            "a",
            "a",
            "a",
            "a",
            "a",
            "a",
            "xfjhaqjkcjiepmcbhopgpxwwth",
            "a",
            "a",
        ];
        let arg = strings.iter().map(|s| (*s).to_owned()).collect();

        let cdb = make_temp_cdb_single_vals(&arg);

        for QueryResult(q, r) in read_keys(cdb, &arg) {
            assert_eq!(Some(q), r);
        }
    }

    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::path::Path;

    #[test]
    fn test_with_dictionary() {
        let mut args: Vec<String> = Vec::new();

        {
            let f = File::open(Path::new("/usr/share/dict/words")).unwrap();
            let bufr = BufReader::new(&f);

            for line in bufr.lines() {
                let word = line.unwrap();
                args.push(word.to_owned());
            }
        }

        let cdb = make_temp_cdb_single_vals(&args);

        for QueryResult(q, r) in read_keys(cdb, &args) {
            assert_eq!(Some(q), r);
        }
    }
}
