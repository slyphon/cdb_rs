use bytes::{Buf,Bytes};
use memmap::{Mmap, MmapOptions};
use std::fs::File;
use std::io;
use std::io::{Read,Cursor};


pub enum SliceFactory<'a> {
    HeapStorage(Bytes),
    MmapStorage(&'a Mmap),
}

const BUF_LEN: usize = 8192;

impl<'a> SliceFactory<'a> {
    pub fn load(mut f: File) -> io::Result<SliceFactory<'a>> {
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;

        Ok(SliceFactory::HeapStorage(Bytes::from(buffer)))
    }

    pub fn make_map(path: &str) -> io::Result<Mmap> {
        let f = File::open(path)?;
        let mmap: Mmap = unsafe { MmapOptions::new().map_private(&f)? };

        let mut buf = [0u8; BUF_LEN];
        let mut count = 0;

        debug!("begin pretouch pages");
        {
            let mut cur = Cursor::new(&mmap[..]);
            loop {
                let remain = cur.remaining();
                if remain < BUF_LEN {
                    let mut buf = Vec::with_capacity(remain);
                    cur.copy_to_slice(&mut buf[..]);
                    count += buf.len();
                    break
                } else {
                    cur.copy_to_slice(&mut buf);
                    count += BUF_LEN;
                }
            }
        }
        debug!("end pretouch pages: {} bytes", count);

        Ok(mmap)
    }

    pub fn slice(&self, start: usize, end: usize) -> Bytes {
        assert!(end >= start);
        if end == start {
            return Bytes::new();
        }
        
        match self {
            SliceFactory::HeapStorage(bytes) => bytes.slice(start, end),
            SliceFactory::MmapStorage(mmap) => {
                let mut v = Vec::with_capacity(end - start);
                v.extend_from_slice(&mmap[start..end]);
                Bytes::from(v)
            },
        }
    }

}

impl<'a> Clone for SliceFactory<'a> {
    fn clone(&self) -> Self {
        match self {
            SliceFactory::HeapStorage(bytes) => SliceFactory::HeapStorage(bytes.clone()),
            SliceFactory::MmapStorage(mmap) => SliceFactory::MmapStorage(mmap)
        }
    }
}
