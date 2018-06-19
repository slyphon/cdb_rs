use bytes::{Buf, Bytes, BytesMut};
use crypto::digest::Digest;
use crypto::md5::Md5;
use memmap::{Mmap, MmapOptions};
use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::io::{Cursor, Read, SeekFrom};
use std::io::prelude::*;
use std::ops::Range;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use super::Result;

pub enum SliceFactory<'a> {
    HeapStorage(Bytes),
    MmapStorage(&'a Mmap),
    StdioStorage(FileWrap),
}

const BUF_LEN: usize = 8192;

impl<'a> SliceFactory<'a> {
    pub fn load(mut f: File) -> Result<SliceFactory<'a>> {
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;

        Ok(SliceFactory::HeapStorage(Bytes::from(buffer)))
    }

    pub fn make_map(path: &str) -> Result<Mmap> {
        let f = File::open(path)?;
        let mmap: Mmap = unsafe { MmapOptions::new().map_private(&f)? };

        let mut buf = [0u8; BUF_LEN];
        let mut count = 0;
        let mut md5 = Md5::new();

        debug!("begin pretouch pages");
        {
            let mut cur = Cursor::new(&mmap[..]);
            loop {
                let remain = cur.remaining();
                if remain < BUF_LEN {
                    let mut buf = Vec::with_capacity(remain);
                    cur.copy_to_slice(&mut buf[..]);
                    count += buf.len();
                    md5.input(&buf);
                    break
                } else {
                    cur.copy_to_slice(&mut buf);
                    count += BUF_LEN;
                    md5.input(&buf);
                }
            }
        }
        debug!("end pretouch pages: {} bytes, md5: {}", count, md5.result_str());

        Ok(mmap)
    }

    pub fn make_filewrap(path: &str) -> Result<SliceFactory<'a>> {
        let f = File::open(path)?;
        Ok(SliceFactory::StdioStorage(FileWrap::new(f)))
    }

    pub fn slice(&self, rng: Range<usize>) -> Result<Bytes> {
        if rng.end == rng.start {
            return Ok(Bytes::new());
        }

        let range_len = rng.start - rng.end;
        
        match self {
            SliceFactory::HeapStorage(bytes) => Ok(Bytes::from(&bytes[rng])),
            SliceFactory::MmapStorage(mmap) => {
                let mut v = BytesMut::with_capacity(range_len);
                v.extend_from_slice(&mmap[rng]);
                Ok(Bytes::from(v))
            },
            SliceFactory::StdioStorage(filewrap) => filewrap.slice(rng),
        }
    }
}

impl<'a> Clone for SliceFactory<'a> {
    fn clone(&self) -> Self {
        match self {
            SliceFactory::HeapStorage(bytes) => SliceFactory::HeapStorage(bytes.clone()),
            SliceFactory::MmapStorage(mmap) => SliceFactory::MmapStorage(mmap),
            SliceFactory::StdioStorage(fw) => SliceFactory::StdioStorage(fw.clone()),
        }
    }
}

pub struct FileWrap {
    file: RefCell<File>
}

impl FileWrap {
    fn new(f: File) -> Self {
        FileWrap{file: RefCell::new(f)}
    }

    fn slice(&self, rng: Range<usize>) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(rng.end - rng.start);
        {
            let mut fp = self.file.borrow_mut();
            fp.seek(SeekFrom::Start(rng.start as u64))?;
            fp.read(&mut buf)?;
        }
        Ok(buf.freeze())
    }

    fn dup(&self) -> Result<File> {
        let rawfd = self.as_raw_fd();
        let fd2 =
            match unsafe { ::libc::dup(rawfd) } {
                -1 => return Err(io::Error::last_os_error().into()),
                fd => fd
            };

        Ok(unsafe{File::from_raw_fd(fd2)})
    }
}

impl AsRawFd for FileWrap {
    fn as_raw_fd(&self) -> RawFd {
        let fp = self.file.borrow_mut();
        fp.as_raw_fd()
    }
}

impl Clone for FileWrap {
    fn clone(&self) -> Self {
        // NOTE: this is "a bit shit" as yschimke would say
        // Clone should probably never panic (?), but since
        // dup() returns a Result, we unwrap it here. this whole
        // API should likely be changed to avoid this
        let f = self.dup().unwrap();
        FileWrap::new(f)
    }
}

pub trait Sliceable {
    fn slice(&self, start: usize, end: usize) -> Result<Bytes>;
}
