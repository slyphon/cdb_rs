use bytes::{Buf, Bytes, BytesMut};
use crypto::digest::Digest;
use crypto::md5::Md5;
use memmap::{Mmap, MmapOptions};
use std::cell::RefCell;
use std::fs::File;
use std::io::{Cursor, Read};
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::fs::FileExt;
use super::Result;

pub enum SliceFactory<'a> {
    HeapStorage(Bytes),
    MmapStorage(&'a Mmap),
    StdioStorage(FileWrap),
}

const BUF_LEN: usize = 8192;

pub fn readybuf(size: usize) -> BytesMut {
    let mut b = BytesMut::with_capacity(size);
    b.resize(size, 0);
    b
}

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
                    let mut buf = readybuf(remain);
                    cur.copy_to_slice(&mut buf[..]);
                    count += buf.len();
                    md5.input(&buf);
                    break;
                } else {
                    cur.copy_to_slice(&mut buf);
                    count += BUF_LEN;
                    md5.input(&buf);
                }
            }
        }
        debug!(
            "end pretouch pages: {} bytes, md5: {}",
            count,
            md5.result_str()
        );

        Ok(mmap)
    }

    pub fn make_filewrap(f: File) -> Result<SliceFactory<'a>> {
        Ok(SliceFactory::StdioStorage(FileWrap::new(f)))
    }

    pub fn slice(&self, start: usize, end: usize) -> Result<Bytes> {
        assert!(end >= start);

        if end == start {
            return Ok(Bytes::new());
        }

        let range_len = end - start;

        match self {
            SliceFactory::HeapStorage(bytes) => Ok(Bytes::from(&bytes[start..end])),
            SliceFactory::MmapStorage(mmap) => {
                let mut v = Vec::with_capacity(range_len);
                v.extend_from_slice(&mmap[start..end]);
                Ok(Bytes::from(v))
            }
            SliceFactory::StdioStorage(filewrap) => filewrap.slice(start, end),
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
    inner: RefCell<File>,
}

impl FileWrap {
    fn new(f: File) -> Self {
        FileWrap {
            inner: RefCell::new(f),
        }
    }

    fn slice(&self, start: usize, end: usize) -> Result<Bytes> {
        assert!(end >= start);
        let mut buf = readybuf(end - start);
        {
            let fp = self.inner.borrow_mut();
            fp.read_at(&mut buf, start as u64)?;
            trace!("read: {:?}", buf);
        }
        Ok(Bytes::from(buf))
    }

    #[cfg(test)]
    fn temp() -> Result<FileWrap> {
        use tempfile;
        let tmp: File = tempfile::tempfile()?;
        let fw = FileWrap::new(tmp);
        Ok(fw)
    }
}

impl AsRawFd for FileWrap {
    fn as_raw_fd(&self) -> RawFd {
        let fp = self.inner.borrow_mut();
        fp.as_raw_fd()
    }
}

impl Clone for FileWrap {
    fn clone(&self) -> Self {
        let f = self.inner.borrow_mut();
        FileWrap::new(f.try_clone().unwrap())
    }
}

pub trait Sliceable {
    fn slice(&self, start: usize, end: usize) -> Result<Bytes>;
}

struct BMString(BytesMut);

impl ToString for BMString {
    fn to_string(&self) -> String {
       String::from(self)
    }
}

impl<'a> From<&'a BMString> for String {
    fn from(bm: &'a BMString) -> Self {
        String::from_utf8(bm.0.to_vec()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use super::*;
    use tempfile;
    use std::io::prelude::*;

    fn assert_ok<T>(f: T)
    where
        T: Fn() -> Result<()>,
    {
        f().unwrap()
    }


    #[test]
    fn basic_file_io_sanity() {
        assert_ok(|| {
            let mut tmp: File = tempfile::tempfile()?;

            tmp.write_all("abcdefghijklmnopqrstuvwxyz".as_bytes())?;
            tmp.sync_all()?;

            let mut buf = BytesMut::with_capacity(3);
            buf.resize(3, 0);
            let n = tmp.read_at(&mut buf, 23)?;
            assert_eq!(n, 3);
            assert_eq!(&buf[..], "xyz".as_bytes());
            Ok(())
        })
    }

    #[test]
    fn file_wrap_slice_test() {
        assert_ok(||{
            let fw = FileWrap::temp()?;

            {
                let mut f = fw.inner.borrow_mut();
                f.write_all("abcdefghijklmnopqrstuvwxyz".as_bytes())?;
                f.sync_all()?;
            }

            assert_eq!(fw.slice(3, 5)?, "de".as_bytes());
            Ok(())
        })
    }
}


