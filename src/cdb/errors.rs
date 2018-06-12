use std::error;
use std::fmt;
use std::io;
use std::num::ParseIntError;
use std::str::Utf8Error;

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
