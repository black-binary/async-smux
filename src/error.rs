use std::fmt::Display;

#[derive(Debug)]
pub enum Error {
    DispatcherClosed,
    StreamClosed,
    IoError(std::io::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            Error::DispatcherClosed => "MuxDispatcher closed".to_string(),
            Error::StreamClosed => "MuxStream closed".to_string(),
            Error::IoError(e) => e.to_string(),
        };
        f.write_str(&msg)?;
        Ok(())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IoError(e)
    }
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::DispatcherClosed => {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "MuxDispatcher closed")
            }
            Error::StreamClosed => {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "MuxDispatcher closed")
            }
            Error::IoError(e) => e,
        }
    }
}

impl Error {
    pub fn stream_closed() -> Self {
        Error::StreamClosed
    }

    pub fn dispatcher_closed() -> Self {
        Error::DispatcherClosed
    }
}

pub type Result<T> = core::result::Result<T, Error>;
