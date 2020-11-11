//! A lightweight asynchronous [smux](https://github.com/xtaci/smux) (Simple MUltipleXing) library for smol, async-std and any async runtime compatible to `futures`.

pub(crate) mod config;
pub(crate) mod error;
pub(crate) mod frame;
pub(crate) mod mux;

pub use config::MuxConfig;
pub use error::{Error, Result};
pub use mux::Mux;
pub use mux::MuxStream;
