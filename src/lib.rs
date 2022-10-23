//! A lightweight asynchronous [smux](https://github.com/xtaci/smux) (Simple MUltipleXing) library for smol, async-std and any async runtime compatible to `futures`.

pub mod error;
pub(crate) mod frame;
pub(crate) mod mux;

pub use mux::{mux_connection, MuxAcceptor, MuxConnector, MuxStream};
