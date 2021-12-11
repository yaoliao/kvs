#[macro_use]
extern crate failure;

pub use crate::engines::KvStore;
pub use crate::engines::KvsEngine;
pub use crate::log::KvsLog;
pub use client::KvsClient;
pub use engines::SledKvsEngine;
pub use error::KvsError;
pub use error::Result;
pub use server::KvsServer;

// #![deny(missing_docs)]
mod client;
mod common;
pub mod engines;
mod error;
mod log;
pub mod server;
pub mod thread_pool;
