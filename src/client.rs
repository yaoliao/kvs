use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

use log::{debug, error, info, warn};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};

use crate::error::{Result, KvsError};

use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};

pub struct KvsClient {
    writer: BufWriter<TcpStream>,
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
}

impl KvsClient {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        debug!("connection to server success");

        Ok(KvsClient {
            writer: BufWriter::new(stream.try_clone()?),
            reader: Deserializer::new(IoRead::new(BufReader::new(stream))),
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        debug!("client get key:{}", key);

        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;

        let resp = GetResponse::deserialize(&mut self.reader)?;
        match resp {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(KvsError::StringError(msg))
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        debug!("client set key:{} value:{}", key, value);

        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;

        let resp = SetResponse::deserialize(&mut self.reader)?;
        match resp {
            SetResponse::Ok(()) => Ok(()),
            SetResponse::Err(msg) => Err(KvsError::StringError(msg))
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        debug!("client remove key:{}", key);

        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;

        let resp = RemoveResponse::deserialize(&mut self.reader)?;
        match resp {
            RemoveResponse::Ok(()) => Ok(()),
            RemoveResponse::Err(msg) => Err(KvsError::StringError(msg))
        }
    }
}