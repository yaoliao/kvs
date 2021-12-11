use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use log::{debug, error};
use serde_json::Deserializer;

use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::engines::KvsEngine;
use crate::error::Result;
use crate::thread_pool::ThreadPool;

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer { engine, pool }
    }

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        debug!("server bind success");

        for stream in listener.incoming() {
            let engine_clone = self.engine.clone();
            self.pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = serve(stream, engine_clone) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            });
        }
        Ok(())
    }
}

fn serve<E: KvsEngine>(tcp: TcpStream, engine: E) -> Result<()> {
    let peer_addr = tcp.peer_addr()?;
    let reader = BufReader::new(&tcp);
    let mut writer = BufWriter::new(&tcp);

    let req = Deserializer::from_reader(reader).into_iter::<Request>();

    for result in req {
        let result = result?;
        debug!("Receive request from {}: {:?}", peer_addr, result);
        match result {
            Request::Get { key } => {
                let resp = match engine.get(key) {
                    Result::Ok(v) => GetResponse::Ok(v),
                    Result::Err(e) => GetResponse::Err(format!("{}", e)),
                };
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
            }
            Request::Set { key, value } => {
                let resp = match engine.set(key, value) {
                    Result::Ok(()) => SetResponse::Ok(()),
                    Result::Err(e) => SetResponse::Err(format!("{}", e)),
                };
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
            }
            Request::Remove { key } => {
                let resp = match engine.remove(key) {
                    Result::Ok(v) => RemoveResponse::Ok(v),
                    Result::Err(e) => RemoveResponse::Err(format!("{}", e)),
                };
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
            }
        }
    }

    Ok(())
}
