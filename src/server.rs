use log::{debug, error};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::common::{Request, Response};
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

    pub async fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        debug!("server bind success");

        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            debug!("client {} connection ......", addr);

            let engine_clone = self.engine.clone();
            tokio::spawn(async move {
                if let Err(e) = serve(stream, engine_clone).await {
                    error!("Error on serving client: {}", e);
                }
            });
        }
    }
}

async fn serve<E: KvsEngine>(tcp: TcpStream, engine: E) -> Result<()> {
    use futures_util::{SinkExt, TryStreamExt};
    use tokio_serde::formats::*;
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    let length_delimited = Framed::new(tcp, LengthDelimitedCodec::new());
    let mut stream: tokio_serde::Framed<
        Framed<TcpStream, LengthDelimitedCodec>,
        Request,
        Response,
        Json<Request, Response>,
    > = tokio_serde::Framed::new(length_delimited, Json::<Request, Response>::default());
    if let Some(result) = stream.try_next().await? {
        match result {
            Request::Get { key } => {
                let resp = match engine.get(key) {
                    Result::Ok(v) => Response::Get(v),
                    Result::Err(e) => Response::Err(format!("{}", e)),
                };
                stream.send(resp).await?;
                stream.flush().await?
            }
            Request::Set { key, value } => {
                let resp = match engine.set(key, value) {
                    Result::Ok(()) => Response::Set,
                    Result::Err(e) => Response::Err(format!("{}", e)),
                };
                stream.send(resp).await?;
                stream.flush().await?
            }
            Request::Remove { key } => {
                let resp = match engine.remove(key) {
                    Result::Ok(()) => Response::Remove,
                    Result::Err(e) => Response::Err(format!("{}", e)),
                };
                stream.send(resp).await?;
                stream.flush().await?
            }
        }
    }

    Ok(())
}
