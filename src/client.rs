use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_serde::formats::*;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::common::{Request, Response};
use crate::KvsError;
use crate::Result;

pub struct KvsClient {
    stream: tokio_serde::Framed<
        Framed<TcpStream, LengthDelimitedCodec>,
        Response,
        Request,
        Json<Response, Request>,
    >,
}

impl KvsClient {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let socket = TcpStream::connect(&addr).await?;

        let length_delimited = Framed::new(socket, LengthDelimitedCodec::new());
        let stream =
            tokio_serde::Framed::new(length_delimited, Json::<Response, Request>::default());

        Ok(KvsClient { stream })
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        debug!("client get key:{}", key);

        self.stream.send(Request::Get { key }).await?;
        self.stream.flush().await?;

        if let Some(msg) = self.stream.try_next().await.unwrap() {
            match msg {
                Response::Get(value) => Ok(value),
                Response::Err(e) => Err(KvsError::StringError(e)),
                _ => Err(KvsError::StringError("Invalid response".to_owned())),
            }
        } else {
            Err(KvsError::StringError("Invalid response".to_owned()))
        }
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        debug!("client set key:{} value:{}", key, value);

        self.stream.send(Request::Set { key, value }).await?;
        self.stream.flush().await?;

        if let Some(msg) = self.stream.try_next().await.unwrap() {
            match msg {
                Response::Set => Ok(()),
                Response::Err(e) => Err(KvsError::StringError(e)),
                _ => Err(KvsError::StringError("Invalid response".to_owned())),
            }
        } else {
            Err(KvsError::StringError("Invalid response".to_owned()))
        }
    }

    pub async fn remove(&mut self, key: String) -> Result<()> {
        debug!("client remove key:{}", key);

        self.stream.send(Request::Remove { key }).await?;
        self.stream.flush().await?;

        if let Some(msg) = self.stream.try_next().await.unwrap() {
            match msg {
                Response::Remove => Ok(()),
                Response::Err(e) => Err(KvsError::StringError(e)),
                _ => Err(KvsError::StringError("Invalid response".to_owned())),
            }
        } else {
            Err(KvsError::StringError("Invalid response".to_owned()))
        }
    }
}
