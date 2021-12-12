use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;

use clap::arg_enum;
use log::{debug, error, info, warn};
use structopt::StructOpt;

use kvs::engines::KvsEngine;
use kvs::server::KvsServer;
use kvs::thread_pool::{NaiveThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::Result;
use kvs::SledKvsEngine;
use kvs::{KvStore, KvsLog};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server", about = "server for kvs")]
struct Opt {
    #[structopt(
        long,
        help = "Sets the listening address",
        value_name = "IP:PORT",
        default_value(DEFAULT_LISTENING_ADDRESS),
        parse(try_from_str)
    )]
    addr: SocketAddr,

    #[structopt(
    long,
    help = "Sets the storage engine",
    value_name = "ENGINE-NAME",
    possible_values(& Engine::variants())
    )]
    engine: Option<Engine>,
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum Engine {
        kvs,
        sled
    }
}

#[tokio::main]
async fn main() {
    KvsLog::log_setting();

    let mut opt: Opt = Opt::from_args();
    debug!("opt: {:?}", opt);

    let curr_engine = current_engine().unwrap();
    if opt.engine.is_none() {
        opt.engine = curr_engine;
    }
    if curr_engine.is_some() && opt.engine != curr_engine {
        error!("Wrong engine!");
        exit(1);
    }

    let result = tokio::join!(run(opt));
    if let (Err(e),) = result {
        error!("server error........ {}", e);
    }
}

fn current_engine() -> Result<Option<Engine>> {
    let engine_dir = current_dir()?.join("engine");
    if !engine_dir.exists() {
        return Result::Ok(None);
    }
    match fs::read_to_string(engine_dir)?.parse() {
        Ok(engine) => Result::Ok(Some(engine)),
        Err(e) => {
            debug!("exec current_engine error: {}", e);
            Result::Ok(None)
        }
    }
}

async fn run(opt: Opt) -> Result<()> {
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", opt.addr);

    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;

    match engine {
        Engine::kvs => run_with_engine(KvStore::open(current_dir()?)?, opt.addr).await,
        Engine::sled => {
            run_with_engine(SledKvsEngine::new(sled::open(current_dir()?)?), opt.addr).await
        }
    }
}

async fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let cpus = num_cpus::get();
    info!("cpu num is {}", cpus);
    // let mut server = KvsServer::new(engine, SharedQueueThreadPool::new(cpus as u32)?);
    let mut server = KvsServer::new(engine, NaiveThreadPool);
    server.run(addr).await
}
