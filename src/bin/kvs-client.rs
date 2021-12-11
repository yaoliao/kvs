use std::net::SocketAddr;
use std::process::exit;

use log::{debug, error, info, LevelFilter};
use structopt::StructOpt;

use kvs::Result;
use kvs::{KvsClient, KvsLog};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client", about = "client for kvs")]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[structopt(name = "KEY", required = true, help = "A string key")]
        key: String,

        #[structopt(name = "VALUE", required = true, help = "The string value of the key")]
        value: String,

        #[structopt(
            long,
            help = "Sets the listening address",
            value_name = "IP:PORT",
            default_value(DEFAULT_LISTENING_ADDRESS),
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },

    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", required = true, help = "A string key")]
        key: String,

        #[structopt(
            long,
            help = "Sets the listening address",
            value_name = "IP:PORT",
            default_value(DEFAULT_LISTENING_ADDRESS),
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },

    #[structopt(name = "rm", about = "Remove a given string key")]
    Remove {
        #[structopt(name = "KEY", required = true, help = "A string key")]
        key: String,

        #[structopt(
            long,
            help = "Sets the listening address",
            value_name = "IP:PORT",
            default_value(DEFAULT_LISTENING_ADDRESS),
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
}

fn main() {
    KvsLog::log_setting();

    let opt: Opt = Opt::from_args();
    debug!("{:?}", opt);

    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    match opt.command {
        Command::Get { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Set { key, value, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        Command::Remove { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
    }

    Ok(())
}
