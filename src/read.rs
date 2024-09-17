use std::{io::Write, os::fd::AsRawFd, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use color_eyre::{eyre::eyre, Result};
use tokio::net::TcpStream;
use tracing::{info, warn};

use crate::nbd::{client::Client, kernel};

#[derive(Args)]
pub struct ReadCli {
    #[arg(long, default_value = "127.0.0.1:10844")]
    host: String,

    #[arg(short, long, default_value = "/dev/nbd0")]
    offset: u64,

    #[arg(short, long)]
    size: u32,

    #[arg(short, long, default_value = "-")]
    output: String,

    #[arg(short, long)]
    raw: bool
}

pub async fn read_main(args: ReadCli) -> Result<()> {
    let tcp = TcpStream::connect(args.host).await?;
    let client = Client::new(tcp).await?;

    info!("Connected, reading blocks...");
    let out = client.read(args.offset, args.size).await?;

    if args.output == "-" {
        if args.raw {
            std::io::stdout().write_all(&out)?;
        } else {
            hexdump::hexdump(&out);
        }
    } else {
        let mut fpath = std::fs::OpenOptions::new().write(true).truncate(true).create(true).open(&args.output)?;

        fpath.write_all(&out)?;
    }

    Ok(())
}
