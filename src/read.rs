use std::{io::Write, os::fd::AsRawFd, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use color_eyre::{eyre::eyre, Result};
use itertools::Itertools;
use rand::RngCore;
use tokio::net::TcpStream;
use tracing::{info, warn};

use crate::nbd::{client::Client, kernel};

#[derive(Args)]
pub struct TestCli {
    #[arg(long, default_value = "127.0.0.1:10844")]
    host: String,

    #[arg(long, default_value = "0")]
    offset: u64,

    #[arg(short, long, default_value = "0")]
    size: u32,

    #[arg(short, long, default_value = "-")]
    output: String,

    #[arg(short, long)]
    raw: bool,

    #[arg(short, long)]
    write: bool,
}

pub async fn test_main(args: TestCli) -> Result<()> {
    let tcp = TcpStream::connect(args.host).await?;
    let client = Client::new(tcp).await?;

    let out = if args.write {
        info!("Connected, writing random data...");
        let mut rng = rand::thread_rng();
        let data = (0..args.size)
            .into_iter()
            .map(|_| (rng.next_u64() % 256) as u8)
            .collect_vec();
        client.write(args.offset, &data).await?;
        let out = client.read(args.offset, args.size).await?;
        if out != data {
            warn!("Inconsitency read and write!!!");

            for (idx, lhs) in data.into_iter().enumerate() {
                let rhs = out[idx];
                if lhs != rhs {
                    warn!(idx, lhs, rhs, "inconsistency");
                }
            }
        }
        out
    } else {
        info!("Reading directly...");
        let out = client.read(args.offset, args.size).await?;
        out
    };

    if args.output == "-" {
        if args.raw {
            std::io::stdout().write_all(&out)?;
        } else {
            hexdump::hexdump(&out);
        }
    } else {
        let mut fpath = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&args.output)?;

        fpath.write_all(&out)?;
    }

    Ok(())
}
