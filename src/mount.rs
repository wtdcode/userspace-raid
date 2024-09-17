use std::{os::fd::AsRawFd, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use color_eyre::{eyre::eyre, Result};
use tokio::net::TcpStream;
use tracing::{info, warn};

use crate::nbd::{client::Client, kernel};

#[derive(Args)]
pub struct MountCli {
    #[arg(long, default_value = "127.0.0.1:10844")]
    host: String,

    #[arg(short, long, default_value = "/dev/nbd0")]
    device: String,
}

pub async fn mount_main(args: MountCli) -> Result<()> {
    let tcp = TcpStream::connect(args.host).await?;
    let client = Client::new(tcp).await?;

    // info!("Clearing previous connection if any...");
    // let nbd = std::fs::OpenOptions::new()
    //     .read(true)
    //     .write(true)
    //     .open(&args.device)?;
    // kernel::close(&nbd)?;
    info!("Reopen our connection...");
    let nbd = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&args.device)?;
    kernel::set_client(&nbd, client.size(), client.as_raw_fd())?;
    info!("Mounted {}, waiting...", args.device);
    kernel::wait(&nbd)?;
    Ok(())
}
