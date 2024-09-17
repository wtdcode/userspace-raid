use std::collections::HashMap;

use clap::{Args, Parser, Subcommand};
use color_eyre::{eyre::eyre, Result};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::{
    device::DeviceConfiguration,
    nbd::server::{MemBlocks, Server},
    raid::{cli_configurations, RaidConfiguration, RAID},
};
#[derive(Args, Debug)]
pub struct ServerCli {
    #[arg(short, long)]
    level: usize,

    #[arg(short, long, value_parser = cli_configurations)]
    raid: HashMap<String, String>,

    #[arg(short, long, value_parser = cli_configurations)]
    device: Vec<HashMap<String, String>>,

    #[arg(long, default_value = "127.0.0.1:10844")]
    listen: String,
}

pub async fn server_main(args: ServerCli) -> Result<()> {
    info!("Server mode: {:?}", &args);

    let raid = RaidConfiguration::from_level_and_string(args.level, args.raid)?;
    let mut devices = vec![];

    for conf in args.device.into_iter() {
        let dev = DeviceConfiguration::from_confs(conf).await?;
        devices.push(dev);
    }

    if devices.len() == 0 {
        return Err(eyre!("No device is given"));
    }

    let blocks = RAID::new(devices, raid).await?;

    info!("Our server is {:?}", &blocks);
    let server = Server::new(blocks);
    let listener = TcpListener::bind(args.listen).await?;
    server.start(listener).await?;

    Ok(())
}
