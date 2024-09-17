use clap::{Args, Parser, Subcommand};
use mount::{mount_main, MountCli};
use color_eyre::Result;
use read::{read_main, ReadCli};
use server::{server_main, ServerCli};
use tracing::info;

mod mount;
mod device;
mod nbd;
mod raid;
mod server;
mod read;

#[derive(Subcommand)]
enum Command {
    Server(ServerCli),
    Mount(MountCli),
    Read(ReadCli)
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

async fn cli_main(args: Cli) -> Result<()> {
    match args.cmd {
        Command::Server(args) => server_main(args).await,
        Command::Mount(args) => mount_main(args).await,
        Command::Read(args) => read_main(args).await
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(cli_main(args))
}
