use clap::{Args, Parser, Subcommand};
use color_eyre::Result;
use mount::{mount_main, MountCli};
use read::{test_main, TestCli};
use server::{server_main, ServerCli};
use tracing::info;

mod device;
mod mount;
mod nbd;
mod parity;
mod raid;
mod read;
mod server;
#[derive(Subcommand)]
enum Command {
    Server(ServerCli),
    Mount(MountCli),
    Test(TestCli),
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
        Command::Test(args) => test_main(args).await,
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
