use clap::Parser;
use tracing::info;

mod nbd;

#[derive(Parser)]
struct Cli {}

fn main() {
    tracing_subscriber::fmt::init();
}
