use clap::Parser;

/// Redis server
#[derive(Parser, Debug)]
#[clap(version, author = "Daniel Bolivar")]
pub(crate) struct Args {
    /// Port to listen on
    #[clap(short, long, default_value = "6379")]
    pub(crate) port: u16,

    /// Host and port of the master server
    #[clap(short, long, num_args = 2)]
    pub(crate) replicaof: Vec<String>,
}
