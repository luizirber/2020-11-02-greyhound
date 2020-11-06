use std::path::PathBuf;

use structopt::StructOpt;

use greyhound_core::{gather, index};

#[derive(StructOpt, Debug)]
enum Cli {
    Gather {
        /// Query signature
        #[structopt(parse(from_os_str))]
        query_path: PathBuf,

        /// Precomputed index or list of reference signatures
        #[structopt(parse(from_os_str))]
        siglist: PathBuf,

        /// ksize
        #[structopt(short = "k", long = "ksize", default_value = "31")]
        ksize: u8,

        /// scaled
        #[structopt(short = "s", long = "scaled", default_value = "1000")]
        scaled: usize,

        /// threshold_bp
        #[structopt(short = "t", long = "threshold_bp", default_value = "50000")]
        threshold_bp: usize,

        /// The path for output
        #[structopt(parse(from_os_str), short = "o", long = "output")]
        output: Option<PathBuf>,

        /// Is the index a list of signatures?
        #[structopt(long = "--from-file")]
        from_file: bool,

        /// Delay loading queries into memory
        #[structopt(long = "--lazy")]
        lazy: bool,

        /// Preload reference signatures into memory
        #[structopt(long = "--preload")]
        preload: bool,
    },
    Index {
        /// The path for output
        #[structopt(parse(from_os_str))]
        output: PathBuf,

        /// List of reference signatures
        #[structopt(parse(from_os_str))]
        siglist: PathBuf,

        /// ksize
        #[structopt(short = "k", long = "ksize", default_value = "31")]
        ksize: u8,

        /// scaled
        #[structopt(short = "s", long = "scaled", default_value = "1000")]
        scaled: usize,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match Cli::from_args() {
        Cli::Gather {
            query_path,
            siglist,
            ksize,
            scaled,
            threshold_bp,
            output,
            from_file,
            lazy,
            preload,
        } => gather(
            query_path,
            siglist,
            ksize,
            scaled,
            threshold_bp,
            output,
            from_file,
            lazy,
            preload,
        )?,
        Cli::Index {
            output,
            siglist,
            ksize,
            scaled,
        } => index(siglist, ksize, scaled, output)?,
    };

    Ok(())
}
