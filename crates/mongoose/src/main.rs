//! `mongoose` binary — parse the CLI and dispatch.
//!
//! Exit codes: 0 = success (including a deliberate SIGINT/SIGTERM
//! stop — resume with `mongoose copy`); 1 = error; 2 = the copy
//! completed but recorded per-file failures (see `failures/`).

use clap::Parser;
use mongoose::cli::{Cli, Command};
use std::process::ExitCode;

fn main() -> ExitCode {
    // Default the full reserved-port range on (libnfs checks only the
    // variable's *presence*): without it, /etc/services name
    // registrations throttle a host to ~55 context pairs, and every
    // invocation needed a LIBNFS_USE_ALL_RESERVED=1 prefix. Opt out
    // by setting it to "0" or "" — mongoose then unsets it so libnfs
    // sees it as absent. Must run before the tokio runtime exists:
    // env mutation is only sound while the process is single-threaded.
    match std::env::var("LIBNFS_USE_ALL_RESERVED") {
        Err(std::env::VarError::NotPresent) => std::env::set_var("LIBNFS_USE_ALL_RESERVED", "1"),
        Ok(v) if v == "0" || v.is_empty() => std::env::remove_var("LIBNFS_USE_ALL_RESERVED"),
        _ => {}
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async_main())
}

async fn async_main() -> ExitCode {
    let cli = Cli::parse();
    // Compact by default: engine libraries (walker, rewrite, shard
    // processor, mover) log at warn; mongoose's own stage lines and
    // progress ticks stay. -v = full info, -vv = debug. RUST_LOG wins
    // when set.
    let default_filter = match cli.verbose {
        0 => "warn,mongoose=info",
        1 => "info",
        _ => "debug",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .init();
    let result = match &cli.command {
        Command::Prepare(args) => mongoose::prepare::run(args).await.map(|_| None),
        Command::Copy(args) => mongoose::copy::run(&args.work_dir, &args.tuning)
            .await
            .map(Some),
        Command::Run(args) => match mongoose::prepare::run(&args.prepare).await {
            Ok(_) => mongoose::copy::run(&args.prepare.work_dir, &args.tuning)
                .await
                .map(Some),
            Err(e) => Err(e),
        },
        Command::Sync(args) => mongoose::sync::run(args).await.map(|outcome| outcome.copy),
    };

    match result {
        Ok(Some(summary)) if !summary.interrupted && summary.files_failed > 0 => ExitCode::from(2),
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}
