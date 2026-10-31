mod probe;
mod venue;

use anyhow::{Context, Result, ensure};
use clap::{CommandFactory, Parser};
use futures_util::future::join_all;
use probe::{Probe, print_results};
use std::{collections::HashMap, process::ExitCode, time::Duration};
use tokio::time::Instant;
use venue::Venue;

#[derive(Debug, Parser)]
#[command(about = "Measure public WebSocket book age at receipt, without trading credentials.")]
struct Args {
    /// Venues to compare (default: all eight supported venues)
    #[arg(long, value_enum, num_args = 1.., default_values = ["hyperliquid", "entropy", "lighter", "lighter-rh", "tradexyz", "gate", "arcus", "ondo"])]
    venues: Vec<Venue>,
    /// Base symbol
    #[arg(long, default_value = "BTC")]
    symbol: String,
    /// Override a selected venue's symbol; repeat for multiple venues
    #[arg(long, value_name = "VENUE=SYMBOL")]
    market: Vec<String>,
    /// Seconds including WS connect, after metadata loads
    #[arg(long, default_value_t = 60.0, allow_hyphen_values = true)]
    duration: f64,
    /// Percentiles from 0 to 100; p50 is always included
    #[arg(long, alias = "pxx", num_args = 1.., default_values = ["50", "90"], value_name = "P")]
    percentiles: Vec<f64>,
}

impl Args {
    fn validate(&mut self) -> Result<HashMap<Venue, String>> {
        ensure!(
            self.duration.is_finite() && self.duration > 0.0,
            "--duration must be finite and > 0"
        );
        let duration = Duration::try_from_secs_f64(self.duration)
            .context("--duration is outside the supported range")?;
        ensure!(
            !duration.is_zero() && Instant::now().checked_add(duration).is_some(),
            "--duration is outside the supported range"
        );
        ensure!(
            self.percentiles
                .iter()
                .all(|p| p.is_finite() && (0.0..=100.0).contains(p)),
            "--percentiles must be finite and in [0, 100]"
        );
        self.percentiles.push(50.0);
        self.percentiles.sort_by(f64::total_cmp);
        self.percentiles.dedup();
        let mut seen = Vec::new();
        self.venues.retain(|v| {
            if seen.contains(v) {
                false
            } else {
                seen.push(*v);
                true
            }
        });
        self.symbol = self.symbol.trim().to_uppercase();
        ensure!(!self.symbol.is_empty(), "--symbol cannot be empty");
        let mut markets = HashMap::new();
        for entry in &self.market {
            let (name, symbol) = entry
                .split_once('=')
                .context("--market requires a selected VENUE=SYMBOL")?;
            let venue = self
                .venues
                .iter()
                .find(|v| v.name() == name)
                .context("--market requires a selected VENUE=SYMBOL")?;
            ensure!(
                !symbol.trim().is_empty(),
                "--market requires a selected VENUE=SYMBOL"
            );
            markets.insert(*venue, symbol.trim().to_uppercase());
        }
        Ok(markets)
    }
}

async fn measure(args: Args, markets: HashMap<Venue, String>) -> Result<u8> {
    // Both HTTP metadata and WebSocket traffic use direct connections.
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut probes: Vec<_> = args
        .venues
        .iter()
        .map(|v| Probe::new(*v, markets.get(v).unwrap_or(&args.symbol).clone()))
        .collect();
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    println!("Loading public market metadata...");
    let interrupted = tokio::select! {
        _ = join_all(probes.iter_mut().map(|p| p.prepare(&client))) => false,
        result = &mut interrupt => { result?; true },
    };
    if interrupted {
        print_results(&mut probes, &args.percentiles)?;
        return Ok(130);
    }
    let ready = probes.iter().filter(|p| p.error.is_none()).count();
    let mut interrupted = false;
    if ready > 0 {
        println!("Measuring {ready} venue(s) for {}s...", args.duration);
        let deadline = Instant::now() + Duration::from_secs_f64(args.duration);
        tokio::select! {
            _ = join_all(probes.iter_mut().filter(|p| p.error.is_none())
                .map(|p| p.run(deadline))) => {},
            result = &mut interrupt => { result?; interrupted = true; },
        }
    }
    let failed = print_results(&mut probes, &args.percentiles)?;
    Ok(if interrupted { 130 } else { u8::from(failed) })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let mut args = Args::parse();
    let markets = args.validate().unwrap_or_else(|error| {
        Args::command()
            .error(clap::error::ErrorKind::ValueValidation, error)
            .exit()
    });
    match measure(args, markets).await {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("Error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_normalizes_overrides_and_percentiles() {
        let mut args = Args::try_parse_from([
            "probe",
            "--venues",
            "gate",
            "entropy",
            "gate",
            "--pxx",
            "99.9",
            "90",
            "90",
            "--market",
            "entropy=sndk",
            "--duration",
            "0.5",
        ])
        .unwrap();
        let markets = args.validate().unwrap();
        assert_eq!(args.venues, [Venue::Gate, Venue::Entropy]);
        assert_eq!(args.percentiles, [50.0, 90.0, 99.9]);
        assert_eq!(markets[&Venue::Entropy], "SNDK");
        assert_eq!(args.duration, 0.5);
        assert_eq!(Args::try_parse_from(["probe"]).unwrap().venues.len(), 8);
    }

    #[test]
    fn cli_rejects_invalid_inputs() {
        for argv in [
            vec!["--duration", "0"],
            vec!["--duration", "nan"],
            vec!["--duration", "inf"],
            vec!["--duration", "-1"],
            vec!["--duration", "1e100"],
            vec!["--duration", "1e-20"],
            vec!["--percentiles", "101"],
            vec!["--percentiles", "-1"],
            vec!["--percentiles", "nan"],
            vec!["--symbol", " "],
            vec!["--market", "gate="],
            vec!["--market", "unknown=BTC"],
            vec!["--venues", "gate", "--market", "ondo=BTC"],
        ] {
            let result = Args::try_parse_from(std::iter::once("probe").chain(argv.clone()));
            assert!(
                result.is_err() || result.unwrap().validate().is_err(),
                "{argv:?}"
            );
        }
    }
}
