use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use forge_core::Failpoint;
use forge_server::Config;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let mut data = PathBuf::from("forge-data");
    let mut redis = "127.0.0.1:6379".to_string();
    let mut http = "127.0.0.1:8080".to_string();
    let mut failpoint = env::var("FORGE_FAILPOINT").ok();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" => data = PathBuf::from(args.next().unwrap_or_default()),
            "--redis" => redis = args.next().unwrap_or_default(),
            "--http" => http = args.next().unwrap_or_default(),
            "--failpoint" => failpoint = args.next(),
            "-h" | "--help" => {
                eprintln!("forge-server --data DIR [--redis ADDR] [--http ADDR]");
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }
    let mut config = Config::new(data);
    config.redis_addr = match redis.parse() {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("bad redis address: {err}");
            return ExitCode::from(2);
        }
    };
    config.http_addr = match http.parse() {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("bad http address: {err}");
            return ExitCode::from(2);
        }
    };
    if let Some(name) = failpoint.as_deref() {
        match Failpoint::parse(name) {
            Some(point) => {
                config.failpoint = Some(point);
                config.exit_on_failpoint = true;
            }
            None => {
                eprintln!("unknown failpoint {name}");
                return ExitCode::from(2);
            }
        }
    }
    let handle = match forge_server::spawn(config) {
        Ok(handle) => handle,
        Err(err) => {
            eprintln!("forge-server: {err}");
            return ExitCode::from(1);
        }
    };
    println!(
        "forge ready redis={} http={}",
        handle.redis_port, handle.http_port
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
    handle.wait();
    ExitCode::SUCCESS
}
