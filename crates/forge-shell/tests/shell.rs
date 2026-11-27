use std::io::Read;
use std::process::{Command, Stdio};
use std::time::Duration;

use forge_server::{spawn, Config};

fn shell() -> Command {
    Command::new(env!("CARGO_BIN_EXE_forge-shell"))
}

#[test]
fn unix_pipeline_counts_words() {
    let out = shell()
        .args(["-c", "/bin/echo hello there | /usr/bin/wc -w"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let text = String::from_utf8_lossy(&out.stdout);
    assert_eq!(text.split_whitespace().next(), Some("2"), "{text:?}");
}

#[test]
fn redirection_writes_a_file() {
    let dir = std::env::temp_dir().join(format!("forge-sh-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("out.txt");
    let _ = std::fs::remove_file(&path);
    let script = format!("/bin/echo hello > {}", path.display());
    let out = shell().args(["-c", &script]).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello\n");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn get_output_pipes_into_tr() {
    let dir = std::env::temp_dir().join(format!("forge-sh-db-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = Config::new(dir.clone());
    cfg.redis_addr = "127.0.0.1:0".parse().unwrap();
    cfg.http_addr = "127.0.0.1:0".parse().unwrap();
    cfg.exit_on_fatal = false;
    let srv = spawn(cfg).unwrap();
    let port = srv.redis_port.to_string();
    let set = shell()
        .args(["--port", &port, "-c", "set greeting hello"])
        .output()
        .unwrap();
    assert!(set.status.success(), "{}", String::from_utf8_lossy(&set.stderr));
    assert!(String::from_utf8_lossy(&set.stdout).contains("OK"));
    let got = shell()
        .args([
            "--port",
            &port,
            "-c",
            "get greeting | /usr/bin/tr a-z A-Z",
        ])
        .output()
        .unwrap();
    assert!(got.status.success(), "{}", String::from_utf8_lossy(&got.stderr));
    assert_eq!(String::from_utf8_lossy(&got.stdout).trim(), "HELLO");
    let ttl = shell()
        .args(["--port", &port, "-c", "ttl greeting"])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&ttl.stdout).trim(), "-1");
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn background_job_is_listed_and_fg_waits() {
    let out = shell()
        .args(["-c", "/bin/sleep 0.2 &\njobs\nfg"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "stdout {text} stderr {err}");
    assert!(text.contains("running"), "{text}");
    assert!(text.contains("/bin/sleep 0.2"), "{text}");
}

#[test]
fn bg_continues_a_job() {
    let mut child = shell()
        .args(["-c", "/bin/sleep 30 &\nbg\njobs"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut buf = Vec::new();
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(3) && !buf.windows(7).any(|w| w == b"running") {
        let mut tmp = [0u8; 256];
        match stdout.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => break,
        }
    }
    let text = String::from_utf8_lossy(&buf);
    assert!(text.contains("continued"), "{text}");
    assert!(text.contains("running"), "{text}");
    if let Some(pgid) = text
        .split_whitespace()
        .find_map(|w| w.parse::<i32>().ok())
        .filter(|n| *n > 1)
    {
        let _ = Command::new("/bin/kill")
            .args(["-KILL", &format!("-{pgid}")])
            .status();
    }
    let _ = child.kill();
    let _ = child.wait();
}
