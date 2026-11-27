//! A small shell: forge builtins plus Unix pipelines.
//!
//! Job control (`fg`, `bg`, `jobs`) runs in this process. A builtin that sits
//! in a pipeline runs in the forked child and exits with `_exit`, so it does
//! not return into the parent's memory.

use std::io::{self, BufRead, Read, Write};
use std::net::TcpStream;
use std::os::raw::{c_char, c_int};
use std::process::ExitCode;
use std::time::Duration;

struct Stage {
    argv: Vec<String>,
    infile: Option<String>,
    outfile: Option<String>,
    append: bool,
}

struct Job {
    id: usize,
    pgid: i32,
    cmd: String,
    live: bool,
}

fn main() -> ExitCode {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 6379;
    let mut scripts: Vec<String> = Vec::new();
    let mut files: Vec<String> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--host" => host = args.next().unwrap_or_default(),
            "--port" => {
                port = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
            }
            "-c" => scripts.push(args.next().unwrap_or_default()),
            "-h" | "--help" => {
                eprintln!("forge-shell [--host H] [--port N] [-c SCRIPT] [file...]");
                return ExitCode::SUCCESS;
            }
            other if other.starts_with('-') => {
                eprintln!("unknown argument {other}");
                return ExitCode::from(2);
            }
            other => files.push(other.to_string()),
        }
    }
    if port == 0 {
        eprintln!("bad port");
        return ExitCode::from(2);
    }
    let mut jobs: Vec<Job> = Vec::new();
    let mut status = 0;
    if scripts.is_empty() && files.is_empty() {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = match line {
                Ok(line) => line,
                Err(_) => break,
            };
            status = run_line(&line, &host, port, &mut jobs);
        }
    } else {
        for script in &scripts {
            for line in script.lines() {
                status = run_line(line, &host, port, &mut jobs);
            }
        }
        for path in &files {
            let text = match std::fs::read_to_string(path) {
                Ok(text) => text,
                Err(err) => {
                    eprintln!("{path}: {err}");
                    return ExitCode::from(1);
                }
            };
            for line in text.lines() {
                status = run_line(line, &host, port, &mut jobs);
            }
        }
    }
    ExitCode::from(status as u8)
}

fn run_line(line: &str, host: &str, port: u16, jobs: &mut Vec<Job>) -> i32 {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return 0;
    }
    let (stages, background) = match parse(trimmed) {
        Ok(parsed) => parsed,
        Err(err) => {
            eprintln!("forge-shell: {err}");
            return 2;
        }
    };
    if stages.is_empty() || stages.iter().any(|s| s.argv.is_empty()) {
        eprintln!("forge-shell: empty command");
        return 2;
    }
    if stages.len() == 1 && is_job_builtin(&stages[0].argv[0]) {
        if background || stages[0].infile.is_some() || stages[0].outfile.is_some() {
            eprintln!("forge-shell: job control cannot be piped or redirected");
            return 1;
        }
        return job_builtin(&stages[0].argv, jobs);
    }
    let single = stages.len() == 1
        && !background
        && stages[0].infile.is_none()
        && stages[0].outfile.is_none()
        && is_builtin(&stages[0].argv[0]);
    if single {
        return builtin_status(&stages[0].argv, host, port);
    }
    match spawn_pipeline(&stages, background, host, port, jobs, trimmed) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("forge-shell: {err}");
            1
        }
    }
}

fn is_job_builtin(name: &str) -> bool {
    matches!(name, "fg" | "bg" | "jobs")
}

fn is_builtin(name: &str) -> bool {
    matches!(name, "get" | "set" | "del" | "ttl" | "dump" | "scan")
}

fn job_builtin(argv: &[String], jobs: &mut Vec<Job>) -> i32 {
    reap(jobs);
    match argv[0].as_str() {
        "jobs" => {
            if jobs.iter().all(|j| !j.live) {
                return 0;
            }
            for job in jobs.iter().filter(|j| j.live) {
                println!("[{}] running {} {}", job.id, job.pgid, job.cmd);
            }
            0
        }
        "fg" => {
            let Some(job) = jobs.iter_mut().rev().find(|j| j.live) else {
                eprintln!("forge-shell: fg: no current job");
                return 1;
            };
            let pgid = job.pgid;
            job.live = false;
            wait_group(pgid)
        }
        "bg" => {
            let Some(job) = jobs.iter().rev().find(|j| j.live) else {
                eprintln!("forge-shell: bg: no current job");
                return 1;
            };
            let rc = unsafe { libc::kill(-job.pgid, libc::SIGCONT) };
            if rc < 0 {
                eprintln!("forge-shell: bg: {}", io::Error::last_os_error());
                return 1;
            }
            println!("[{}] continued {}", job.id, job.cmd);
            0
        }
        _ => 1,
    }
}

fn builtin_status(argv: &[String], host: &str, port: u16) -> i32 {
    match run_builtin(argv, host, port) {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("forge-shell: {err}");
            1
        }
    }
}

fn run_builtin(argv: &[String], host: &str, port: u16) -> Result<(), String> {
    match argv[0].as_str() {
        "get" if argv.len() == 2 => {
            match call(host, port, &[b"GET", argv[1].as_bytes()])? {
                Reply::Bulk(Some(v)) => {
                    let mut out = io::stdout().lock();
                    out.write_all(&v).map_err(|e| e.to_string())?;
                    out.write_all(b"\n").map_err(|e| e.to_string())?;
                }
                Reply::Bulk(None) => println!("(nil)"),
                other => return Err(format!("unexpected {other:?}")),
            }
            Ok(())
        }
        "set" if argv.len() == 3 => {
            expect_ok(&call(
                host,
                port,
                &[b"SET", argv[1].as_bytes(), argv[2].as_bytes()],
            )?)?;
            println!("OK");
            Ok(())
        }
        "set" if argv.len() == 5 && argv[3].eq_ignore_ascii_case("ex") => {
            expect_ok(&call(
                host,
                port,
                &[
                    b"SET",
                    argv[1].as_bytes(),
                    argv[2].as_bytes(),
                    b"EX",
                    argv[4].as_bytes(),
                ],
            )?)?;
            println!("OK");
            Ok(())
        }
        "del" if argv.len() == 2 => {
            let n = expect_int(&call(host, port, &[b"DEL", argv[1].as_bytes()])?)?;
            println!("{n}");
            Ok(())
        }
        "ttl" if argv.len() == 2 => {
            let n = expect_int(&call(host, port, &[b"TTL", argv[1].as_bytes()])?)?;
            println!("{n}");
            Ok(())
        }
        "dump" if argv.len() == 1 => {
            let mut cursor = "0".to_string();
            loop {
                let reply = call(host, port, &[b"SCAN", cursor.as_bytes(), b"COUNT", b"64"])?;
                let (next, keys) = scan_page(&reply)?;
                for key in keys {
                    let shown = String::from_utf8_lossy(&key);
                    match call(host, port, &[b"GET", &key])? {
                        Reply::Bulk(Some(v)) => {
                            println!("{shown} {}", String::from_utf8_lossy(&v));
                        }
                        _ => println!("{shown}"),
                    }
                }
                if next == "0" {
                    break;
                }
                cursor = next;
            }
            Ok(())
        }
        "dump" if argv.len() == 2 => {
            match call(host, port, &[b"GET", argv[1].as_bytes()])? {
                Reply::Bulk(Some(v)) => {
                    println!("{}", String::from_utf8_lossy(&v));
                }
                Reply::Bulk(None) => println!("(nil)"),
                other => return Err(format!("unexpected {other:?}")),
            }
            Ok(())
        }
        "scan" => {
            let pattern = if argv.len() == 2 {
                Some(argv[1].as_str())
            } else if argv.len() == 1 {
                None
            } else {
                return Err("usage: scan [pattern]".into());
            };
            let mut cursor = "0".to_string();
            loop {
                let reply = if let Some(pat) = pattern {
                    call(
                        host,
                        port,
                        &[b"SCAN", cursor.as_bytes(), b"MATCH", pat.as_bytes()],
                    )?
                } else {
                    call(host, port, &[b"SCAN", cursor.as_bytes()])?
                };
                let (next, keys) = scan_page(&reply)?;
                for key in keys {
                    println!("{}", String::from_utf8_lossy(&key));
                }
                if next == "0" {
                    break;
                }
                cursor = next;
            }
            Ok(())
        }
        _ => Err(format!("usage: {}", argv[0])),
    }
}

fn spawn_pipeline(
    stages: &[Stage],
    background: bool,
    host: &str,
    port: u16,
    jobs: &mut Vec<Job>,
    cmd: &str,
) -> Result<i32, String> {
    let mut prev_read: c_int = -1;
    let mut pids = Vec::new();
    let mut pgid: i32 = 0;
    for (index, stage) in stages.iter().enumerate() {
        let last = index + 1 == stages.len();
        let mut ends = [-1, -1];
        if !last {
            let rc = unsafe { libc::pipe(ends.as_mut_ptr()) };
            if rc < 0 {
                return Err(io::Error::last_os_error().to_string());
            }
        }
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(io::Error::last_os_error().to_string());
        }
        if pid == 0 {
            if pgid == 0 {
                unsafe { libc::setpgid(0, 0) };
            } else {
                unsafe { libc::setpgid(0, pgid) };
            }
            if prev_read >= 0 {
                unsafe {
                    libc::dup2(prev_read, 0);
                    libc::close(prev_read);
                }
            }
            if !last {
                unsafe {
                    libc::dup2(ends[1], 1);
                    libc::close(ends[0]);
                    libc::close(ends[1]);
                }
            }
            if let Some(path) = &stage.infile {
                let fd = open_in(path);
                if fd < 0 {
                    eprintln!("forge-shell: {path}: {}", io::Error::last_os_error());
                    unsafe { libc::_exit(1) };
                }
                unsafe {
                    libc::dup2(fd, 0);
                    libc::close(fd);
                }
            }
            if let Some(path) = &stage.outfile {
                let fd = open_out(path, stage.append);
                if fd < 0 {
                    eprintln!("forge-shell: {path}: {}", io::Error::last_os_error());
                    unsafe { libc::_exit(1) };
                }
                unsafe {
                    libc::dup2(fd, 1);
                    libc::close(fd);
                }
            }
            if is_builtin(&stage.argv[0]) {
                let code = builtin_status(&stage.argv, host, port);
                unsafe { libc::_exit(code) };
            }
            exec_stage(&stage.argv);
        }
        if pgid == 0 {
            pgid = pid;
        }
        unsafe { libc::setpgid(pid, pgid) };
        if prev_read >= 0 {
            unsafe { libc::close(prev_read) };
        }
        if !last {
            unsafe { libc::close(ends[1]) };
            prev_read = ends[0];
        }
        pids.push(pid);
    }
    if prev_read >= 0 {
        unsafe { libc::close(prev_read) };
    }
    if background {
        let id = jobs.len() + 1;
        jobs.push(Job {
            id,
            pgid,
            cmd: cmd.to_string(),
            live: true,
        });
        println!("[{id}] {pgid}");
        return Ok(0);
    }
    let mut code = 0;
    for pid in pids {
        code = wait_pid(pid);
    }
    Ok(code)
}

fn exec_stage(argv: &[String]) -> ! {
    let c_args: Vec<std::ffi::CString> = argv
        .iter()
        .map(|s| std::ffi::CString::new(s.as_str()).unwrap_or_else(|_| std::ffi::CString::new("").unwrap()))
        .collect();
    let mut ptrs: Vec<*const c_char> = c_args.iter().map(|s| s.as_ptr()).collect();
    ptrs.push(std::ptr::null());
    unsafe { libc::execvp(ptrs[0], ptrs.as_ptr()) };
    eprintln!(
        "forge-shell: {}: {}",
        argv.first().map(String::as_str).unwrap_or(""),
        io::Error::last_os_error()
    );
    unsafe { libc::_exit(127) };
}

fn open_in(path: &str) -> c_int {
    let c = std::ffi::CString::new(path).unwrap_or_default();
    unsafe { libc::open(c.as_ptr(), libc::O_RDONLY) }
}

fn open_out(path: &str, append: bool) -> c_int {
    let c = std::ffi::CString::new(path).unwrap_or_default();
    let flags = libc::O_WRONLY | libc::O_CREAT | if append { libc::O_APPEND } else { libc::O_TRUNC };
    unsafe { libc::open(c.as_ptr(), flags, 0o644) }
}

fn wait_pid(pid: i32) -> i32 {
    loop {
        let mut status: c_int = 0;
        let rc = unsafe { libc::waitpid(pid, &mut status, 0) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return 1;
        }
        if libc::WIFEXITED(status) {
            return libc::WEXITSTATUS(status);
        }
        if libc::WIFSIGNALED(status) {
            return 128 + libc::WTERMSIG(status);
        }
    }
}

fn wait_group(pgid: i32) -> i32 {
    let mut code = 0;
    loop {
        let mut status: c_int = 0;
        let rc = unsafe { libc::waitpid(-pgid, &mut status, 0) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        if libc::WIFEXITED(status) {
            code = libc::WEXITSTATUS(status);
        } else if libc::WIFSIGNALED(status) {
            code = 128 + libc::WTERMSIG(status);
        }
    }
    code
}

fn reap(jobs: &mut [Job]) {
    for job in jobs.iter_mut().filter(|j| j.live) {
        let mut status: c_int = 0;
        let rc = unsafe { libc::waitpid(-job.pgid, &mut status, libc::WNOHANG) };
        if rc > 0 && (libc::WIFEXITED(status) || libc::WIFSIGNALED(status)) {
            let again = unsafe { libc::waitpid(-job.pgid, &mut status, libc::WNOHANG) };
            if again < 0 {
                job.live = false;
            }
        } else if rc < 0 {
            job.live = false;
        }
    }
}

fn parse(line: &str) -> Result<(Vec<Stage>, bool), String> {
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    let mut stages = vec![Stage {
        argv: Vec::new(),
        infile: None,
        outfile: None,
        append: false,
    }];
    let mut background = false;
    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        match chars[i] {
            '|' => {
                stages.push(Stage {
                    argv: Vec::new(),
                    infile: None,
                    outfile: None,
                    append: false,
                });
                i += 1;
            }
            '<' => {
                i += 1;
                let word = take_word(&chars, &mut i)?;
                stages.last_mut().unwrap().infile = Some(word);
            }
            '>' => {
                i += 1;
                let append = if i < chars.len() && chars[i] == '>' {
                    i += 1;
                    true
                } else {
                    false
                };
                let word = take_word(&chars, &mut i)?;
                let stage = stages.last_mut().unwrap();
                stage.outfile = Some(word);
                stage.append = append;
            }
            '&' => {
                i += 1;
                while i < chars.len() && chars[i].is_whitespace() {
                    i += 1;
                }
                if i != chars.len() {
                    return Err("'&' is only allowed at the end".into());
                }
                background = true;
            }
            _ => {
                let word = take_word(&chars, &mut i)?;
                stages.last_mut().unwrap().argv.push(word);
            }
        }
    }
    Ok((stages, background))
}

fn take_word(chars: &[char], i: &mut usize) -> Result<String, String> {
    while *i < chars.len() && chars[*i].is_whitespace() {
        *i += 1;
    }
    if *i >= chars.len() {
        return Err("missing word".into());
    }
    let mut out = String::new();
    if chars[*i] == '\'' || chars[*i] == '"' {
        let quote = chars[*i];
        *i += 1;
        while *i < chars.len() && chars[*i] != quote {
            out.push(chars[*i]);
            *i += 1;
        }
        if *i >= chars.len() {
            return Err("unclosed quote".into());
        }
        *i += 1;
        return Ok(out);
    }
    while *i < chars.len() {
        let c = chars[*i];
        if c.is_whitespace() || matches!(c, '|' | '<' | '>' | '&') {
            break;
        }
        out.push(c);
        *i += 1;
    }
    if out.is_empty() {
        return Err("missing word".into());
    }
    Ok(out)
}

#[derive(Debug)]
enum Reply {
    Ok,
    Err(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
}

fn call(host: &str, port: u16, args: &[&[u8]]) -> Result<Reply, String> {
    let mut stream = TcpStream::connect((host, port)).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| e.to_string())?;
    let mut req = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        req.extend(format!("${}\r\n", arg.len()).into_bytes());
        req.extend_from_slice(arg);
        req.extend_from_slice(b"\r\n");
    }
    stream.write_all(&req).map_err(|e| e.to_string())?;
    read_reply(&mut stream)
}

fn read_reply(stream: &mut TcpStream) -> Result<Reply, String> {
    let line = read_line(stream)?;
    if line.is_empty() {
        return Err("empty reply".into());
    }
    match line[0] {
        b'+' => Ok(Reply::Ok),
        b'-' => Ok(Reply::Err(
            String::from_utf8_lossy(&line[1..]).into_owned(),
        )),
        b':' => {
            let n = std::str::from_utf8(&line[1..])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "bad integer".to_string())?;
            Ok(Reply::Integer(n))
        }
        b'$' => {
            let n: i64 = std::str::from_utf8(&line[1..])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "bad bulk".to_string())?;
            if n < 0 {
                return Ok(Reply::Bulk(None));
            }
            let mut buf = vec![0u8; n as usize];
            stream.read_exact(&mut buf).map_err(|e| e.to_string())?;
            let mut crlf = [0u8; 2];
            stream.read_exact(&mut crlf).map_err(|e| e.to_string())?;
            Ok(Reply::Bulk(Some(buf)))
        }
        b'*' => {
            let n: i64 = std::str::from_utf8(&line[1..])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "bad array".to_string())?;
            if n < 0 {
                return Ok(Reply::Array(Vec::new()));
            }
            let mut items = Vec::with_capacity(n as usize);
            for _ in 0..n {
                items.push(read_reply(stream)?);
            }
            Ok(Reply::Array(items))
        }
        _ => Err("bad reply".into()),
    }
}

fn read_line(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("connection closed".into());
        }
        if byte[0] == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(line);
        }
        line.push(byte[0]);
        if line.len() > 1024 * 1024 {
            return Err("reply too long".into());
        }
    }
}

fn expect_ok(reply: &Reply) -> Result<(), String> {
    match reply {
        Reply::Ok => Ok(()),
        Reply::Err(err) => Err(err.clone()),
        other => Err(format!("unexpected {other:?}")),
    }
}

fn expect_int(reply: &Reply) -> Result<i64, String> {
    match reply {
        Reply::Integer(n) => Ok(*n),
        Reply::Err(err) => Err(err.clone()),
        other => Err(format!("unexpected {other:?}")),
    }
}

fn scan_page(reply: &Reply) -> Result<(String, Vec<Vec<u8>>), String> {
    let Reply::Array(items) = reply else {
        return Err("scan reply".into());
    };
    if items.len() != 2 {
        return Err("scan reply".into());
    }
    let cursor = match &items[0] {
        Reply::Bulk(Some(v)) => String::from_utf8_lossy(v).into_owned(),
        _ => return Err("scan cursor".into()),
    };
    let Reply::Array(keys) = &items[1] else {
        return Err("scan keys".into());
    };
    let mut out = Vec::new();
    for key in keys {
        match key {
            Reply::Bulk(Some(v)) => out.push(v.clone()),
            _ => return Err("scan key".into()),
        }
    }
    Ok((cursor, out))
}
