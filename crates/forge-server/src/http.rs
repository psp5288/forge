#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
}

/// Parse one HTTP/1.1 request header block. `Ok(None)` means the headers are
/// incomplete. A request larger than `limit` is an error and is not buffered
/// further by the caller.
pub fn pull_request(buf: &[u8], limit: usize) -> Result<Option<(Request, usize)>, String> {
    if buf.len() > limit && buf.windows(4).all(|w| w != b"\r\n\r\n") {
        return Err("headers too large".into());
    }
    let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Ok(None);
    };
    if pos > limit {
        return Err("headers too large".into());
    }
    let head = std::str::from_utf8(&buf[..pos]).map_err(|_| "headers are not utf-8".to_string())?;
    let mut lines = head.split("\r\n");
    let req = lines.next().unwrap_or("");
    let mut parts = req.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    if method.is_empty() || path.is_empty() {
        return Err("bad request line".into());
    }
    if path.len() > limit {
        return Err("path too large".into());
    }
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                let n: u64 = value.trim().parse().unwrap_or(u64::MAX);
                if n > 1024 {
                    return Err("body too large".into());
                }
            }
        }
    }
    Ok(Some((
        Request { method, path },
        pos + 4,
    )))
}

pub fn response(status: u16, reason: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

pub fn percent_decode(path: &str) -> Option<Vec<u8>> {
    let bytes = path.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = from_hex(bytes[i + 1])?;
            let lo = from_hex(bytes[i + 2])?;
            out.push((hi << 4) | lo);
            i += 3;
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Some(out)
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
