//! RESP2 subset: arrays of bulk strings, plus inline commands.

#[derive(Debug)]
pub enum Pull {
    NeedMore,
    Command(Vec<Vec<u8>>),
}

pub fn pull(buf: &mut Vec<u8>, max_bulk: usize) -> Result<Pull, String> {
    if buf.is_empty() {
        return Ok(Pull::NeedMore);
    }
    if buf[0] == b'*' {
        pull_array(buf, max_bulk)
    } else {
        pull_inline(buf, max_bulk)
    }
}

fn pull_array(buf: &mut Vec<u8>, max_bulk: usize) -> Result<Pull, String> {
    let header_end = match find_crlf(buf, 0) {
        Some(p) => p,
        None => {
            if buf.len() > max_bulk {
                return Err("request too large".into());
            }
            return Ok(Pull::NeedMore);
        }
    };
    if header_end > max_bulk {
        return Err("request too large".into());
    }
    let n: i64 = std::str::from_utf8(&buf[1..header_end])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "bad array".to_string())?;
    if !(0..=1024).contains(&n) {
        return Err("bad array length".into());
    }
    let mut pos = header_end + 2;
    let mut args = Vec::with_capacity(n as usize);
    for _ in 0..n {
        if pos >= buf.len() {
            return Ok(Pull::NeedMore);
        }
        if buf[pos] != b'$' {
            return Err("expected bulk string".into());
        }
        let hend = match find_crlf(buf, pos) {
            Some(p) => p,
            None => {
                if buf.len() - pos > max_bulk {
                    return Err("request too large".into());
                }
                return Ok(Pull::NeedMore);
            }
        };
        let len: i64 = std::str::from_utf8(&buf[pos + 1..hend])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| "bad bulk length".to_string())?;
        if len < 0 {
            return Err("null bulk in command".into());
        }
        if len as usize > max_bulk {
            return Err("bulk too large".into());
        }
        let start = hend + 2;
        let end = start + len as usize;
        if buf.len() < end + 2 {
            return Ok(Pull::NeedMore);
        }
        if &buf[end..end + 2] != b"\r\n" {
            return Err("bad bulk terminator".into());
        }
        args.push(buf[start..end].to_vec());
        pos = end + 2;
    }
    buf.drain(..pos);
    Ok(Pull::Command(args))
}

fn pull_inline(buf: &mut Vec<u8>, max_bulk: usize) -> Result<Pull, String> {
    let Some(end) = buf.iter().position(|b| *b == b'\n') else {
        if buf.len() > max_bulk {
            return Err("request too large".into());
        }
        return Ok(Pull::NeedMore);
    };
    let mut line = buf[..end].to_vec();
    buf.drain(..end + 1);
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    if line.is_empty() {
        return pull(buf, max_bulk);
    }
    if line.len() > max_bulk {
        return Err("request too large".into());
    }
    Ok(Pull::Command(split_ws(&line)))
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..]
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|p| p + from)
}

fn split_ws(line: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut cur = Vec::new();
    for b in line {
        if b.is_ascii_whitespace() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push(*b);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

pub fn ok() -> Vec<u8> {
    b"+OK\r\n".to_vec()
}

pub fn pong() -> Vec<u8> {
    b"+PONG\r\n".to_vec()
}

pub fn err(msg: &str) -> Vec<u8> {
    let clean: String = msg.chars().filter(|c| *c != '\r' && *c != '\n').collect();
    format!("-ERR {clean}\r\n").into_bytes()
}

pub fn bulk(data: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", data.len()).into_bytes();
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

pub fn null_bulk() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

pub fn integer(n: i64) -> Vec<u8> {
    format!(":{n}\r\n").into_bytes()
}

pub fn scan_reply(cursor: &str, keys: &[Vec<u8>]) -> Vec<u8> {
    let mut keys_arr = format!("*{}\r\n", keys.len()).into_bytes();
    for key in keys {
        keys_arr.extend(bulk(key));
    }
    let mut out = b"*2\r\n".to_vec();
    out.extend(bulk(cursor.as_bytes()));
    out.extend(keys_arr);
    out
}

pub fn empty_array() -> Vec<u8> {
    b"*0\r\n".to_vec()
}
