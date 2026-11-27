/// Byte glob: `*` any run, `?` one byte. Used by SCAN MATCH.
pub fn glob_match(pat: &[u8], text: &[u8]) -> bool {
    fn rec(pat: &[u8], text: &[u8]) -> bool {
        let mut i = 0;
        let mut j = 0;
        while i < pat.len() {
            if pat[i] == b'*' {
                if i + 1 == pat.len() {
                    return true;
                }
                let mut k = j;
                while k <= text.len() {
                    if rec(&pat[i + 1..], &text[k..]) {
                        return true;
                    }
                    k += 1;
                }
                return false;
            } else if pat[i] == b'?' {
                if j >= text.len() {
                    return false;
                }
                i += 1;
                j += 1;
            } else {
                if j >= text.len() || text[j] != pat[i] {
                    return false;
                }
                i += 1;
                j += 1;
            }
        }
        j == text.len()
    }
    rec(pat, text)
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = from_hex(bytes[i])?;
        let lo = from_hex(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
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
