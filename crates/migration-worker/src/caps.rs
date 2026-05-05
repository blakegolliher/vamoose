//! Linux capability probing for the worker.
//!
//! M2 only needs to know whether `CAP_CHOWN` is held effectively — if
//! `preserve_owner` is on but the worker can't chown, every file
//! copied by a non-root user would EPERM on the chown step. We'd
//! rather fail at startup with a clear message than burn through a
//! shard producing one EPERM per file.
//!
//! Implementation reads `/proc/self/status` and parses the `CapEff:`
//! hex bitfield. CAP_CHOWN is bit 0 (see `man 7 capabilities`). No
//! external crate dep needed.

const CAP_CHOWN_BIT: u64 = 0;

pub fn has_cap_chown() -> bool {
    let s = match std::fs::read_to_string("/proc/self/status") {
        Ok(s) => s,
        Err(_) => return false,
    };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("CapEff:") {
            let hex = rest.trim();
            if let Ok(v) = u64::from_str_radix(hex, 16) {
                return (v >> CAP_CHOWN_BIT) & 1 == 1;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The function should at least be callable and return *some*
    /// answer without panicking. The actual value depends on how the
    /// test process was launched, so we just assert it doesn't crash.
    #[test]
    fn callable_without_panic() {
        let _ = has_cap_chown();
    }
}
