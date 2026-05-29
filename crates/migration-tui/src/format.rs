//! Tiny formatting helpers for the render layer.
//!
//! All functions are pure and fit-narrow on purpose — terminal real
//! estate is precious, so the standard SI / IEC formatters are
//! customized to ≤ 6 characters wherever possible.

use chrono::{DateTime, Utc};

/// Format a byte count using IEC binary units. Output is always at
/// most 7 characters: e.g. `0 B`, `512 B`, `1.2KiB`, `34MiB`,
/// `1.0GiB`, `999TiB`.
pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    const TIB: u64 = GIB * 1024;
    if bytes < KIB {
        return format!("{bytes}B");
    }
    let (val, suffix) = if bytes < MIB {
        (bytes as f64 / KIB as f64, "KiB")
    } else if bytes < GIB {
        (bytes as f64 / MIB as f64, "MiB")
    } else if bytes < TIB {
        (bytes as f64 / GIB as f64, "GiB")
    } else {
        (bytes as f64 / TIB as f64, "TiB")
    };
    if val >= 100.0 {
        format!("{val:.0}{suffix}")
    } else if val >= 10.0 {
        format!("{val:.1}{suffix}")
    } else {
        format!("{val:.2}{suffix}")
    }
}

/// Format a count using SI suffixes. Output is at most 6 characters:
/// e.g. `0`, `999`, `1.2k`, `45k`, `1.2M`, `34G`.
pub fn format_count(n: u64) -> String {
    const K: u64 = 1_000;
    const M: u64 = K * 1_000;
    const G: u64 = M * 1_000;
    const T: u64 = G * 1_000;
    if n < K {
        return n.to_string();
    }
    let (val, suffix) = if n < M {
        (n as f64 / K as f64, "k")
    } else if n < G {
        (n as f64 / M as f64, "M")
    } else if n < T {
        (n as f64 / G as f64, "G")
    } else {
        (n as f64 / T as f64, "T")
    };
    if val >= 100.0 {
        format!("{val:.0}{suffix}")
    } else if val >= 10.0 {
        format!("{val:.1}{suffix}")
    } else {
        format!("{val:.2}{suffix}")
    }
}

/// Format an elapsed duration as a compact relative label. Output is
/// at most 5 characters: `now`, `30s`, `5m`, `2h`, `4d`.
pub fn format_elapsed(then: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let secs = now.signed_duration_since(then).num_seconds();
    if secs < 0 {
        // Clock skew or pre-then "now" — display as "now" rather
        // than confuse with a negative number.
        return "now".to_string();
    }
    if secs < 5 {
        return "now".to_string();
    }
    if secs < 60 {
        return format!("{secs}s");
    }
    let mins = secs / 60;
    if mins < 60 {
        return format!("{mins}m");
    }
    let hours = mins / 60;
    if hours < 24 {
        return format!("{hours}h");
    }
    let days = hours / 24;
    format!("{days}d")
}

/// Format a progress ratio as a 4-character percentage: `  0%`,
/// ` 50%`, `100%`. Returns ` -- ` when the denominator is zero
/// (job hasn't started scanning yet).
pub fn format_pct(done: u64, total: u64) -> String {
    if total == 0 {
        return " -- ".to_string();
    }
    let pct = (done as f64 / total as f64) * 100.0;
    let pct = pct.clamp(0.0, 100.0);
    format!("{pct:>3.0}%")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(s, 0).unwrap()
    }

    #[test]
    fn bytes_under_kib_is_raw() {
        assert_eq!(format_bytes(0), "0B");
        assert_eq!(format_bytes(1), "1B");
        assert_eq!(format_bytes(1023), "1023B");
    }

    #[test]
    fn bytes_kib_mib_gib_tib() {
        assert_eq!(format_bytes(1024), "1.00KiB");
        assert_eq!(format_bytes(10 * 1024), "10.0KiB");
        assert_eq!(format_bytes(512 * 1024), "512KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.00MiB");
        assert_eq!(format_bytes(1024_u64.pow(3)), "1.00GiB");
        assert_eq!(format_bytes(1024_u64.pow(4)), "1.00TiB");
    }

    #[test]
    fn count_under_thousand_is_raw() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
    }

    #[test]
    fn count_k_m_g_t() {
        assert_eq!(format_count(1_000), "1.00k");
        assert_eq!(format_count(12_345), "12.3k");
        assert_eq!(format_count(1_234_567), "1.23M");
        assert_eq!(format_count(1_500_000_000), "1.50G");
        assert_eq!(format_count(2_000_000_000_000), "2.00T");
    }

    #[test]
    fn elapsed_buckets_into_now_s_m_h_d() {
        assert_eq!(format_elapsed(at(100), at(101)), "now");
        assert_eq!(format_elapsed(at(100), at(110)), "10s");
        assert_eq!(format_elapsed(at(100), at(400)), "5m");
        assert_eq!(format_elapsed(at(100), at(100 + 3 * 3600)), "3h");
        assert_eq!(format_elapsed(at(100), at(100 + 4 * 86_400)), "4d");
    }

    #[test]
    fn elapsed_handles_clock_skew() {
        // "then" is in the future relative to "now" — show as "now"
        // rather than a negative number.
        assert_eq!(format_elapsed(at(200), at(100)), "now");
    }

    #[test]
    fn pct_zero_denominator_shows_placeholder() {
        assert_eq!(format_pct(0, 0), " -- ");
        assert_eq!(format_pct(5, 0), " -- ");
    }

    #[test]
    fn pct_renders_in_4_chars() {
        assert_eq!(format_pct(0, 100), "  0%");
        assert_eq!(format_pct(50, 100), " 50%");
        assert_eq!(format_pct(100, 100), "100%");
        // Overflow clamps at 100.
        assert_eq!(format_pct(200, 100), "100%");
    }
}
