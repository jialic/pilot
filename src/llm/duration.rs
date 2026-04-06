use std::time::Duration;

/// Parse a Go-style duration string (e.g. "6m0s", "5s", "200ms", "1m30.5s").
///
/// Supports hours (h), minutes (m), seconds (s), and milliseconds (ms).
/// Fractional values are allowed (e.g. "1.5s" = 1500ms).
pub fn parse_go_duration(s: &str) -> Option<Duration> {
    let mut total_ms: u64 = 0;
    let mut num = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
        } else if c == 'm' && chars.peek() == Some(&'s') {
            chars.next(); // consume 's'
            let n: f64 = num.parse().ok()?;
            total_ms += n as u64;
            num.clear();
        } else {
            if num.is_empty() {
                return None;
            }
            let n: f64 = num.parse().ok()?;
            match c {
                'h' => total_ms += (n * 3_600_000.0) as u64,
                'm' => total_ms += (n * 60_000.0) as u64,
                's' => total_ms += (n * 1_000.0) as u64,
                _ => return None,
            }
            num.clear();
        }
    }

    // Trailing number without unit is invalid
    if !num.is_empty() {
        return None;
    }

    if total_ms == 0 {
        return None;
    }

    Some(Duration::from_millis(total_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds() {
        assert_eq!(parse_go_duration("5s"), Some(Duration::from_secs(5)));
    }

    #[test]
    fn milliseconds() {
        assert_eq!(parse_go_duration("200ms"), Some(Duration::from_millis(200)));
    }

    #[test]
    fn minutes() {
        assert_eq!(parse_go_duration("3m"), Some(Duration::from_secs(180)));
    }

    #[test]
    fn hours() {
        assert_eq!(parse_go_duration("2h"), Some(Duration::from_secs(7200)));
    }

    #[test]
    fn minutes_and_seconds() {
        assert_eq!(parse_go_duration("1m30s"), Some(Duration::from_secs(90)));
    }

    #[test]
    fn minutes_zero_seconds() {
        assert_eq!(parse_go_duration("6m0s"), Some(Duration::from_secs(360)));
    }

    #[test]
    fn real_openai_header() {
        // Actual value seen from OpenAI: "1m1.283s"
        assert_eq!(
            parse_go_duration("1m1.283s"),
            Some(Duration::from_millis(61_283)),
        );
    }

    #[test]
    fn fractional_seconds() {
        assert_eq!(parse_go_duration("1.5s"), Some(Duration::from_millis(1500)));
    }

    #[test]
    fn hours_minutes_seconds() {
        assert_eq!(
            parse_go_duration("1h30m15s"),
            Some(Duration::from_secs(5415)),
        );
    }

    #[test]
    fn just_milliseconds_small() {
        assert_eq!(parse_go_duration("50ms"), Some(Duration::from_millis(50)));
    }

    #[test]
    fn empty_string() {
        assert_eq!(parse_go_duration(""), None);
    }

    #[test]
    fn no_unit() {
        assert_eq!(parse_go_duration("123"), None);
    }

    #[test]
    fn zero() {
        assert_eq!(parse_go_duration("0s"), None);
    }

    #[test]
    fn invalid_unit() {
        assert_eq!(parse_go_duration("5x"), None);
    }

    #[test]
    fn just_unit_no_number() {
        assert_eq!(parse_go_duration("s"), None);
    }

    #[test]
    fn fractional_minutes() {
        assert_eq!(parse_go_duration("1.5m"), Some(Duration::from_secs(90)));
    }

    #[test]
    fn large_milliseconds() {
        assert_eq!(parse_go_duration("5000ms"), Some(Duration::from_secs(5)));
    }

    #[test]
    fn negative_rejected() {
        assert_eq!(parse_go_duration("-5s"), None);
    }

    #[test]
    fn whitespace_rejected() {
        assert_eq!(parse_go_duration(" 5s"), None);
        assert_eq!(parse_go_duration("5s "), None);
    }

    #[test]
    fn duplicate_units_accumulate() {
        // Go's parser rejects this, but accumulating is a safe behavior
        assert_eq!(parse_go_duration("5s5s"), Some(Duration::from_secs(10)));
    }
}
