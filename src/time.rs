use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Formats a `SystemTime` as an RFC 3339 string (e.g. `2026-03-27T13:49:25.852+00:00`).
pub(crate) fn format_rfc3339(time: SystemTime) -> String {
    let since_epoch = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let total_secs = since_epoch.as_secs();
    let millis = since_epoch.subsec_millis();

    let (days, remaining) = (total_secs / 86400, total_secs % 86400);
    let (hours, remaining) = (remaining / 3600, remaining % 3600);
    let (minutes, seconds) = (remaining / 60, remaining % 60);
    let (year, month, day) = epoch_days_to_date(days);

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}+00:00",
        year, month, day, hours, minutes, seconds, millis
    )
}

/// Converts days since Unix epoch to (year, month, day).
/// Algorithm from <https://howardhinnant.github.io/date_algorithms.html>.
fn epoch_days_to_date(days: u64) -> (u64, u64, u64) {
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_epoch() {
        assert_eq!(format_rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00.000+00:00");
    }

    #[test]
    fn known_timestamp() {
        // 2026-03-27T13:49:25.852Z
        let time = UNIX_EPOCH + Duration::new(1774619365, 852_000_000);
        assert_eq!(
            format_rfc3339(time),
            "2026-03-27T13:49:25.852+00:00"
        );
    }

    #[test]
    fn leap_year() {
        // 2024-02-29T00:00:00.000Z (leap day)
        let time = UNIX_EPOCH + Duration::from_secs(1709164800);
        assert_eq!(
            format_rfc3339(time),
            "2024-02-29T00:00:00.000+00:00"
        );
    }

    #[test]
    fn end_of_year() {
        // 2025-12-31T23:59:59.999Z
        let time = UNIX_EPOCH + Duration::new(1767225599, 999_000_000);
        assert_eq!(
            format_rfc3339(time),
            "2025-12-31T23:59:59.999+00:00"
        );
    }

    #[test]
    fn no_millis() {
        // 2000-01-01T00:00:00.000Z
        let time = UNIX_EPOCH + Duration::from_secs(946684800);
        assert_eq!(
            format_rfc3339(time),
            "2000-01-01T00:00:00.000+00:00"
        );
    }

    #[test]
    fn century_leap_year() {
        // 2000-02-29T12:00:00.000Z (century leap year — divisible by 400)
        let time = UNIX_EPOCH + Duration::from_secs(951825600);
        assert_eq!(
            format_rfc3339(time),
            "2000-02-29T12:00:00.000+00:00"
        );
    }

    #[test]
    fn non_leap_century() {
        // 1900-03-01T00:00:00.000Z (1900 is NOT a leap year — divisible by 100 but not 400)
        // Negative offset from epoch: use a known positive instead.
        // 2100-03-01T00:00:00.000Z
        let time = UNIX_EPOCH + Duration::from_secs(4107542400);
        assert_eq!(
            format_rfc3339(time),
            "2100-03-01T00:00:00.000+00:00"
        );
    }

    #[test]
    fn day_before_non_leap_century() {
        // 2100-02-28T23:59:59.999Z (no Feb 29 in 2100)
        let time = UNIX_EPOCH + Duration::new(4107542399, 999_000_000);
        assert_eq!(
            format_rfc3339(time),
            "2100-02-28T23:59:59.999+00:00"
        );
    }
}
