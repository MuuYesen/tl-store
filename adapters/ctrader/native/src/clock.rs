use anyhow::{Result, ensure};

#[cfg(unix)]
pub fn monotonic_ns() -> Result<u64> {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // POSIX CLOCK_MONOTONIC is shared with the strategy; Rust Instant's epoch is not interchangeable.
    ensure!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &raw mut t) } == 0,
        "CLOCK_MONOTONIC failed"
    );
    ensure!(t.tv_sec >= 0 && t.tv_nsec >= 0, "Invalid monotonic clock");
    (t.tv_sec as u64)
        .checked_mul(1_000_000_000)
        .and_then(|s| s.checked_add(t.tv_nsec as u64))
        .ok_or_else(|| anyhow::anyhow!("Monotonic clock overflow"))
}
#[cfg(any(windows, test))]
fn qpc_nanoseconds(counter: i64, frequency: i64) -> Result<u64> {
    ensure!(counter >= 0 && frequency > 0, "Invalid QPC clock");
    let value = (counter as u128) * 1_000_000_000 / frequency as u128;
    u64::try_from(value).map_err(|_| anyhow::anyhow!("Monotonic clock overflow"))
}

#[cfg(windows)]
pub fn monotonic_ns() -> Result<u64> {
    use std::sync::OnceLock;
    use windows_sys::Win32::System::Performance::{
        QueryPerformanceCounter, QueryPerformanceFrequency,
    };
    static FREQUENCY: OnceLock<i64> = OnceLock::new();
    let frequency = *FREQUENCY.get_or_init(|| {
        let mut value = 0;
        if unsafe { QueryPerformanceFrequency(&raw mut value) } == 0 {
            0
        } else {
            value
        }
    });
    let mut counter = 0;
    ensure!(
        unsafe { QueryPerformanceCounter(&raw mut counter) } != 0,
        "QPC clock failed"
    );
    // Use the OS QPC origin shared by CPython time.monotonic_ns, not a local Instant epoch.
    qpc_nanoseconds(counter, frequency)
}

#[cfg(not(any(unix, windows)))]
pub fn monotonic_ns() -> Result<u64> {
    anyhow::bail!("Unsupported monotonic clock platform")
}

pub fn transact_time(text: &str) -> Result<time::OffsetDateTime> {
    let pattern = if text.contains('.') {
        "[year][month][day]-[hour]:[minute]:[second].[subsecond]"
    } else {
        "[year][month][day]-[hour]:[minute]:[second]"
    };
    Ok(time::PrimitiveDateTime::parse(
        text,
        &time::format_description::parse_borrowed::<2>(pattern)?,
    )?
    .assume_utc())
}

#[cfg(test)]
mod qpc_tests {
    use super::*;

    #[test]
    fn qpc_integer_nanoseconds_keep_system_epoch_and_fraction() {
        assert_eq!(qpc_nanoseconds(1, 3).unwrap(), 333_333_333);
        assert_eq!(
            qpc_nanoseconds(30_000_001, 10_000_000).unwrap(),
            3_000_000_100
        );
        assert_eq!(
            qpc_nanoseconds(i64::MAX, 1_000_000_000).unwrap(),
            i64::MAX as u64
        );
    }

    #[test]
    fn qpc_invalid_counter_frequency_and_overflow_are_not_timestamps() {
        for (counter, frequency) in [(-1, 1), (1, 0), (1, -1), (i64::MAX, 1)] {
            assert!(qpc_nanoseconds(counter, frequency).is_err());
        }
    }
}
