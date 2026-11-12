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
#[cfg(not(unix))]
pub fn monotonic_ns() -> Result<u64> {
    anyhow::bail!("This adapter requires POSIX CLOCK_MONOTONIC")
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
