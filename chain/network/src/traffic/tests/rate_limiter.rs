use crate::traffic::rate_limiter::*;
use near_async::*;
use std::sync::*;

#[tokio::test]
async fn rate_limiter_simple() -> anyhow::Result<()> {
    let capacity: usize = 10;
    let refill_period: time::Duration = time::Duration::seconds(1);
    let refill_amount: usize = 3;
    let config: RateLimiterConfig = RateLimiterConfig {
        capacity: capacity,
        refill_period: refill_period,
        refill_amount: refill_amount,
    };
    let clock = time::FakeClock::default();
    let rate_limiter: RateLimiter = RateLimiter::new(config, Arc::new(clock.clock()));

    // Check initial value is full
    assert_eq!(capacity, rate_limiter.get_balance().await);

    // Check doesn't exceed max capacity after refill_period
    clock.advance(refill_period);
    assert_eq!(capacity, rate_limiter.get_balance().await);

    // Can acquire capacity and that it ends up empty
    let result = rate_limiter.acquire(capacity).await;
    assert!(result.is_ok());
    assert_eq!(0, rate_limiter.get_balance().await);

    // Check that acquiring capacity accounts for duration that has passed within one refill_period
    clock.advance(refill_period / 2);
    assert_eq!(0, rate_limiter.get_balance().await); // It doesn't refill after half the refill_period
    clock.advance(refill_period / 2);
    assert_eq!(refill_amount, rate_limiter.get_balance().await); // It should refill after other half of refill_period has passed
    let result = rate_limiter.acquire(refill_amount).await; // should return immediately
    assert!(result.is_ok());
    assert_eq!(0, rate_limiter.get_balance().await);

    // Check refill period and doesn't exceed capacity
    let num_refill_to_full = ((capacity as f64) / (refill_amount as f64)).ceil() as usize;
    for n in 1..(num_refill_to_full + 1) {
        clock.advance(refill_period);
        assert_eq!((n * refill_amount).min(capacity), rate_limiter.get_balance().await);
    }

    // Test can acquire to exceed max capacity as long as wait long enough
    let exec = rate_limiter.acquire(capacity);
    clock.advance(refill_period * (capacity as f64)); // sleep with fake clock waits for it to be moved by another thread
    let result = exec.await;
    assert!(result.is_ok());
    let exec = rate_limiter.acquire(capacity);
    clock.advance(refill_period * (capacity as f64)); // sleep with fake clock waits for it to be moved by another thread
    let result = exec.await;
    assert!(result.is_ok());

    Ok(())
}

#[tokio::test]
async fn rate_limiter_concurrent_test() -> anyhow::Result<()> {
    let capacity: usize = 10;
    let refill_period: time::Duration = time::Duration::seconds(1);
    let refill_amount: usize = 3;
    let config: RateLimiterConfig = RateLimiterConfig {
        capacity: capacity,
        refill_period: refill_period,
        refill_amount: refill_amount,
    };
    let clock = time::FakeClock::default();
    let rate_limiter: Arc<RateLimiter> =
        Arc::new(RateLimiter::new(config, Arc::new(clock.clock())));

    // Define an execution flow using futures
    let rate_limiter_a = rate_limiter.clone();
    let execution1 = tokio::spawn(async move {
        let _result = &rate_limiter_a.acquire(1).await;
    });

    let rate_limiter_b = rate_limiter.clone();
    let execution2 = tokio::spawn(async move {
        let _result = rate_limiter_b.acquire(1).await;
    });

    let rate_limiter_c = rate_limiter.clone();
    let execution3 = tokio::spawn(async move {
        let _result = rate_limiter_c.acquire(1).await;
    });

    // Execute all 3 tasks asynchronously
    let _ = tokio::join!(execution1, execution2, execution3);

    assert_eq!(capacity - 3, rate_limiter.get_balance().await);
    Ok(())
}
