use near_async::time;

use crate::stats::metrics;
use std::sync::Arc;

// Error messages for rate limiter
#[derive(thiserror::Error, Debug)]
#[error("amount requested: {amount_requested} , max allowed: {max_allowed}")]
pub struct RequestTooLargeError {
    pub amount_requested: usize,
    pub max_allowed: usize,
}

// Configurations to set RateLimiter
#[derive(Copy, Clone)]
pub struct RateLimiterConfig {
    pub capacity: usize,               // maximum capacity in token bucket
    pub refill_period: time::Duration, // duration to wait between refills
    pub refill_amount: usize,          // amount of tokens to refill each period
}

// Mutable shared variables that must be guarded with mutex
pub struct RateLimiterInner {
    last_refill: time::Instant, // last time the rate limiter refilled itself
    balance: usize, // amount of memory in bytes available to be used by an incoming message
    clock: Arc<time::Clock>,
}

// Rate Limiter can be used to rate limit network traffic for by concurrent connections for the traffic policing purposes.
pub struct RateLimiter {
    config: RateLimiterConfig,
    shared: tokio::sync::Mutex<RateLimiterInner>,
}

impl RateLimiter {
    pub fn new(config: RateLimiterConfig, clock: Arc<time::Clock>) -> RateLimiter {
        let inner: tokio::sync::Mutex<RateLimiterInner> =
            tokio::sync::Mutex::new(RateLimiterInner {
                balance: config.capacity,
                last_refill: clock.now(),
                clock: clock,
            });
        metrics::READ_RATE_LIMITER_BALANCE.set(config.capacity as i64);
        return RateLimiter { config: config, shared: inner };
    }

    // This method is recomputes and then returns the current available balance for the rate limiter.
    #[cfg(test)]
    pub(crate) async fn get_balance(&self) -> usize {
        let mut shared = self.shared.lock().await;
        let now = shared.clock.now();
        let time_passed = now - shared.last_refill;
        let number_of_refill_periods = (time_passed / self.config.refill_period).floor();
        shared.last_refill += self.config.refill_period * number_of_refill_periods;
        let amount_to_increment = (number_of_refill_periods as usize) * self.config.refill_amount;
        shared.balance += amount_to_increment;
        shared.balance = shared.balance.min(self.config.capacity);
        return shared.balance;
    }

    pub fn capacity(&self) -> usize {
        return self.config.capacity;
    }

    pub fn refill_period(&self) -> time::Duration {
        return self.config.refill_period;
    }

    // This method is used by an incoming request to try and gain approval from the rate limiter to be accepted.
    // The amount acquired is the incoming request message's size.
    // If there is sufficient balance to be acquired, this method deducts the amount being acquired from the balance and returns immediately.
    // Otherwise, it waits until the balance has sufficiently filled up to more than the amount acquired.
    // If the amount acquired is larger than the maximum capacity that the balance can ever reach, this method returns an error.
    pub async fn acquire(&self, amount: usize) -> Result<(), RequestTooLargeError> {
        if self.capacity() < amount {
            return Err(RequestTooLargeError {
                max_allowed: self.capacity(),
                amount_requested: amount,
            });
        }
        let mut shared = self.shared.lock().await; // shared lock gets acquired after this line
        let mut now = shared.clock.now();
        let time_passed = now - shared.last_refill;
        let number_of_refill_periods = (time_passed / self.config.refill_period).floor();
        shared.last_refill += self.config.refill_period * number_of_refill_periods;
        let amount_to_increment = (number_of_refill_periods as usize) * self.config.refill_amount;
        shared.balance += amount_to_increment;
        shared.balance = shared.balance.min(self.config.capacity);
        metrics::READ_RATE_LIMITER_BALANCE.set(shared.balance as i64);
        while amount > shared.balance {
            let remainder_amount = amount - shared.balance;
            let number_of_periods_needed =
                ((remainder_amount as f64) / (self.config.refill_amount as f64)).ceil();
            let unused_remainder_duration = now - shared.last_refill;
            let sleep_duration =
                (self.refill_period() * number_of_periods_needed) - unused_remainder_duration;
            shared.clock.sleep(sleep_duration).await; // sleep while holding the shared lock
            now = shared.clock.now();
            let time_passed = now - shared.last_refill;
            let number_of_refill_periods = (time_passed / self.config.refill_period).floor();
            shared.last_refill += self.config.refill_period * number_of_refill_periods;
            let amount_to_increment =
                (number_of_refill_periods as usize) * self.config.refill_amount;
            shared.balance += amount_to_increment;
            shared.balance = shared.balance.min(self.config.capacity);
            metrics::READ_RATE_LIMITER_BALANCE.set(shared.balance as i64);
        }
        shared.balance -= amount;
        metrics::READ_RATE_LIMITER_BALANCE.set(shared.balance as i64);
        Ok(()) // the shared lock gets released after this line
    }
}
