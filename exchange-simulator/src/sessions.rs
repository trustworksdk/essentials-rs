//! Trading sessions per market (plan item 6.10, SR-16): when each market, by MIC, is open. A market without a
//! session (and an instrument without a MIC) is always open, the default.
//!
//! - **Outside a session** new client orders are rejected with `MARKET_CLOSED`; cancellations still work, and the
//!   liquidity provider applies its closed-market policy.
//! - **`DAY` orders** (SR-17) expire at the first close after they were accepted. On a market without a session they
//!   expire at the business day's end (`clearing.cutoff`), so a `DAY` order means the same thing everywhere.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use simulator_kit::calendar::{CutoffCalendar, DailySession};

#[derive(Clone, Debug, Default)]
pub struct TradingSessions {
    /// Sessions by uppercase MIC.
    markets: BTreeMap<String, DailySession>,
    /// Where a `DAY` order on a market without a session ends; `None` keeps it until cancelled.
    day_end: Option<CutoffCalendar>,
}

impl TradingSessions {
    pub fn new(markets: BTreeMap<String, DailySession>, day_end: Option<CutoffCalendar>) -> Self {
        Self { markets, day_end }
    }

    pub fn session(&self, mic: Option<&str>) -> Option<&DailySession> {
        mic.and_then(|mic| self.markets.get(mic))
    }

    /// Whether the instrument's market is open at `at`; always for a market without a session.
    pub fn is_open(&self, mic: Option<&str>, at: DateTime<Utc>) -> bool {
        self.session(mic).is_none_or(|s| s.is_open(at))
    }

    /// When a `DAY` order accepted at `accepted_at` expires: its session's first close after then, or on a market
    /// without a session the end of the business day it was accepted in.
    pub fn day_expiry(
        &self,
        mic: Option<&str>,
        accepted_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match self.session(mic) {
            Some(session) => Some(session.close_after(accepted_at)),
            None => self
                .day_end
                .map(|calendar| calendar.cutoff_at(calendar.business_date(accepted_at))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    #[test]
    fn day_orders_end_at_the_close_or_the_business_day() {
        let sessions = TradingSessions::new(
            [(
                "XNAS".to_string(),
                DailySession::parse("09:30", "16:00", "America/New_York").unwrap(),
            )]
            .into(),
            Some(CutoffCalendar::parse("17:30", "Europe/Copenhagen").unwrap()),
        );
        assert!(
            sessions.is_open(None, at("2030-01-12T03:00:00Z")),
            "no MIC: always open"
        );
        assert!(
            sessions.is_open(Some("XCSE"), at("2030-01-12T03:00:00Z")),
            "no session: always open"
        );
        assert!(!sessions.is_open(Some("XNAS"), at("2030-01-07T21:00:00Z")));
        assert_eq!(
            sessions.day_expiry(Some("XNAS"), at("2030-01-07T15:00:00Z")),
            Some(at("2030-01-07T21:00:00Z"))
        );
        assert_eq!(
            sessions.day_expiry(Some("XCSE"), at("2030-01-07T15:00:00Z")),
            Some(at("2030-01-07T16:30:00Z")),
            "the business day's cutoff"
        );
        assert_eq!(
            TradingSessions::default().day_expiry(None, at("2030-01-07T15:00:00Z")),
            None
        );
    }
}
