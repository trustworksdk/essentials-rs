//! Trading sessions per market (plan item 6.10, SR-16): when each market, by MIC, is open. A market without a
//! session (and an instrument without a MIC) is always open, the default.
//!
//! - **Outside a session** new client orders are rejected with `MARKET_CLOSED`; cancellations still work, and the
//!   liquidity provider applies its closed-market policy.
//! - **`DAY` orders** (SR-17) expire at the first close after they were accepted. On a market without a session they
//!   expire at the business day's end (`clearing.cutoff`), so a `DAY` order means the same thing everywhere.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
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

    /// When a `DAY` order accepted at `accepted_at` expires, and the business day that ends then (SR-32): its
    /// session's first close after then and the session's date, or on a market without a session the end of the
    /// business day it was accepted in and that day.
    pub fn day_expiry(
        &self,
        mic: Option<&str>,
        accepted_at: DateTime<Utc>,
    ) -> Option<(DateTime<Utc>, NaiveDate)> {
        match self.session(mic) {
            Some(session) => {
                let close = session.close_after(accepted_at);
                Some((close, close.with_timezone(&session.time_zone).date_naive()))
            }
            None => self.day_end.map(|calendar| {
                let date = calendar.business_date(accepted_at);
                (calendar.cutoff_at(date), date)
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn date(text: &str) -> NaiveDate {
        text.parse().unwrap()
    }

    #[test]
    fn day_orders_end_at_the_close_or_the_business_day() {
        let sessions = TradingSessions::new(
            [
                (
                    "XNAS".to_string(),
                    DailySession::parse("09:30", "16:00", "America/New_York").unwrap(),
                ),
                (
                    "XPAC".to_string(),
                    DailySession::parse("06:30", "16:00", "America/Los_Angeles").unwrap(),
                ),
            ]
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
            Some((at("2030-01-07T21:00:00Z"), date("2030-01-07")))
        );
        assert_eq!(
            sessions.day_expiry(Some("XNAS"), at("2030-01-07T22:00:00Z")),
            Some((at("2030-01-08T21:00:00Z"), date("2030-01-08"))),
            "after the close: the next session's"
        );
        assert_eq!(
            sessions.day_expiry(Some("XPAC"), at("2030-01-07T20:00:00Z")),
            Some((at("2030-01-08T00:00:00Z"), date("2030-01-07"))),
            "the session's own date, not the close's UTC date"
        );
        assert_eq!(
            sessions.day_expiry(Some("XCSE"), at("2030-01-07T15:00:00Z")),
            Some((at("2030-01-07T16:30:00Z"), date("2030-01-07"))),
            "the business day's cutoff"
        );
        assert_eq!(
            sessions.day_expiry(Some("XCSE"), at("2030-01-07T17:00:00Z")),
            Some((at("2030-01-08T16:30:00Z"), date("2030-01-08"))),
            "after the cutoff: the next business day's"
        );
        assert_eq!(
            TradingSessions::default().day_expiry(None, at("2030-01-07T15:00:00Z")),
            None
        );
    }
}
