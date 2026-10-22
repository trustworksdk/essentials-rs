//! Business days and daily cutoffs. Business days are Monday to Friday, less configured holidays (SR-35). Used for
//! T+N settlement dates, end-of-day statements, value dates and trading sessions.

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Utc, Weekday};
use chrono_tz::Tz;

/// Which days are business days: weekdays that are not holidays. The default has no holidays. Cheap to clone, so every
/// calendar of a service can hold the same set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BusinessDays {
    holidays: Arc<BTreeSet<NaiveDate>>,
}

impl BusinessDays {
    pub fn with_holidays(holidays: impl IntoIterator<Item = NaiveDate>) -> Self {
        Self {
            holidays: Arc::new(holidays.into_iter().collect()),
        }
    }

    /// The configured holidays, in date order (weekend dates included, if configured).
    pub fn holidays(&self) -> impl Iterator<Item = NaiveDate> + '_ {
        self.holidays.iter().copied()
    }

    pub fn is_holiday(&self, date: NaiveDate) -> bool {
        self.holidays.contains(&date)
    }

    pub fn is_business_day(&self, date: NaiveDate) -> bool {
        !matches!(date.weekday(), Weekday::Sat | Weekday::Sun) && !self.is_holiday(date)
    }

    /// The date `days` business days after `date`. A trade on a day that is not a business day counts from the next
    /// one, so T+0 of a Saturday trade is Monday.
    pub fn add_business_days(&self, date: NaiveDate, days: u32) -> NaiveDate {
        let mut current = date;
        while !self.is_business_day(current) {
            current = current.succ_opt().expect("date in range");
        }
        let mut remaining = days;
        while remaining > 0 {
            current = current.succ_opt().expect("date in range");
            if self.is_business_day(current) {
                remaining -= 1;
            }
        }
        current
    }

    /// The business day after `date`.
    pub fn next_business_day(&self, date: NaiveDate) -> NaiveDate {
        self.add_business_days(date.succ_opt().expect("date in range"), 0)
    }

    /// The business day before `date`.
    pub fn previous_business_day(&self, date: NaiveDate) -> NaiveDate {
        let mut current = date.pred_opt().expect("date in range");
        while !self.is_business_day(current) {
            current = current.pred_opt().expect("date in range");
        }
        current
    }
}

/// A daily cutoff: `cutoff` local time in `time_zone`, on business days, for end-of-day statements.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CutoffCalendar {
    pub cutoff: NaiveTime,
    pub time_zone: Tz,
    pub days: BusinessDays,
}

impl CutoffCalendar {
    /// Parses `cutoff` as `HH:MM` (or `HH:MM:SS`) and `time_zone` as an IANA name such as `Europe/Copenhagen`.
    pub fn parse(cutoff: &str, time_zone: &str) -> Result<Self, String> {
        let cutoff = NaiveTime::parse_from_str(cutoff, "%H:%M")
            .or_else(|_| NaiveTime::parse_from_str(cutoff, "%H:%M:%S"))
            .map_err(|_| format!("cutoff must be HH:MM, got {cutoff:?}"))?;
        let time_zone = time_zone
            .parse()
            .map_err(|_| format!("time_zone is not a known time zone: {time_zone:?}"))?;
        Ok(Self {
            cutoff,
            time_zone,
            days: BusinessDays::default(),
        })
    }

    /// The same calendar with these business days.
    #[must_use]
    pub fn with_days(mut self, days: BusinessDays) -> Self {
        self.days = days;
        self
    }

    /// The instant of `date`'s cutoff. A cutoff that falls in a daylight-saving gap moves an hour later.
    pub fn cutoff_at(&self, date: NaiveDate) -> DateTime<Utc> {
        let local = date.and_time(self.cutoff);
        self.time_zone
            .from_local_datetime(&local)
            .earliest()
            .or_else(|| {
                self.time_zone
                    .from_local_datetime(&(local + chrono::Duration::hours(1)))
                    .earliest()
            })
            .expect("a local time an hour after a gap exists")
            .with_timezone(&Utc)
    }

    /// The business date an instant belongs to: the first business day whose cutoff is after it.
    pub fn business_date(&self, at: DateTime<Utc>) -> NaiveDate {
        let local = at.with_timezone(&self.time_zone).date_naive();
        if self.days.is_business_day(local) && at < self.cutoff_at(local) {
            local
        } else {
            self.days.next_business_day(local)
        }
    }

    /// The window of instants `date`'s statement covers, `[from, until)`: from the previous business day's cutoff to
    /// this one's.
    pub fn window(&self, date: NaiveDate) -> (DateTime<Utc>, DateTime<Utc>) {
        (
            self.cutoff_at(self.days.previous_business_day(date)),
            self.cutoff_at(date),
        )
    }
}

/// A market's trading session: open from `open` to `close` local time in `time_zone`, on business days. Sessions do
/// not span midnight.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DailySession {
    pub open: NaiveTime,
    pub close: NaiveTime,
    pub time_zone: Tz,
    pub days: BusinessDays,
}

impl DailySession {
    /// Parses `open` and `close` as `HH:MM` and `time_zone` as an IANA name such as `America/New_York`.
    pub fn parse(open: &str, close: &str, time_zone: &str) -> Result<Self, String> {
        let time = |field: &str, text: &str| {
            NaiveTime::parse_from_str(text, "%H:%M")
                .map_err(|_| format!("{field} must be HH:MM, got {text:?}"))
        };
        let session = Self {
            open: time("open", open)?,
            close: time("close", close)?,
            time_zone: time_zone
                .parse()
                .map_err(|_| format!("time_zone is not a known time zone: {time_zone:?}"))?,
            days: BusinessDays::default(),
        };
        if session.open >= session.close {
            return Err(format!("open ({open}) must be before close ({close})"));
        }
        Ok(session)
    }

    /// The same session with these business days: the market is closed on its holidays.
    #[must_use]
    pub fn with_days(mut self, days: BusinessDays) -> Self {
        self.days = days;
        self
    }

    fn at(&self, date: NaiveDate, time: NaiveTime) -> DateTime<Utc> {
        let local = date.and_time(time);
        self.time_zone
            .from_local_datetime(&local)
            .earliest()
            .or_else(|| {
                self.time_zone
                    .from_local_datetime(&(local + chrono::Duration::hours(1)))
                    .earliest()
            })
            .expect("a local time an hour after a gap exists")
            .with_timezone(&Utc)
    }

    /// Whether the market is open at `at`: a business day in its time zone, at or after the open, before the close.
    pub fn is_open(&self, at: DateTime<Utc>) -> bool {
        let date = at.with_timezone(&self.time_zone).date_naive();
        self.days.is_business_day(date)
            && self.at(date, self.open) <= at
            && at < self.at(date, self.close)
    }

    /// The first close strictly after `after`.
    pub fn close_after(&self, after: DateTime<Utc>) -> DateTime<Utc> {
        let mut date = after.with_timezone(&self.time_zone).date_naive();
        loop {
            if self.days.is_business_day(date) {
                let close = self.at(date, self.close);
                if close > after {
                    return close;
                }
            }
            date = date.succ_opt().expect("date in range");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_is_open_on_business_days_in_its_own_time_zone() {
        let nasdaq = DailySession::parse("09:30", "16:00", "America/New_York").unwrap();
        let at = |text: &str| text.parse::<DateTime<Utc>>().unwrap();
        // Monday 2030-01-07; New York is UTC-5 in winter.
        assert!(!nasdaq.is_open(at("2030-01-07T14:29:59Z")));
        assert!(
            nasdaq.is_open(at("2030-01-07T14:30:00Z")),
            "09:30 local opens"
        );
        assert!(nasdaq.is_open(at("2030-01-07T20:59:59Z")));
        assert!(
            !nasdaq.is_open(at("2030-01-07T21:00:00Z")),
            "16:00 local closes"
        );
        assert!(!nasdaq.is_open(at("2030-01-12T15:00:00Z")), "Saturday");
        // Summer: UTC-4.
        assert!(nasdaq.is_open(at("2030-07-01T13:30:00Z")));
        assert_eq!(
            nasdaq.close_after(at("2030-01-07T15:00:00Z")),
            at("2030-01-07T21:00:00Z")
        );
        assert_eq!(
            nasdaq.close_after(at("2030-01-07T21:00:00Z")),
            at("2030-01-08T21:00:00Z"),
            "strictly after: at the close, the next day's"
        );
        assert_eq!(
            nasdaq.close_after(at("2030-01-11T22:00:00Z")),
            at("2030-01-14T21:00:00Z"),
            "Friday evening: Monday's"
        );
        assert!(DailySession::parse("16:00", "09:30", "UTC").is_err());
        assert!(DailySession::parse("09:30", "16:00", "Mars/Olympus").is_err());
    }

    fn d(text: &str) -> NaiveDate {
        text.parse().unwrap()
    }

    /// Thursday 2030-01-10 a holiday (SR-35).
    fn thursday_off() -> BusinessDays {
        BusinessDays::with_holidays([d("2030-01-10")])
    }

    #[test]
    fn holidays_are_not_business_days() {
        let days = thursday_off();
        assert!(!days.is_business_day(d("2030-01-10")));
        assert!(days.is_holiday(d("2030-01-10")));
        assert_eq!(
            days.add_business_days(d("2030-01-09"), 1),
            d("2030-01-11"),
            "Wednesday T+1: Friday"
        );
        assert_eq!(
            days.add_business_days(d("2030-01-10"), 0),
            d("2030-01-11"),
            "a holiday counts from the next day"
        );
        assert_eq!(days.next_business_day(d("2030-01-09")), d("2030-01-11"));
        assert_eq!(days.previous_business_day(d("2030-01-11")), d("2030-01-09"));
        assert_eq!(days.holidays().collect::<Vec<_>>(), [d("2030-01-10")]);
    }

    #[test]
    fn a_holiday_has_no_cutoff_and_closes_no_session() {
        let at = |text: &str| text.parse::<DateTime<Utc>>().unwrap();
        let calendar = CutoffCalendar::parse("17:30", "UTC")
            .unwrap()
            .with_days(thursday_off());
        assert_eq!(
            calendar.business_date(at("2030-01-09T18:00:00Z")),
            d("2030-01-11"),
            "after Wednesday's cutoff: Friday"
        );
        assert_eq!(
            calendar.business_date(at("2030-01-10T12:00:00Z")),
            d("2030-01-11")
        );
        assert_eq!(
            calendar.window(d("2030-01-11")),
            (at("2030-01-09T17:30:00Z"), at("2030-01-11T17:30:00Z")),
            "Friday's statement covers the holiday"
        );
        let session = DailySession::parse("09:00", "17:00", "UTC")
            .unwrap()
            .with_days(thursday_off());
        assert!(!session.is_open(at("2030-01-10T12:00:00Z")));
        assert_eq!(
            session.close_after(at("2030-01-09T18:00:00Z")),
            at("2030-01-11T17:00:00Z")
        );
    }

    #[test]
    fn t_plus_n_skips_weekends() {
        // 2030-01-07 is a Monday.
        assert_eq!(
            BusinessDays::default().add_business_days(d("2030-01-07"), 0),
            d("2030-01-07")
        );
        assert_eq!(
            BusinessDays::default().add_business_days(d("2030-01-07"), 2),
            d("2030-01-09")
        );
        assert_eq!(
            BusinessDays::default().add_business_days(d("2030-01-10"), 2),
            d("2030-01-14"),
            "Thursday + 2 is Monday"
        );
        assert_eq!(
            BusinessDays::default().add_business_days(d("2030-01-11"), 1),
            d("2030-01-14"),
            "Friday + 1 is Monday"
        );
        assert_eq!(
            BusinessDays::default().add_business_days(d("2030-01-11"), 5),
            d("2030-01-18"),
            "a full week"
        );
    }

    #[test]
    fn weekend_trades_count_from_monday() {
        assert_eq!(
            BusinessDays::default().add_business_days(d("2030-01-12"), 0),
            d("2030-01-14"),
            "Saturday T+0"
        );
        assert_eq!(
            BusinessDays::default().add_business_days(d("2030-01-13"), 2),
            d("2030-01-16"),
            "Sunday T+2"
        );
    }

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn copenhagen() -> CutoffCalendar {
        CutoffCalendar::parse("17:30", "Europe/Copenhagen").unwrap()
    }

    #[test]
    fn cutoffs_follow_the_local_time_zone_through_daylight_saving() {
        let calendar = copenhagen();
        assert_eq!(
            calendar.cutoff_at(d("2030-01-07")),
            at("2030-01-07T16:30:00Z")
        );
        assert_eq!(
            calendar.cutoff_at(d("2030-07-01")),
            at("2030-07-01T15:30:00Z")
        );
        assert!(CutoffCalendar::parse("25:00", "UTC").is_err());
        assert!(CutoffCalendar::parse("17:30", "Mars/Olympus").is_err());
        assert_eq!(
            CutoffCalendar::parse("17:30:15", "UTC").unwrap().cutoff,
            NaiveTime::from_hms_opt(17, 30, 15).unwrap()
        );
    }

    #[test]
    fn an_instant_belongs_to_the_first_business_day_whose_cutoff_is_after_it() {
        let calendar = copenhagen();
        // Monday 2030-01-07; the cutoff is 16:30 UTC.
        assert_eq!(
            calendar.business_date(at("2030-01-07T09:00:00Z")),
            d("2030-01-07")
        );
        assert_eq!(
            calendar.business_date(at("2030-01-07T16:29:59Z")),
            d("2030-01-07")
        );
        assert_eq!(
            calendar.business_date(at("2030-01-07T16:30:00Z")),
            d("2030-01-08")
        );
        // Friday after the cutoff, and the weekend, go to Monday.
        assert_eq!(
            calendar.business_date(at("2030-01-11T17:00:00Z")),
            d("2030-01-14")
        );
        assert_eq!(
            calendar.business_date(at("2030-01-12T12:00:00Z")),
            d("2030-01-14")
        );
        // 23:30 UTC on Sunday is already Monday in Copenhagen, before its cutoff.
        assert_eq!(
            calendar.business_date(at("2030-01-13T23:30:00Z")),
            d("2030-01-14")
        );
        assert_eq!(
            calendar.window(d("2030-01-14")),
            (at("2030-01-11T16:30:00Z"), at("2030-01-14T16:30:00Z"))
        );
    }
}
