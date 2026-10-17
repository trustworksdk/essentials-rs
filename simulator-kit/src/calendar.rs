//! Business days and daily cutoffs. Business days are Monday to Friday; there are no holiday calendars yet (decision
//! D2). Used for T+N settlement dates and for end-of-day statements.

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Utc, Weekday};
use chrono_tz::Tz;

pub fn is_business_day(date: NaiveDate) -> bool {
    !matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

/// The date `days` business days after `date`. A trade on a weekend counts from the following Monday, so T+0 of a
/// Saturday trade is Monday.
pub fn add_business_days(date: NaiveDate, days: u32) -> NaiveDate {
    let mut current = date;
    while !is_business_day(current) {
        current = current.succ_opt().expect("date in range");
    }
    let mut remaining = days;
    while remaining > 0 {
        current = current.succ_opt().expect("date in range");
        if is_business_day(current) {
            remaining -= 1;
        }
    }
    current
}

/// The business day after `date`.
pub fn next_business_day(date: NaiveDate) -> NaiveDate {
    add_business_days(date.succ_opt().expect("date in range"), 0)
}

/// The business day before `date`.
pub fn previous_business_day(date: NaiveDate) -> NaiveDate {
    let mut current = date.pred_opt().expect("date in range");
    while !is_business_day(current) {
        current = current.pred_opt().expect("date in range");
    }
    current
}

/// A daily cutoff: `cutoff` local time in `time_zone`, on business days (Monday to Friday), for end-of-day
/// statements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CutoffCalendar {
    pub cutoff: NaiveTime,
    pub time_zone: Tz,
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
        Ok(Self { cutoff, time_zone })
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
        if is_business_day(local) && at < self.cutoff_at(local) {
            local
        } else {
            next_business_day(local)
        }
    }

    /// The window of instants `date`'s statement covers, `[from, until)`: from the previous business day's cutoff to
    /// this one's.
    pub fn window(&self, date: NaiveDate) -> (DateTime<Utc>, DateTime<Utc>) {
        (
            self.cutoff_at(previous_business_day(date)),
            self.cutoff_at(date),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(text: &str) -> NaiveDate {
        text.parse().unwrap()
    }

    #[test]
    fn t_plus_n_skips_weekends() {
        // 2030-01-07 is a Monday.
        assert_eq!(add_business_days(d("2030-01-07"), 0), d("2030-01-07"));
        assert_eq!(add_business_days(d("2030-01-07"), 2), d("2030-01-09"));
        assert_eq!(
            add_business_days(d("2030-01-10"), 2),
            d("2030-01-14"),
            "Thursday + 2 is Monday"
        );
        assert_eq!(
            add_business_days(d("2030-01-11"), 1),
            d("2030-01-14"),
            "Friday + 1 is Monday"
        );
        assert_eq!(
            add_business_days(d("2030-01-11"), 5),
            d("2030-01-18"),
            "a full week"
        );
    }

    #[test]
    fn weekend_trades_count_from_monday() {
        assert_eq!(
            add_business_days(d("2030-01-12"), 0),
            d("2030-01-14"),
            "Saturday T+0"
        );
        assert_eq!(
            add_business_days(d("2030-01-13"), 2),
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
