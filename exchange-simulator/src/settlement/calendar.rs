//! Business-day arithmetic for T+N settlement. Business days are Monday to Friday; there are no holiday calendars yet
//! (decision D2).

use chrono::{Datelike, NaiveDate, Weekday};

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
}
