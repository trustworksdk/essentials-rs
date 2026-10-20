//! The replay source and the recorder (plan item 6.13, SR-4).
//!
//! - **Replay:** a file of `(timestamp, referenceSymbol, price, volume)` rows plays on the exchange's clock. The
//!   file's first row lands when an instrument's source starts (at startup, the clock's start), every later row
//!   that much later on the clock, so the clock's speed sets the replay's. An instrument plays its `referenceSymbol`
//!   (else its `symbol`): its price at any time is that of the symbol's last row by then, and none before its first.
//!   With `loop`, the file starts over `loop_gap_ms` after its last row. The price is a pure function of the clock,
//!   so a replay reproduces exactly, however often it is read.
//! - **Recorder:** with `price_sources.finnhub.record_to`, every live price the exchange adopts is appended to a file
//!   in the replay format, stamped with the exchange's time, so a live session can be replayed later.
//! - **Formats:** CSV with the header `timestamp,referenceSymbol,price,volume` (`.csv`), or JSON lines with those
//!   fields (any other extension, e.g. `.jsonl`). Timestamps are RFC 3339.
//!
//! Recordings of live data stay local (`recordings/` is gitignored): vendor data must not be committed.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// One row of a replay file.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Row {
    pub timestamp: DateTime<Utc>,
    pub reference_symbol: String,
    pub price: f64,
    #[serde(default)]
    pub volume: f64,
}

fn is_csv(path: &Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("csv"))
}

/// A loaded replay file: per symbol, its prices by milliseconds after the file's first row.
#[derive(Clone, Debug)]
pub struct Replay {
    symbols: HashMap<String, Vec<(i64, f64)>>,
    /// With `loop`: the length of one pass, after which the file starts over.
    period_ms: Option<i64>,
}

impl Replay {
    /// Reads `path`; errors name the line. `loop_gap_ms` is `Some` to loop.
    pub fn load(path: &Path, loop_gap_ms: Option<u64>) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::parse(&text, is_csv(path), loop_gap_ms)
            .map_err(|e| format!("{}: {e}", path.display()))
    }

    pub fn parse(text: &str, csv: bool, loop_gap_ms: Option<u64>) -> Result<Self, String> {
        let mut rows = Vec::new();
        let mut lines = text
            .lines()
            .enumerate()
            .filter(|(_, l)| !l.trim().is_empty());
        if csv {
            let (_, header) = lines.next().ok_or("the file is empty")?;
            let columns: Vec<&str> = header.split(',').map(str::trim).collect();
            if columns != ["timestamp", "referenceSymbol", "price", "volume"] {
                return Err(format!(
                    "line 1: the header must be timestamp,referenceSymbol,price,volume, got {header:?}"
                ));
            }
        }
        for (index, line) in lines {
            let at = |e: String| format!("line {}: {e}", index + 1);
            let row: Row = if csv {
                let fields: Vec<&str> = line.split(',').map(str::trim).collect();
                let [timestamp, symbol, price, volume] = fields.as_slice() else {
                    return Err(at(format!("expected 4 fields, got {}", fields.len())));
                };
                Row {
                    timestamp: timestamp
                        .parse()
                        .map_err(|e| at(format!("timestamp: {e}")))?,
                    reference_symbol: symbol.to_string(),
                    price: price.parse().map_err(|e| at(format!("price: {e}")))?,
                    volume: volume.parse().map_err(|e| at(format!("volume: {e}")))?,
                }
            } else {
                serde_json::from_str(line).map_err(|e| at(e.to_string()))?
            };
            if !row.price.is_finite() || row.price <= 0.0 {
                return Err(at(format!("price must be positive, got {}", row.price)));
            }
            rows.push(row);
        }
        let first = rows
            .iter()
            .map(|r| r.timestamp)
            .min()
            .ok_or("the file has no rows")?;
        let last = rows
            .iter()
            .map(|r| r.timestamp)
            .max()
            .expect("there are rows");
        let mut symbols: HashMap<String, Vec<(i64, f64)>> = HashMap::new();
        for row in rows {
            let offset = (row.timestamp - first).num_milliseconds();
            symbols
                .entry(row.reference_symbol)
                .or_default()
                .push((offset, row.price));
        }
        for prices in symbols.values_mut() {
            // Stable: rows at the same instant keep their file order, the last one wins.
            prices.sort_by_key(|(offset, _)| *offset);
        }
        let period_ms = loop_gap_ms
            .map(|gap| (last - first).num_milliseconds() + i64::try_from(gap).unwrap_or(0));
        Ok(Self {
            symbols,
            period_ms: period_ms.map(|p| p.max(1)),
        })
    }

    pub fn has_symbol(&self, symbol: &str) -> bool {
        self.symbols.contains_key(symbol)
    }

    /// The symbol's price `elapsed_ms` after the replay started, and when (in milliseconds after the start) the row
    /// it comes from played; `None` before the symbol's first row.
    pub fn price_at(&self, symbol: &str, elapsed_ms: i64) -> Option<(f64, i64)> {
        let prices = self.symbols.get(symbol)?;
        if elapsed_ms < 0 {
            return None;
        }
        let (pass, within) = match self.period_ms {
            Some(period) => (elapsed_ms / period, elapsed_ms % period),
            None => (0, elapsed_ms),
        };
        let found = prices.partition_point(|(offset, _)| *offset <= within);
        match (found, self.period_ms) {
            (0, Some(period)) if pass > 0 => {
                // Before the symbol's first row of this pass: its last row of the previous pass still holds.
                let (offset, price) = *prices.last().expect("a symbol has rows");
                Some((price, (pass - 1) * period + offset))
            }
            (0, _) => None,
            (n, _) => {
                let (offset, price) = prices[n - 1];
                Some((price, pass * self.period_ms.unwrap_or(0) + offset))
            }
        }
    }
}

/// Appends adopted live prices to a file in the replay format, on its own thread so the exchange never waits on the
/// disk. A write error is logged once and recording stops; it never stops the exchange.
#[derive(Clone, Debug)]
pub struct Recorder {
    sender: std::sync::mpsc::Sender<Row>,
}

impl Recorder {
    /// Opens `path` for appending (writing the CSV header to a new `.csv` file) and starts the writer thread.
    pub fn start(path: &Path) -> Result<Self, String> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        let csv = is_csv(path);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if csv && file.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
            writeln!(file, "timestamp,referenceSymbol,price,volume")
                .map_err(|e| format!("{}: {e}", path.display()))?;
        }
        let (sender, receiver) = std::sync::mpsc::channel::<Row>();
        let name = path.display().to_string();
        std::thread::Builder::new()
            .name("price-recorder".into())
            .spawn(move || {
                let mut out = std::io::LineWriter::new(file);
                for row in receiver {
                    let line = if csv {
                        format!(
                            "{},{},{},{}",
                            row.timestamp
                                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            row.reference_symbol,
                            row.price,
                            row.volume
                        )
                    } else {
                        serde_json::to_string(&row).expect("a row serializes")
                    };
                    if let Err(error) = writeln!(out, "{line}") {
                        tracing::error!(file = name, %error, "price recording stopped");
                        return;
                    }
                }
            })
            .map_err(|e| format!("starting the recorder: {e}"))?;
        Ok(Self { sender })
    }

    pub fn record(&self, row: Row) {
        let _ = self.sender.send(row);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CSV: &str = "timestamp,referenceSymbol,price,volume
2030-01-07T14:30:00Z,AAPL,187.50,100
2030-01-07T14:30:02Z,MSFT,410.00,10
2030-01-07T14:30:05Z,AAPL,188.00,50
";

    #[test]
    fn prices_play_from_the_first_row_and_hold_until_the_next() {
        let replay = Replay::parse(CSV, true, None).unwrap();
        assert_eq!(replay.price_at("AAPL", -1), None);
        assert_eq!(replay.price_at("AAPL", 0), Some((187.5, 0)));
        assert_eq!(replay.price_at("AAPL", 4_999), Some((187.5, 0)));
        assert_eq!(replay.price_at("AAPL", 5_000), Some((188.0, 5_000)));
        assert_eq!(
            replay.price_at("AAPL", 999_999),
            Some((188.0, 5_000)),
            "the last price holds"
        );
        assert_eq!(
            replay.price_at("MSFT", 1_999),
            None,
            "nothing before the symbol's first row"
        );
        assert_eq!(replay.price_at("MSFT", 2_000), Some((410.0, 2_000)));
        assert_eq!(replay.price_at("NVDA", 0), None);
    }

    #[test]
    fn a_looping_replay_starts_over_after_the_gap() {
        // One pass: 5 s of rows plus a 1 s gap.
        let replay = Replay::parse(CSV, true, Some(1_000)).unwrap();
        assert_eq!(replay.price_at("AAPL", 5_999), Some((188.0, 5_000)));
        assert_eq!(
            replay.price_at("AAPL", 6_000),
            Some((187.5, 6_000)),
            "the second pass"
        );
        assert_eq!(replay.price_at("AAPL", 11_000), Some((188.0, 11_000)));
        assert_eq!(
            replay.price_at("MSFT", 7_000),
            Some((410.0, 2_000)),
            "until its first row of the next pass, the last one holds"
        );
    }

    #[test]
    fn json_lines_read_like_csv_and_errors_name_the_line() {
        let lines = r#"{"timestamp":"2030-01-07T14:30:00Z","referenceSymbol":"AAPL","price":187.5,"volume":100}

{"timestamp":"2030-01-07T14:30:05Z","referenceSymbol":"AAPL","price":188.0}"#;
        let replay = Replay::parse(lines, false, None).unwrap();
        assert_eq!(replay.price_at("AAPL", 5_000), Some((188.0, 5_000)));
        for (text, csv, error) in [
            ("timestamp,price\n", true, "line 1: the header"),
            (
                "timestamp,referenceSymbol,price,volume\n2030-01-07T14:30:00Z,AAPL,abc,1\n",
                true,
                "line 2: price",
            ),
            (
                "timestamp,referenceSymbol,price,volume\n2030-01-07T14:30:00Z,AAPL,-1,1\n",
                true,
                "line 2: price must be positive",
            ),
            ("{\"timestamp\":\"x\"}", false, "line 1:"),
            ("timestamp,referenceSymbol,price,volume\n", true, "no rows"),
        ] {
            let got = Replay::parse(text, csv, None).unwrap_err();
            assert!(got.contains(error), "{got}");
        }
    }

    #[test]
    fn the_recorder_writes_what_replay_reads() {
        let dir = std::env::temp_dir().join(format!("exchange-recorder-{}", uuid::Uuid::now_v7()));
        for name in ["session.csv", "session.jsonl"] {
            let path = dir.join(name);
            let recorder = Recorder::start(&path).unwrap();
            for (seconds, price) in [(0, 187.5), (3, 187.75)] {
                recorder.record(Row {
                    timestamp: "2030-01-07T14:30:00Z".parse::<DateTime<Utc>>().unwrap()
                        + chrono::Duration::seconds(seconds),
                    reference_symbol: "AAPL".into(),
                    price,
                    volume: 10.0,
                });
            }
            drop(recorder);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let replay = loop {
                if let Ok(replay) = Replay::load(&path, None)
                    && replay
                        .price_at("AAPL", 3_000)
                        .is_some_and(|(p, _)| p == 187.75)
                {
                    break replay;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "{name} never complete"
                );
                std::thread::sleep(std::time::Duration::from_millis(20));
            };
            assert_eq!(replay.price_at("AAPL", 0), Some((187.5, 0)), "{name}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
