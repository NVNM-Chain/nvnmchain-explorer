//! The schema versions both backends share, and the checks of what a
//! database has applied.
//!
//! Version 1 is the baseline: on SQLite it is `init_db` itself, with no file;
//! on Postgres it is `migrations/postgres/0001_baseline.sql`. From 0002 on,
//! every version is a twin pair, `migrations/sqlite/NNNN_name.sql` and
//! `migrations/postgres/NNNN_name.sql`, with the same number and name.
//! Version numbers are assigned on NVNM-Chain's `main` only (docs/database.md).

use anyhow::bail;
use sha3::{Digest, Sha3_256};

/// One schema version. `sqlite` is `None` for version 1, which is `init_db`.
pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub sqlite: Option<&'static str>,
    pub postgres: &'static str,
}

/// `NNNN => "name";`, one line per version, in order. The number is written
/// zero-padded because it is also the file name's prefix.
macro_rules! migrations {
    ($v1:literal => $n1:literal; $($v:literal => $n:literal;)*) => {
        #[allow(clippy::zero_prefixed_literal)]
        pub static MIGRATIONS: &[Migration] = &[
            Migration {
                version: $v1,
                name: $n1,
                sqlite: None,
                postgres: include_str!(concat!(
                    "../../migrations/postgres/", stringify!($v1), "_", $n1, ".sql"
                )),
            },
            $(Migration {
                version: $v,
                name: $n,
                sqlite: Some(include_str!(concat!(
                    "../../migrations/sqlite/", stringify!($v), "_", $n, ".sql"
                ))),
                postgres: include_str!(concat!(
                    "../../migrations/postgres/", stringify!($v), "_", $n, ".sql"
                )),
            },)*
        ];
    };
}

migrations! {
    0001 => "baseline";
}

/// B: the newest version this binary knows.
pub fn binary_version() -> i64 {
    MIGRATIONS.last().map_or(0, |m| m.version)
}

/// sha3-256 of a file's text, hex, with CRLF read as LF so a checkout's line
/// endings never look like an edit.
pub fn checksum(text: &str) -> String {
    hex::encode(Sha3_256::digest(text.replace("\r\n", "\n").as_bytes()))
}

/// Whether a web replica may serve a database at version `db`: once the
/// indexer has applied every migration this binary knows.
pub fn web_ready(db: i64) -> bool {
    db >= binary_version()
}

/// Check a database's applied versions, `(version, checksum)` in order,
/// against this binary's list, `sum` giving each file's expected checksum, and
/// return D. The runners record versions in order, so a gap means hand edits.
pub(crate) fn check_applied(
    applied: &[(i64, String)],
    sum: impl Fn(&Migration) -> String,
) -> anyhow::Result<i64> {
    for ((v, _), want) in applied.iter().zip(1..) {
        if *v != want {
            bail!(
                "schema_migrations has version {v} but not {want}: its rows were edited by \
                 hand, so which files ran is unknown"
            );
        }
    }
    let db = applied.len() as i64;
    let binary = binary_version();
    if db > binary {
        bail!(
            "the database is at schema version {db}, newer than this binary's {binary}; \
             deploy a release at version {db} or later (the only way back is forward)"
        );
    }
    for ((v, stored), m) in applied.iter().zip(MIGRATIONS) {
        if *stored != sum(m) {
            bail!("migration {v} was edited after it was applied");
        }
    }
    Ok(db)
}

/// Who applied a version, for the `applied_by` column.
pub(crate) const APPLIED_BY: &str = concat!("nvnmchain-explorer ", env!("CARGO_PKG_VERSION"));

/// Every rule the list breaks: versions run 1, 2, 3… with no gap or repeat.
pub fn check_list(list: &[Migration]) -> Vec<String> {
    list.iter()
        .zip(1..)
        .filter(|(m, want)| m.version != *want)
        .map(|(m, want)| {
            format!(
                "{:04}_{}: expected version {want:04}; versions run 1, 2, 3… with no gap or repeat",
                m.version, m.name
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_list_is_contiguous_from_one() {
        let list = [entry(1, "baseline"), entry(3, "skipped")];
        let found = check_list(&list);
        assert!(found.iter().any(|e| e.contains("0003")), "{found:?}");
        let list = [entry(1, "baseline"), entry(2, "a"), entry(2, "b")];
        let found = check_list(&list);
        assert!(found.iter().any(|e| e.contains("0002")), "{found:?}");
    }

    fn entry(version: i64, name: &'static str) -> Migration {
        Migration {
            version,
            name,
            sqlite: (version > 1).then_some(""),
            postgres: "",
        }
    }

    /// The real list: numbered in order, and every file on disk in it.
    #[test]
    fn the_shipped_migrations_follow_the_rules() {
        assert_eq!(check_list(MIGRATIONS), Vec::<String>::new());
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        for dialect in ["sqlite", "postgres"] {
            let Ok(dir) = std::fs::read_dir(root.join(dialect)) else {
                continue;
            };
            for file in dir {
                let name = file.unwrap().file_name().into_string().unwrap();
                let listed = MIGRATIONS.iter().any(|m| {
                    name == format!("{:04}_{}.sql", m.version, m.name)
                        && (dialect == "postgres" || m.version > 1)
                });
                assert!(
                    listed,
                    "migrations/{dialect}/{name} is not in migrations!{{}}"
                );
            }
        }
    }

    #[test]
    fn a_checksum_ignores_line_endings() {
        assert_eq!(checksum("a\r\nb\r\n"), checksum("a\nb\n"));
        assert_ne!(checksum("a\nb\n"), checksum("a\nc\n"));
        assert_eq!(checksum("").len(), 64);
    }

    #[test]
    fn web_is_ready_once_the_database_has_caught_up() {
        let b = binary_version();
        assert!(!web_ready(b - 1));
        assert!(web_ready(b));
        assert!(web_ready(b + 1), "an older web replica keeps serving");
    }
}
