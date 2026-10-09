//! What a Postgres failure means to the writer.
//!
//! `Unavailable` is the default: anything not known to depend on a write's
//! content is retried until it succeeds, because a dropped block would be a
//! permanent hole (backfill only walks below `MIN(number)`). Only `Data`
//! reaches the caller, where the indexer's per-bundle fallback isolates the
//! bundle that caused it.

use std::fmt;

pub(crate) fn sqlstate(e: &sqlx::Error) -> Option<String> {
    e.as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.into_owned())
}

/// Whether a SQLSTATE depends on the write's content: a data exception (22),
/// an integrity constraint violation (23) or a cardinality violation (21000).
fn is_content(code: &str) -> bool {
    code.starts_with("22") || code.starts_with("23") || code == "21000"
}

/// A Postgres write's failure, as the writer reports it.
#[derive(Debug)]
pub enum DbError {
    /// The write's content was refused; the same write will fail again.
    Data(anyhow::Error),
    /// The database is unreachable or refused for a reason of its own. The
    /// writer retries these itself and never returns one from `write`.
    Unavailable(anyhow::Error),
    /// A chain-derived write in a process with no writer (`ROLE=web`).
    NotWriter,
    /// The writer must stop: the process exits with this code.
    Fatal(i32),
}

impl DbError {
    pub(crate) fn from_sqlx(what: &str, e: sqlx::Error) -> Self {
        let code = sqlstate(&e).unwrap_or_default();
        let err = anyhow::anyhow!("{what}: {e} {code}");
        if matches!(e, sqlx::Error::Encode(_)) || is_content(&code) {
            DbError::Data(err)
        } else {
            DbError::Unavailable(err)
        }
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Data(e) => write!(f, "{e:#}"),
            DbError::Unavailable(e) => write!(f, "database unavailable: {e:#}"),
            DbError::NotWriter => {
                f.write_str("this process has no writer (ROLE=web); the indexer writes chain data")
            }
            DbError::Fatal(code) => write!(f, "the writer stopped the process (exit {code})"),
        }
    }
}

impl std::error::Error for DbError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_errors_are_data() {
        for code in ["22003", "22P02", "23505", "23503", "21000"] {
            assert!(is_content(code), "{code}");
        }
        let e = DbError::from_sqlx("x", sqlx::Error::Encode("value too large".into()));
        assert!(matches!(e, DbError::Data(_)), "{e}");
    }

    /// Privileges, missing tables, a full disk, a read-only server, internal
    /// errors, cancellations and conflicts are about the database, not the
    /// block.
    #[test]
    fn everything_else_is_unavailable() {
        for code in [
            "42501", "42P01", "53100", "25006", "XX000", "57014", "57P01", "08006", "55P03",
            "40001", "40P01",
        ] {
            assert!(!is_content(code), "{code}");
        }
        for e in [
            sqlx::Error::Protocol("unexpected message".into()),
            sqlx::Error::PoolTimedOut,
        ] {
            let e = DbError::from_sqlx("x", e);
            assert!(matches!(e, DbError::Unavailable(_)), "{e}");
        }
    }
}
