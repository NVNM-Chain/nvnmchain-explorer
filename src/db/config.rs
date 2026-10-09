//! Which database a process opens, and in which role, from its environment.

use std::fmt;
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use serde::Serialize;

/// What this process does. `All` is today's single process; production runs
/// `Indexer` (one writer) and `Web` (N replicas) against Postgres.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    All,
    Web,
    Indexer,
}

impl FromStr for Role {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "all" => Ok(Role::All),
            "web" => Ok(Role::Web),
            "indexer" => Ok(Role::Indexer),
            other => bail!("ROLE={other:?}: expected all, web or indexer"),
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Role::All => "all",
            Role::Web => "web",
            Role::Indexer => "indexer",
        })
    }
}

/// The database to open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbTarget {
    /// A SQLite file path, from `DB_PATH` or a string that is not a URL.
    Sqlite(String),
    /// A Postgres URL, always with its scheme.
    Postgres(DbUrl),
}

impl DbTarget {
    /// Tell a Postgres URL from a SQLite path. `postgres://` and
    /// `postgresql://` are Postgres, and so is `[user[:password]@]host:port`
    /// with an optional `/dbname` and `?params`, which gains the scheme.
    /// Anything else is a path. A string that looks like Postgres but does
    /// not parse is an error, never a file.
    pub fn parse(raw: &str) -> Result<Self> {
        let url = if raw.starts_with("postgres://") || raw.starts_with("postgresql://") {
            raw.to_string()
        } else if looks_like_host_port(raw) {
            format!("postgres://{raw}")
        } else {
            return Ok(DbTarget::Sqlite(raw.to_string()));
        };
        url::Url::parse(&url).context("not a valid Postgres URL")?;
        Ok(DbTarget::Postgres(DbUrl(url)))
    }
}

/// `[user[:password]@]host:port[/dbname][?params]`, with the host a DNS name,
/// an IPv4 address or a bracketed IPv6 address, and the port 1 to 5 digits.
fn looks_like_host_port(raw: &str) -> bool {
    let before_query = raw.split('?').next().unwrap_or("");
    let after_user = match before_query.rsplit_once('@') {
        Some((user, rest)) if !user.contains('/') => rest,
        Some(_) => return false,
        None => before_query,
    };
    let host_port = after_user.split('/').next().unwrap_or("");
    let (host, port) = if let Some(rest) = host_port.strip_prefix('[') {
        match rest.split_once("]:") {
            Some((v6, port)) if v6.parse::<std::net::Ipv6Addr>().is_ok() => (v6, port),
            _ => return false,
        }
    } else {
        match host_port.rsplit_once(':') {
            Some(parts) => parts,
            None => return false,
        }
    };
    let host_ok = !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == ':');
    let port_ok = (1..=5).contains(&port.len()) && port.chars().all(|c| c.is_ascii_digit());
    host_ok && port_ok
}

/// A Postgres URL whose `Display` and `Debug` never show a password written
/// into it.
#[derive(Clone, PartialEq, Eq)]
pub struct DbUrl(pub String);

impl DbUrl {
    pub fn has_password(&self) -> bool {
        url::Url::parse(&self.0).is_ok_and(|u| u.password().is_some())
    }

    pub fn has_user(&self) -> bool {
        url::Url::parse(&self.0).is_ok_and(|u| !u.username().is_empty())
    }

    /// The `sslmode` (or `ssl-mode`) the URL names, the last of several, as
    /// sqlx reads it.
    pub fn ssl_mode(&self) -> Option<String> {
        let url = url::Url::parse(&self.0).ok()?;
        url.query_pairs()
            .filter(|(key, _)| key == "sslmode" || key == "ssl-mode")
            .last()
            .map(|(_, mode)| mode.into_owned())
    }
}

impl fmt::Display for DbUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match url::Url::parse(&self.0) {
            Ok(mut u) if u.password().is_some() => {
                let _ = u.set_password(Some("***"));
                f.write_str(u.as_str())
            }
            Ok(_) => f.write_str(&self.0),
            Err(_) => f.write_str("<unparsable URL>"),
        }
    }
}

impl fmt::Debug for DbUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DbUrl({self})")
    }
}

/// A secret that never shows in `Debug`.
#[derive(Clone)]
pub struct Secret(String);

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Timings and sizes with production defaults, which tests shorten.
#[derive(Clone, Debug)]
pub struct Tuning {
    /// The read pool's size; by role when `None`.
    pub pool_max: Option<u32>,
    /// The writer's lease: its session's `idle_session_timeout`.
    pub lease: std::time::Duration,
    /// How often a candidate asks for the lock.
    pub candidate_retry: std::time::Duration,
    /// How long a re-acquiring writer keeps losing `try_lock` before it gives
    /// up and exits with code 3.
    pub lost_after: std::time::Duration,
    /// How often the `Long` watchdog checks the lock.
    pub watchdog: std::time::Duration,
    /// Report the writer's fatal exits (3, 4) as errors instead of exiting,
    /// for tests that run several writers in one process.
    pub catch_exits: bool,
}

impl Default for Tuning {
    fn default() -> Self {
        use std::time::Duration;
        Tuning {
            pool_max: None,
            lease: Duration::from_secs(30),
            candidate_retry: Duration::from_secs(5),
            lost_after: Duration::from_secs(120),
            watchdog: Duration::from_secs(15),
            catch_exits: false,
        }
    }
}

/// Everything `db::open_with` needs from the environment.
#[derive(Clone, Debug)]
pub struct DbConfig {
    pub role: Role,
    pub target: DbTarget,
    /// `PGUSER`, applied when the URL names no user.
    pub user: Option<String>,
    /// `PGPASSWORD`, applied when the URL carries no password.
    pub password: Option<Secret>,
    /// `PGSSLMODE`, which the URL's own `sslmode` overrides.
    pub ssl_mode: Option<String>,
    /// `DB_WEB_ROLE`: the database user web replicas connect as, granted read
    /// access by the indexer.
    pub web_role: String,
    /// `FOLLOW_POLL_MS`: how often a web replica polls for new blocks.
    pub follow_poll: std::time::Duration,
    pub tuning: Tuning,
}

impl DbConfig {
    /// Read the configuration through `lookup`, which is `std::env::var` in
    /// production and a fake environment in tests.
    pub fn from_env(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let set = |key: &str| lookup(key).filter(|v| !v.trim().is_empty());
        let role = match set("ROLE") {
            Some(raw) => raw.parse().context("ROLE")?,
            None => Role::All,
        };
        let target = match set("DATABASE_URL") {
            Some(raw) => match DbTarget::parse(raw.trim()).context("DATABASE_URL")? {
                DbTarget::Sqlite(_) => {
                    bail!("DATABASE_URL is not a Postgres URL; use DB_PATH for SQLite")
                }
                pg => pg,
            },
            None => DbTarget::Sqlite(set("DB_PATH").unwrap_or_else(|| "explorer.db".into())),
        };
        if role != Role::All && matches!(target, DbTarget::Sqlite(_)) {
            bail!(
                "ROLE={role} needs Postgres: set DATABASE_URL. A SQLite file has one \
                 process, which runs as ROLE=all"
            );
        }
        let web_role = set("DB_WEB_ROLE").unwrap_or_else(|| "explorer_web".into());
        if !is_identifier(&web_role) {
            bail!("DB_WEB_ROLE={web_role:?}: use a plain identifier (letters, digits, _)");
        }
        let follow_ms: u64 = match set("FOLLOW_POLL_MS") {
            Some(v) => v.trim().parse().context("FOLLOW_POLL_MS")?,
            None => 500,
        };
        Ok(DbConfig {
            role,
            target,
            user: set("PGUSER"),
            password: lookup("PGPASSWORD").filter(|p| !p.is_empty()).map(Secret),
            ssl_mode: set("PGSSLMODE"),
            web_role,
            follow_poll: std::time::Duration::from_millis(follow_ms.max(50)),
            tuning: Tuning::default(),
        })
    }

    /// A SQLite configuration, as `db::open(path)` uses.
    pub fn sqlite(path: &str) -> Self {
        DbConfig {
            role: Role::All,
            target: DbTarget::Sqlite(path.to_string()),
            user: None,
            password: None,
            ssl_mode: None,
            web_role: "explorer_web".into(),
            follow_poll: std::time::Duration::from_millis(500),
            tuning: Tuning::default(),
        }
    }

    /// A Postgres configuration for `url` as `role`, the rest at its defaults.
    /// Where the URL has no user or password, sqlx takes `PGUSER` and
    /// `PGPASSWORD` from the process environment.
    pub fn postgres(url: DbUrl, role: Role) -> Self {
        DbConfig {
            role,
            target: DbTarget::Postgres(url),
            ..DbConfig::sqlite("")
        }
    }

    /// The connect options for the Postgres URL, credentials applied. This
    /// build has no TLS, so every connection is plaintext: run the database on
    /// a private network. Any `sslmode` but `disable`, the URL's or
    /// `PGSSLMODE`, is refused, never quietly downgraded: `prefer` and `allow`
    /// ask for TLS where the server offers it, and sqlx without TLS would
    /// connect them in plaintext. Naming none is plaintext.
    pub fn pg_options(&self) -> Result<sqlx::postgres::PgConnectOptions> {
        use sqlx::postgres::{PgConnectOptions, PgSslMode};
        let DbTarget::Postgres(url) = &self.target else {
            bail!("not a Postgres configuration");
        };
        let mut opts: PgConnectOptions = url.0.parse().with_context(|| format!("parse {url}"))?;
        if !url.has_user() {
            if let Some(user) = &self.user {
                opts = opts.username(user);
            }
        }
        if !url.has_password() {
            if let Some(Secret(password)) = &self.password {
                opts = opts.password(password);
            }
        } else if self.password.is_some() {
            tracing::warn!(
                "DATABASE_URL carries a password and PGPASSWORD is set; using the URL's"
            );
        }
        let asked = match url.ssl_mode() {
            Some(mode) => Some(("sslmode", mode)),
            None => self.ssl_mode.clone().map(|mode| ("PGSSLMODE", mode)),
        };
        if let Some((key, mode)) = asked {
            if !mode.trim().eq_ignore_ascii_case("disable") {
                bail!(
                    "{url}: {key}={mode} may use TLS, which this build does not support; \
                     connect over a private network with no sslmode (or sslmode=disable)"
                );
            }
        }
        Ok(opts.ssl_mode(PgSslMode::Disable))
    }
}

/// A name safe to put into a statement as an identifier.
pub(crate) fn is_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !s.starts_with(|c: char| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn the_default_is_one_process_on_a_sqlite_file() {
        let cfg = DbConfig::from_env(env(&[])).unwrap();
        assert_eq!(cfg.role, Role::All);
        assert_eq!(cfg.target, DbTarget::Sqlite("explorer.db".into()));
    }

    /// Fly, Render and the systemd unit set only `DB_PATH`.
    #[test]
    fn db_path_alone_still_opens_the_file() {
        let cfg = DbConfig::from_env(env(&[("DB_PATH", "/data/explorer.db")])).unwrap();
        assert_eq!(cfg.target, DbTarget::Sqlite("/data/explorer.db".into()));
    }

    #[test]
    fn a_role_is_read_from_role() {
        for (raw, role) in [("all", Role::All), (" ALL ", Role::All)] {
            let cfg = DbConfig::from_env(env(&[("ROLE", raw)])).unwrap();
            assert_eq!(cfg.role, role, "{raw}");
        }
        let err = DbConfig::from_env(env(&[("ROLE", "writer")])).unwrap_err();
        assert!(
            format!("{err:#}").contains("all, web or indexer"),
            "{err:#}"
        );
    }

    /// Every example in both directions: these are Postgres, normalized to a
    /// `postgres://` URL...
    #[test]
    fn host_and_port_is_a_postgres_url() {
        for (raw, url) in [
            (
                "postgres://db.internal:5432/x",
                "postgres://db.internal:5432/x",
            ),
            ("postgresql://db.internal/x", "postgresql://db.internal/x"),
            ("localhost:5432", "postgres://localhost:5432"),
            (
                "127.0.0.1:5432/explorer",
                "postgres://127.0.0.1:5432/explorer",
            ),
            ("[::1]:5432", "postgres://[::1]:5432"),
            (
                "explorer:explorer@localhost:5432/explorer",
                "postgres://explorer:explorer@localhost:5432/explorer",
            ),
            (
                "db.internal:5432/explorer?sslmode=disable",
                "postgres://db.internal:5432/explorer?sslmode=disable",
            ),
        ] {
            assert_eq!(
                DbTarget::parse(raw).unwrap(),
                DbTarget::Postgres(DbUrl(url.into())),
                "{raw}"
            );
        }
    }

    /// ...and these are SQLite paths.
    #[test]
    fn anything_else_is_a_sqlite_path() {
        for raw in [
            "explorer.db",
            "/data/explorer.db",
            ":memory:",
            "C:\\data\\explorer.db",
            "localhost",
            "file:x.db",
        ] {
            assert_eq!(
                DbTarget::parse(raw).unwrap(),
                DbTarget::Sqlite(raw.into()),
                "{raw}"
            );
        }
    }

    #[test]
    fn a_postgres_string_that_does_not_parse_is_an_error_not_a_file() {
        assert!(DbTarget::parse("postgres://[::1:5432/x").is_err());
    }

    #[test]
    fn database_url_wins_over_db_path() {
        let cfg = DbConfig::from_env(env(&[
            ("DATABASE_URL", "localhost:5432/explorer"),
            ("DB_PATH", "/data/explorer.db"),
        ]))
        .unwrap();
        assert_eq!(
            cfg.target,
            DbTarget::Postgres(DbUrl("postgres://localhost:5432/explorer".into()))
        );
    }

    #[test]
    fn database_url_must_name_postgres() {
        let err = DbConfig::from_env(env(&[("DATABASE_URL", "/data/explorer.db")])).unwrap_err();
        assert!(
            format!("{err:#}").contains("use DB_PATH for SQLite"),
            "{err:#}"
        );
    }

    #[test]
    fn a_password_never_shows_in_a_url_display() {
        for raw in [
            "postgres://explorer:s3cret@localhost:5432/explorer",
            "explorer:s3cret@localhost:5432/explorer",
        ] {
            let DbTarget::Postgres(url) = DbTarget::parse(raw).unwrap() else {
                panic!("{raw}");
            };
            assert!(!format!("{url}").contains("s3cret"), "{url}");
            assert!(!format!("{url:?}").contains("s3cret"), "{url:?}");
            assert!(format!("{url}").contains("localhost:5432"), "{url}");
        }
    }

    /// The manifests put credentials in PGUSER and PGPASSWORD, never the URL.
    #[test]
    fn credentials_come_from_pguser_and_pgpassword() {
        let cfg = DbConfig::from_env(env(&[
            ("DATABASE_URL", "postgres://db.internal:5432/explorer"),
            ("PGUSER", "explorer_indexer"),
            ("PGPASSWORD", "from-secret-manager"),
        ]))
        .unwrap();
        let opts = cfg.pg_options().unwrap();
        assert_eq!(opts.get_username(), "explorer_indexer");
    }

    /// Local runs may write them into the URL, which then wins.
    #[test]
    fn credentials_in_the_url_win() {
        let cfg = DbConfig::from_env(env(&[
            (
                "DATABASE_URL",
                "postgres://explorer:explorer@localhost:5432/explorer",
            ),
            ("PGUSER", "someone_else"),
            ("PGPASSWORD", "other"),
        ]))
        .unwrap();
        assert_eq!(cfg.pg_options().unwrap().get_username(), "explorer");
    }

    #[test]
    fn a_password_never_shows_in_the_config() {
        let cfg = DbConfig::from_env(env(&[
            ("DATABASE_URL", "postgres://db.internal:5432/x"),
            ("PGPASSWORD", "from-secret-manager"),
        ]))
        .unwrap();
        assert!(
            !format!("{cfg:?}").contains("from-secret-manager"),
            "{cfg:?}"
        );
    }

    /// This build has no TLS: every connection is plaintext, so any sslmode
    /// but `disable` is refused rather than quietly downgraded, `prefer` and
    /// `allow` included, which sqlx without TLS would connect in plaintext.
    #[test]
    fn connections_are_plaintext_and_a_url_asking_for_tls_is_refused() {
        for url in [
            "postgres://db.internal:5432/x",
            "postgres://db.internal:5432/x?sslmode=disable",
            "postgres://db.internal:5432/x?sslmode=DISABLE",
            "postgres://localhost:5432/x",
            "postgres:///x?host=/var/run/postgresql",
        ] {
            let cfg = DbConfig::from_env(env(&[("DATABASE_URL", url)])).unwrap();
            let opts = cfg.pg_options().unwrap_or_else(|e| panic!("{url}: {e:#}"));
            assert!(
                matches!(opts.get_ssl_mode(), sqlx::postgres::PgSslMode::Disable),
                "{url}"
            );
        }
        for url in [
            "postgres://db.internal:5432/x?sslmode=allow",
            "postgres://db.internal:5432/x?sslmode=prefer",
            "postgres://db.internal:5432/x?ssl-mode=prefer",
            "postgres://db.internal:5432/x?sslmode=disable&sslmode=prefer",
            "postgres://db.internal:5432/x?sslmode=require",
            "postgres://db.internal:5432/x?sslmode=verify-ca",
            "postgres://db.internal:5432/x?sslmode=verify-full",
        ] {
            let cfg = DbConfig::from_env(env(&[("DATABASE_URL", url)])).unwrap();
            let err = cfg.pg_options().map(|_| ()).map_err(|e| format!("{e:#}"));
            assert!(
                err.as_ref().is_err_and(|e| e.contains("TLS")),
                "{url}: {err:?}"
            );
        }
        // PGSSLMODE counts the same, and the URL's own sslmode overrides it.
        for (url, pgsslmode, accepted) in [
            ("postgres://db.internal:5432/x", "prefer", false),
            ("postgres://db.internal:5432/x", "require", false),
            ("postgres://db.internal:5432/x", "disable", true),
            (
                "postgres://db.internal:5432/x?sslmode=disable",
                "require",
                true,
            ),
        ] {
            let cfg = DbConfig::from_env(env(&[("DATABASE_URL", url), ("PGSSLMODE", pgsslmode)]))
                .unwrap();
            assert_eq!(
                cfg.pg_options().is_ok(),
                accepted,
                "{url} with PGSSLMODE={pgsslmode}"
            );
        }
    }

    #[test]
    fn the_split_roles_run_on_postgres() {
        for (raw, role) in [("web", Role::Web), ("indexer", Role::Indexer)] {
            let cfg = DbConfig::from_env(env(&[("ROLE", raw), ("DATABASE_URL", "localhost:5432")]))
                .unwrap();
            assert_eq!(cfg.role, role);
        }
    }

    /// One process owns a SQLite file; the split roles need a server.
    #[test]
    fn the_split_roles_need_postgres() {
        for role in ["web", "indexer"] {
            let err = DbConfig::from_env(env(&[("ROLE", role)])).unwrap_err();
            assert!(format!("{err:#}").contains("DATABASE_URL"), "{err:#}");
        }
    }
}
