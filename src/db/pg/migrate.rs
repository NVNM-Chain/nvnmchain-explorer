//! The Postgres runner, its lock-free preflight, and the web role's grants.

use anyhow::{anyhow, Result};
use sqlx::{AssertSqlSafe, Connection, PgConnection, Row};

use super::q;
use super::DbError;
use crate::db::config::is_identifier;
use crate::db::migrations::{
    binary_version, check_applied, checksum, Migration, APPLIED_BY, MIGRATIONS,
};
use crate::db::now_ts;

pub(crate) enum PreflightError {
    /// The database is not one this binary may write: the process exits.
    Refused(anyhow::Error),
    /// It could not be asked; retried with backoff.
    Unavailable(anyhow::Error),
}

impl From<sqlx::Error> for PreflightError {
    fn from(e: sqlx::Error) -> Self {
        PreflightError::Unavailable(e.into())
    }
}

/// What the indexer refuses before it takes the lock, so a broken image turns
/// unready, or exits, and never replaces a working leader:
///
/// - a schema with tables but no `schema_migrations`, i.e. applied by hand;
/// - a version missing below the highest, i.e. rows edited by hand;
/// - a migration whose file changed after it was applied;
/// - a database newer than this binary (D > B).
pub(crate) async fn preflight(conn: &mut PgConnection) -> Result<(), PreflightError> {
    let row = sqlx::query(
        "SELECT to_regclass('schema_migrations') IS NOT NULL, \
         (SELECT COUNT(*) FROM pg_tables WHERE schemaname = current_schema())",
    )
    .fetch_one(&mut *conn)
    .await?;
    let (versioned, tables): (bool, i64) = (row.try_get(0)?, row.try_get(1)?);
    if !versioned {
        if tables > 0 {
            return Err(PreflightError::Refused(anyhow!(
                "the schema has {tables} table(s) but no schema_migrations: it was applied by \
                 hand. Drop the schema and re-index from the chain"
            )));
        }
        return Ok(());
    }
    let applied: Vec<(i64, String)> =
        sqlx::query("SELECT version, checksum FROM schema_migrations ORDER BY version")
            .fetch_all(&mut *conn)
            .await?
            .iter()
            .map(|r| Ok((r.try_get(0)?, r.try_get(1)?)))
            .collect::<Result<_, sqlx::Error>>()?;
    check_applied(&applied, |m| checksum(m.postgres)).map_err(PreflightError::Refused)?;
    Ok(())
}

fn unavailable(what: &str, e: impl std::fmt::Display) -> DbError {
    DbError::Unavailable(anyhow!("{what}: {e}"))
}

/// Apply the pending versions on the writer session, which holds the lock,
/// then re-apply the web role's grants. Any failure is `Unavailable`: the
/// session is dropped and the whole run retried, never the writes after it.
pub(crate) async fn run(conn: &mut PgConnection, web_role: &str) -> Result<(), DbError> {
    q::raw(
        conn,
        "schema_migrations",
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version BIGINT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL,
            applied_at BIGINT NOT NULL, applied_by TEXT NOT NULL)",
    )
    .await
    .map_err(|e| unavailable("create schema_migrations", e))?;
    let db: i64 = q::run(
        "schema version",
        sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
            .fetch_one(&mut *conn),
    )
    .await
    .map_err(|e| unavailable("read the schema version", e))?;
    // The writer re-runs the preflight under the lock, so D <= B here.
    let pending = usize::try_from(db)
        .ok()
        .and_then(|d| MIGRATIONS.get(d..))
        .ok_or_else(|| {
            unavailable(
                "schema version",
                format!("{db} is newer than this binary's {}", binary_version()),
            )
        })?;
    for m in pending {
        apply(conn, m).await.map_err(|e| {
            unavailable(
                &format!("migration {:04}_{}", m.version, m.name),
                describe(&e),
            )
        })?;
        tracing::info!("applied migration {:04}_{}", m.version, m.name);
    }
    grant(conn, web_role)
        .await
        .map_err(|e| unavailable(&format!("grant the web role {web_role:?}"), e))
}

fn describe(e: &sqlx::Error) -> String {
    format!("{e} {}", super::error::sqlstate(e).unwrap_or_default())
}

async fn record(conn: &mut PgConnection, m: &Migration) -> Result<(), sqlx::Error> {
    q::count_statement();
    sqlx::query(
        "INSERT INTO schema_migrations (version, name, checksum, applied_at, applied_by) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(m.version)
    .bind(m.name)
    .bind(checksum(m.postgres))
    .bind(now_ts())
    .bind(APPLIED_BY)
    .execute(conn)
    .await
    .map(|_| ())
}

/// One version in one transaction, its row included.
async fn apply(conn: &mut PgConnection, m: &Migration) -> Result<(), sqlx::Error> {
    let mut tx = conn.begin().await?;
    sqlx::raw_sql("SET LOCAL statement_timeout = 0; SET LOCAL lock_timeout = '5s'")
        .execute(&mut *tx)
        .await?;
    q::count_statement();
    sqlx::raw_sql(m.postgres).execute(&mut *tx).await?;
    record(&mut tx, m).await?;
    tx.commit().await
}

/// What web replicas may do: read everything, and write the two caches. Run
/// after every start, since a migration that recreates a table drops its
/// grants. A missing role is a warning, not an error: docker-compose and CI
/// create only `explorer`. Any other failure is returned, so the writer never
/// leads without its web role's grants.
async fn grant(conn: &mut PgConnection, web_role: &str) -> Result<(), String> {
    let row =
        sqlx::query("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1), current_schema()")
            .bind(web_role)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| describe(&e))?;
    let exists: bool = row.try_get(0).map_err(|e| describe(&e))?;
    let schema: String = row.try_get(1).map_err(|e| describe(&e))?;
    if !exists {
        tracing::warn!("web role {web_role:?} does not exist; skipping its grants");
        return Ok(());
    }
    if !is_identifier(web_role) || !is_identifier(&schema) {
        return Err(format!(
            "not plain identifiers: role {web_role:?}, schema {schema:?}"
        ));
    }
    let sql = format!(
            "GRANT USAGE ON SCHEMA \"{schema}\" TO \"{web_role}\";
             GRANT SELECT ON ALL TABLES IN SCHEMA \"{schema}\" TO \"{web_role}\";
             GRANT INSERT, UPDATE ON selector_names TO \"{web_role}\";
             GRANT UPDATE (trace_data) ON transactions TO \"{web_role}\";
             ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT SELECT ON TABLES TO \"{web_role}\""
    );
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(&mut *conn)
        .await
        .map_err(|e| describe(&e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A grant that fails for any reason but a missing role fails the run, so
    /// the writer never leads without its web role's grants, and the next run
    /// grants again. Needs `PG_TEST_URL`.
    #[tokio::test]
    #[ignore = "needs PG_TEST_URL; see AGENTS.md"]
    async fn a_failed_grant_fails_the_run() {
        let url = std::env::var("PG_TEST_URL").expect("PG_TEST_URL");
        let schema = format!("t_{}_900001", std::process::id());
        let role = format!("t_grant_{}", std::process::id());
        let mut admin = PgConnection::connect(&url).await.unwrap();
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};
             DROP ROLE IF EXISTS {role}; CREATE ROLE {role}"
        )))
        .execute(&mut admin)
        .await
        .unwrap();
        let mut conn = PgConnection::connect(&format!("{url}?options[search_path]={schema}"))
            .await
            .unwrap();
        run(&mut conn, &role).await.unwrap();

        // On a healthy session, a grant that cannot apply: a column it names
        // is gone.
        sqlx::raw_sql("ALTER TABLE transactions RENAME COLUMN trace_data TO trace_data_gone")
            .execute(&mut conn)
            .await
            .unwrap();
        let err = run(&mut conn, &role)
            .await
            .expect_err("a failed grant fails the run");
        assert!(matches!(err, DbError::Unavailable(_)), "{err}");
        assert!(err.to_string().contains("42703"), "{err}");

        sqlx::raw_sql(AssertSqlSafe(format!(
            "ALTER TABLE transactions RENAME COLUMN trace_data_gone TO trace_data;
             REVOKE ALL ON transactions FROM {role}"
        )))
        .execute(&mut conn)
        .await
        .unwrap();
        run(&mut conn, &role).await.unwrap();
        let granted: bool = sqlx::query_scalar(
            "SELECT has_column_privilege($1, 'transactions', 'trace_data', 'UPDATE')",
        )
        .bind(&role)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(granted, "the next run grants again");
        drop(conn);
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA {schema} CASCADE; DROP OWNED BY {role}; DROP ROLE {role}"
        )))
        .execute(&mut admin)
        .await
        .unwrap();
    }
}
