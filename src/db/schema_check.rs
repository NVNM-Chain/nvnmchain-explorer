//! Refuse to open a database whose tables no longer match `init_db`.
//!
//! `init_db` is idempotent DDL: `CREATE … IF NOT EXISTS` never changes a table
//! that already exists, so an upstream change to an existing table's columns or
//! keys leaves every deployed database silently on the old shape. This compares
//! the opened database with what `init_db` builds from nothing.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use anyhow::{bail, Context, Result};
use rusqlite::{Connection, OpenFlags};

/// Compare `conn` with a fresh `init_db`, and fail naming every difference.
///
/// Tables only in `conn` are ignored: upstream retires a table with `DROP TABLE
/// IF EXISTS`, which `init_db` has already run. Retired indexes are gone for the
/// same reason, so a created index `conn` has and `init_db` does not is drift.
pub(super) fn verify(conn: &Connection) -> Result<()> {
    let actual = shape(conn).context("read the database's schema")?;
    let report = report(&expected()?, &actual)?;
    if !report.is_empty() {
        bail!(
            "the database's tables differ from what this build creates, so it was not opened \
             (see docs/database.md):\n{report}"
        );
    }
    Ok(())
}

/// Put table drift in front of `init_db`'s own error when the database at
/// `path` has any, since an old table can fail `init_db` before `verify` runs.
///
/// Diagnosed after the failure rather than checked before `init_db`: an
/// in-place fix `init_db` may one day carry (a guarded `ALTER TABLE … ADD
/// COLUMN`) would otherwise be refused before it ran. Only tables both sides
/// have count: `init_db` creates missing ones, and may have stopped before
/// creating or dropping an index.
pub(super) fn explain(path: &str, err: anyhow::Error) -> anyhow::Error {
    match table_drift_at(path) {
        Ok(report) if report.is_empty() => err,
        Ok(report) => err.context(format!(
            "init_db failed on a database whose tables differ from what this build creates \
             (see docs/database.md):\n{report}"
        )),
        Err(e) => {
            tracing::warn!("{path}: could not look for table drift behind init_db's error: {e:#}");
            err
        }
    }
}

/// The report for the tables both the database at `path` and `init_db` have,
/// with indexes left out.
fn table_drift_at(path: &str) -> Result<String> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .context("open it read-only")?;
    let mut actual = shape(&conn).context("read the database's schema")?;
    let mut expected = expected()?;
    expected.retain(|name, _| actual.contains_key(name));
    for table in expected.values_mut().chain(actual.values_mut()) {
        table.created.clear();
    }
    report(&expected, &actual)
}

/// What `init_db` builds from nothing.
fn expected() -> Result<BTreeMap<String, Table>> {
    shape(&super::init_db(":memory:").context("build the expected schema")?)
        .context("read the expected schema")
}

/// Every difference, each table's followed by its recovery; empty when none.
fn report(expected: &BTreeMap<String, Table>, actual: &BTreeMap<String, Table>) -> Result<String> {
    let mut report = String::new();
    for (name, want) in expected {
        let (table, index) = match actual.get(name) {
            None => (vec!["missing".to_string()], Vec::new()),
            Some(have) => (table_drift(have, want), index_drift(have, want)),
        };
        if table.is_empty() && index.is_empty() {
            continue;
        }
        for line in &table {
            writeln!(report, "{name}: {line}")?;
        }
        for (_, line) in &index {
            writeln!(report, "{name}: {line}")?;
        }
        if !table.is_empty() {
            writeln!(report, "  recovery: {}", recovery(name))?;
        }
        for index in index.iter().filter_map(|(drop, _)| drop.as_ref()) {
            writeln!(
                report,
                "  recovery for {index}: `DROP INDEX \"{}\";` and restart; \
                 init_db rebuilds it from the existing rows.",
                index.replace('"', "\"\"")
            )?;
        }
    }
    Ok(report.trim_end().to_string())
}

/// What to do about a table whose columns or keys drifted. None of these is a
/// statement to paste: each one discards data, and the operator decides when.
fn recovery(table: &str) -> &'static str {
    match table {
        "blocks" | "transactions" | "transfer_events" | "token_metadata" | "kv" => {
            "no watermark exists, and the backfill only walks below MIN(blocks.number): \
             stop the explorer and re-index from an empty database file."
        }
        "token_balances" => {
            "drop the table; repair_derived_tables rebuilds it from transfer_events \
             and genesis_balances on the next start."
        }
        "counters" => "drop the table; seed_counters recounts it on open.",
        "anchoring_events" => {
            "drop the table and delete the kv key anchoring_backfilled_to; \
             the anchoring backfill refills it."
        }
        "genesis_balances" => {
            "drop it and token_balances, and delete the kv key genesis_balances_cursor; \
             the genesis pass and repair_derived_tables refill both."
        }
        "selector_names" => "drop the table; it is a cache of directory lookups.",
        _ => {
            "No known recovery. Work it out before deploying, and add it to the map \
             in src/db/schema_check.rs."
        }
    }
}

struct Column {
    name: String,
    decl_type: String,
    not_null: bool,
    default: Option<String>,
}

struct Index {
    name: String,
    unique: bool,
    partial: bool,
    /// Key columns in order: a name or `<expr>`, with direction and collation.
    keys: Vec<String>,
    /// `sqlite_master.sql`, normalized; it alone shows a predicate or an expression.
    sql: String,
}

struct Table {
    columns: Vec<Column>,
    primary_key: Vec<String>,
    autoincrement: bool,
    /// The key lists of `UNIQUE` constraints, sorted.
    uniques: Vec<Vec<String>>,
    /// Indexes written as `CREATE INDEX`, by name.
    created: BTreeMap<String, Index>,
}

/// Every table's shape as SQLite's own catalog describes it.
fn shape(conn: &Connection) -> Result<BTreeMap<String, Table>> {
    let mut stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let tables = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = BTreeMap::new();
    for (name, sql) in tables {
        let table = read_table(conn, &name, &sql).with_context(|| format!("table {name}"))?;
        out.insert(name, table);
    }
    Ok(out)
}

fn read_table(conn: &Connection, name: &str, sql: &str) -> Result<Table> {
    let mut stmt = conn.prepare(
        "SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?1) ORDER BY cid",
    )?;
    let mut keyed = Vec::new();
    let columns = stmt
        .query_map([name], |r| {
            Ok((
                Column {
                    name: r.get(0)?,
                    decl_type: r.get::<_, String>(1)?.to_uppercase(),
                    not_null: r.get(2)?,
                    default: r.get(3)?,
                },
                r.get::<_, i64>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|(column, pk)| {
            if pk > 0 {
                keyed.push((pk, column.name.clone()));
            }
            column
        })
        .collect();
    keyed.sort();

    let mut stmt =
        conn.prepare("SELECT name, \"unique\", origin, partial FROM pragma_index_list(?1)")?;
    let listed = stmt
        .query_map([name], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, bool>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, bool>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut uniques = Vec::new();
    let mut created = BTreeMap::new();
    let mut pk_index = None;
    for (index, unique, origin, partial) in listed {
        let keys = index_keys(conn, &index)?;
        match origin.as_str() {
            "pk" => pk_index = Some(keys),
            "u" => uniques.push(keys),
            "c" => {
                let sql: String = conn.query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
                    [&index],
                    |r| r.get(0),
                )?;
                created.insert(
                    index.clone(),
                    Index {
                        name: index,
                        unique,
                        partial,
                        keys,
                        sql: normalize(&sql),
                    },
                );
            }
            _ => {}
        }
    }
    uniques.sort();

    Ok(Table {
        columns,
        // Any key but a rowid alias (`INTEGER PRIMARY KEY`) has an index, and
        // only it shows the direction and collation.
        primary_key: pk_index
            .unwrap_or_else(|| keyed.into_iter().map(|(_, column)| column).collect()),
        autoincrement: normalize(sql).contains("autoincrement"),
        uniques,
        created,
    })
}

fn index_keys(conn: &Connection, index: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT cid, name, \"desc\", coll FROM pragma_index_xinfo(?1) WHERE key = 1 ORDER BY seqno",
    )?;
    let keys = stmt
        .query_map([index], |r| {
            let cid: i64 = r.get(0)?;
            let mut key = match r.get::<_, Option<String>>(1)? {
                Some(column) if cid >= 0 => column,
                _ => "<expr>".to_string(),
            };
            if r.get::<_, bool>(2)? {
                key.push_str(" DESC");
            }
            let coll: String = r.get(3)?;
            // In one case: SQLite matches collation names case-insensitively.
            if !coll.eq_ignore_ascii_case("BINARY") {
                write!(key, " COLLATE {}", coll.to_uppercase()).expect("write to a String");
            }
            Ok(key)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(keys)
}

/// SQL with case and whitespace dropped outside string literals, so a
/// reformatted statement in `init_db` compares equal to the one a database
/// stored before it.
fn normalize(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut quoted = false;
    for c in sql.chars() {
        if c == '\'' {
            quoted = !quoted;
        }
        if quoted || c == '\'' {
            out.push(c);
        } else if !c.is_whitespace() {
            out.extend(c.to_lowercase());
        }
    }
    out
}

fn list(columns: &[String]) -> String {
    format!("({})", columns.join(", "))
}

/// Differences in columns and keys: the ones only re-deriving the table fixes.
fn table_drift(have: &Table, want: &Table) -> Vec<String> {
    let mut out = Vec::new();
    for w in &want.columns {
        let Some(h) = have.columns.iter().find(|h| h.name == w.name) else {
            out.push(format!("column {}: missing", w.name));
            continue;
        };
        if h.decl_type != w.decl_type {
            out.push(format!(
                "column {}: type {}, expected {}",
                w.name, h.decl_type, w.decl_type
            ));
        }
        if h.not_null != w.not_null {
            let (is, expected) = if w.not_null {
                ("nullable", "not null")
            } else {
                ("not null", "nullable")
            };
            out.push(format!("column {}: {is}, expected {expected}", w.name));
        }
        if h.default != w.default {
            let show = |d: &Option<String>| d.clone().unwrap_or_else(|| "none".into());
            out.push(format!(
                "column {}: default {}, expected {}",
                w.name,
                show(&h.default),
                show(&w.default)
            ));
        }
    }
    for h in &have.columns {
        if !want.columns.iter().any(|w| w.name == h.name) {
            out.push(format!("column {}: not in this build", h.name));
        }
    }
    let names = |t: &Table| t.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
    let (have_names, want_names) = (names(have), names(want));
    if have_names != want_names {
        let (mut a, mut b) = (have_names.clone(), want_names.clone());
        a.sort();
        b.sort();
        if a == b {
            out.push(format!(
                "table: column order {}, expected {}",
                list(&have_names),
                list(&want_names)
            ));
        }
    }
    if have.primary_key != want.primary_key {
        out.push(format!(
            "table: primary key {}, expected {}",
            list(&have.primary_key),
            list(&want.primary_key)
        ));
    }
    if have.autoincrement != want.autoincrement {
        out.push(if want.autoincrement {
            "table: autoincrement missing".to_string()
        } else {
            "table: autoincrement, not in this build".to_string()
        });
    }
    for unique in &want.uniques {
        if !have.uniques.contains(unique) {
            out.push(format!("table: missing unique {}", list(unique)));
        }
    }
    for unique in &have.uniques {
        if !want.uniques.contains(unique) {
            out.push(format!("table: unique {}, not in this build", list(unique)));
        }
    }
    out
}

/// Differences in created indexes, each with the index to drop when the
/// database has one: `init_db` then rebuilds it, with no data re-derived.
fn index_drift(have: &Table, want: &Table) -> Vec<(Option<String>, String)> {
    let mut out = Vec::new();
    for (name, w) in &want.created {
        match have.created.get(name) {
            None => out.push((None, format!("index {name}: missing"))),
            Some(h)
                if (h.unique, h.partial, &h.keys, &h.sql)
                    != (w.unique, w.partial, &w.keys, &w.sql) =>
            {
                out.push((
                    Some(name.clone()),
                    format!("index {name}: definition differs"),
                ));
            }
            Some(_) => {}
        }
    }
    for h in have.created.values() {
        if !want.created.contains_key(&h.name) {
            out.push((
                Some(h.name.clone()),
                format!("index {}: not created by this build", h.name),
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::sqlite as db;

    /// A database file at `name` in a fresh directory, set up by `prepare`
    /// before the explorer first opens it.
    fn prepared(prepare: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("drift.db").to_str().unwrap().to_string();
        Connection::open(&path)
            .unwrap()
            .execute_batch(prepare)
            .unwrap();
        (dir, path)
    }

    /// The same, but starting from a database `init_db` already wrote, as a
    /// deployed one would be.
    fn edited(edit: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("drift.db").to_str().unwrap().to_string();
        db::init_db(&path).unwrap().execute_batch(edit).unwrap();
        (dir, path)
    }

    /// The whole error `db::open` refuses with.
    fn refusal(path: &str) -> String {
        match db::open(path) {
            Ok(_) => panic!("{path}: opened despite the drift"),
            Err(e) => format!("{e:#}"),
        }
    }

    fn assert_has(msg: &str, wanted: &[&str]) {
        for w in wanted {
            assert!(msg.contains(w), "missing {w:?} in:\n{msg}");
        }
        assert!(!msg.contains("DROP TABLE"), "suggests a DROP TABLE:\n{msg}");
    }

    #[test]
    fn a_fresh_database_opens() {
        let dir = tempfile::tempdir().unwrap();
        db::open(dir.path().join("fresh.db").to_str().unwrap()).unwrap();
    }

    #[test]
    fn reopening_a_database_passes() {
        let (_dir, path) = edited("");
        db::open(&path).unwrap();
    }

    #[test]
    fn a_missing_column_is_table_drift() {
        let (_dir, path) =
            prepared("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')");
        assert_has(
            &refusal(&path),
            &[
                "kv: column updated_at: missing",
                "re-index from an empty database file",
            ],
        );
    }

    #[test]
    fn a_changed_default_is_table_drift() {
        let (_dir, path) =
            prepared("CREATE TABLE counters (name TEXT PRIMARY KEY, n INTEGER NOT NULL DEFAULT 1)");
        assert_has(
            &refusal(&path),
            &["counters: column n: default 1, expected 0", "seed_counters"],
        );
    }

    /// Upstream's f6a5c27, which old databases never picked up.
    #[test]
    fn a_missing_unique_is_table_drift() {
        let (_dir, path) = prepared(
            "CREATE TABLE transfer_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                log_index INTEGER NOT NULL DEFAULT 0,
                token_addr BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                amount TEXT NOT NULL,
                timestamp INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL DEFAULT 0
            )",
        );
        assert_has(
            &refusal(&path),
            &[
                "transfer_events: table: missing unique (block_number, log_index)",
                "re-index from an empty database file",
            ],
        );
    }

    #[test]
    fn a_dropped_autoincrement_is_table_drift() {
        let (_dir, path) = prepared(
            "CREATE TABLE transfer_events (
                id INTEGER PRIMARY KEY,
                tx_hash BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                log_index INTEGER NOT NULL DEFAULT 0,
                token_addr BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                amount TEXT NOT NULL,
                timestamp INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL DEFAULT 0,
                UNIQUE (block_number, log_index)
            )",
        );
        assert_has(&refusal(&path), &["transfer_events: table: autoincrement"]);
    }

    /// The column metadata and key list match; only the key's index shows it.
    /// The collation is reported in one case, however the table spelled it.
    #[test]
    fn a_collated_primary_key_is_table_drift() {
        let (_dir, path) = prepared(
            "CREATE TABLE kv (
                key TEXT collate nocase PRIMARY KEY,
                value TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL DEFAULT 0
            )",
        );
        assert_has(
            &refusal(&path),
            &[
                "kv: table: primary key (key COLLATE NOCASE), expected (key)",
                "re-index from an empty database file",
            ],
        );
    }

    /// `DESC` makes an `INTEGER PRIMARY KEY` an ordinary key, not the rowid.
    #[test]
    fn a_descending_integer_primary_key_is_table_drift() {
        let (_dir, path) = prepared(
            "CREATE TABLE blocks (
                number INTEGER PRIMARY KEY DESC,
                hash BLOB NOT NULL UNIQUE,
                parent_hash BLOB NOT NULL,
                timestamp INTEGER NOT NULL,
                timestamp_ms INTEGER NOT NULL DEFAULT 0,
                gas_used INTEGER NOT NULL DEFAULT 0,
                gas_limit INTEGER NOT NULL DEFAULT 0,
                miner BLOB NOT NULL DEFAULT X'',
                tx_count INTEGER NOT NULL DEFAULT 0,
                base_fee TEXT NOT NULL DEFAULT '0',
                size INTEGER NOT NULL DEFAULT 0,
                extra_data TEXT NOT NULL DEFAULT '',
                epoch INTEGER NOT NULL DEFAULT 0,
                view INTEGER NOT NULL DEFAULT 0,
                proposer BLOB NOT NULL DEFAULT X'',
                created_at INTEGER NOT NULL DEFAULT 0
            )",
        );
        assert_has(
            &refusal(&path),
            &[
                "blocks: table: primary key (number DESC), expected (number)",
                "re-index from an empty database file",
            ],
        );
    }

    /// Copilot's case: `seed_counters` fails on the old table before the
    /// check runs, and the report still names the drift and its recovery.
    #[test]
    fn drift_that_fails_init_db_is_reported_with_its_error() {
        let (_dir, path) = prepared("CREATE TABLE counters (name TEXT PRIMARY KEY)");
        assert_has(
            &refusal(&path),
            &[
                "init_db failed on a database whose tables differ",
                "counters: column n: missing",
                "seed_counters recounts it",
                "no column named n",
            ],
        );
    }

    /// `init_db`'s `CREATE INDEX` over the missing column fails first.
    #[test]
    fn an_index_over_a_missing_column_is_reported_as_table_drift() {
        let (_dir, path) = prepared(
            "CREATE TABLE transactions (
                hash BLOB PRIMARY KEY,
                block_number INTEGER NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB,
                status INTEGER NOT NULL DEFAULT 1,
                gas_used INTEGER NOT NULL DEFAULT 0,
                base_fee TEXT NOT NULL DEFAULT '0',
                contract_address BLOB,
                fee_token BLOB,
                fee_amount TEXT NOT NULL DEFAULT '0',
                input TEXT NOT NULL DEFAULT '0x',
                raw BLOB,
                trace_data TEXT,
                receipt_data TEXT,
                timestamp INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL DEFAULT 0
            )",
        );
        // SQLite's error quotes the whole batch, `DROP TABLE IF EXISTS` included.
        let msg = refusal(&path);
        let (report, _) = msg
            .split_once("no such column: position")
            .unwrap_or_else(|| panic!("init_db's own error is gone:\n{msg}"));
        assert_has(
            report,
            &[
                "transactions: column position: missing",
                "re-index from an empty database file",
            ],
        );
    }

    #[test]
    fn an_init_db_failure_without_drift_keeps_its_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let own = format!("{:#}", db::init_db(path).unwrap_err());
        let msg = refusal(path);
        assert_eq!(msg, own);
        assert!(!msg.contains("differ"), "claims drift:\n{msg}");
    }

    /// The SQL quotes the name; the labels keep it as written.
    #[test]
    fn an_extra_index_with_an_awkward_name_gets_quoted_sql() {
        let (_dir, path) = edited(
            "CREATE INDEX \"my index\" ON kv(value);
             CREATE INDEX \"odd\"\"name\" ON kv(updated_at)",
        );
        assert_has(
            &refusal(&path),
            &[
                "kv: index my index: not created by this build",
                "recovery for my index: `DROP INDEX \"my index\";`",
                "kv: index odd\"name: not created by this build",
                "recovery for odd\"name: `DROP INDEX \"odd\"\"name\";`",
            ],
        );
    }

    #[test]
    fn an_index_on_other_columns_is_index_drift() {
        let (_dir, path) = edited(
            "DROP INDEX idx_blocks_timestamp;
             CREATE INDEX idx_blocks_timestamp ON blocks(number)",
        );
        let msg = refusal(&path);
        assert_has(
            &msg,
            &[
                "blocks: index idx_blocks_timestamp: definition differs",
                "DROP INDEX \"idx_blocks_timestamp\";",
            ],
        );
        assert!(
            !msg.contains("re-index"),
            "index drift needs no re-index:\n{msg}"
        );
    }

    #[test]
    fn a_changed_partial_predicate_is_index_drift() {
        let (_dir, path) = edited(
            "DROP INDEX idx_tb_holding;
             CREATE INDEX idx_tb_holding
                 ON token_balances(token_addr, LENGTH(balance) DESC, balance DESC)
                 WHERE balance != '0'",
        );
        assert_has(
            &refusal(&path),
            &[
                "token_balances: index idx_tb_holding: definition differs",
                "DROP INDEX \"idx_tb_holding\";",
            ],
        );
    }

    #[test]
    fn an_extra_index_on_a_known_table_is_index_drift() {
        let (_dir, path) = edited("CREATE INDEX idx_kv_value ON kv(value)");
        assert_has(
            &refusal(&path),
            &[
                "kv: index idx_kv_value: not created by this build",
                "DROP INDEX \"idx_kv_value\";",
            ],
        );
    }

    #[test]
    fn reformatting_an_index_is_not_drift() {
        let (_dir, path) = edited(
            "DROP INDEX idx_tx_from_block;
             create index idx_tx_from_block on transactions (from_addr,block_number,  position)",
        );
        db::open(&path).unwrap();
    }

    #[test]
    fn a_table_only_in_the_database_is_ignored() {
        let (_dir, path) = edited(
            "CREATE TABLE leftover_cache (k TEXT PRIMARY KEY, v BLOB);
             CREATE INDEX idx_leftover_v ON leftover_cache(v)",
        );
        db::open(&path).unwrap();
    }

    #[test]
    fn every_drift_is_listed_with_each_tables_recovery() {
        let (_dir, path) = prepared(
            "CREATE TABLE counters (name TEXT PRIMARY KEY, n INTEGER NOT NULL DEFAULT 1);
             CREATE TABLE selector_names (selector TEXT PRIMARY KEY, signature TEXT)",
        );
        assert_has(
            &refusal(&path),
            &[
                "counters: column n: default 1, expected 0",
                "seed_counters",
                "selector_names: column signature: nullable, expected not null",
                "selector_names: column fetched_at: missing",
                "cache of directory lookups",
            ],
        );
    }

    #[test]
    fn an_unmapped_table_says_it_has_no_known_recovery() {
        assert!(recovery("some_new_table").contains("No known recovery"));
    }

    /// The map names watermark keys by value; an upstream rename must fail here
    /// rather than leave the advice deleting a key nothing reads.
    #[test]
    fn the_recovery_map_names_live_watermark_keys() {
        assert!(recovery("genesis_balances").contains(db::GENESIS_CURSOR));
        assert!(recovery("anchoring_events").contains("anchoring_backfilled_to"));
        assert!(include_str!("../indexer.rs")
            .contains("const BACKFILL_KEY: &str = \"anchoring_backfilled_to\";"));
    }

    #[test]
    fn a_table_missing_from_the_database_is_reported() {
        let conn = Connection::open_in_memory().unwrap();
        let msg = format!("{:#}", verify(&conn).unwrap_err());
        assert_has(&msg, &["blocks: missing", "kv: missing"]);
    }

    /// The production path on each committed baseline: a copy goes through
    /// `db::open`, so `init_db` first applies what it can (new tables, index
    /// changes) and the check then sees what it cannot.
    #[test]
    fn every_baseline_fixture_opens() {
        let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/baseline");
        let (mut checked, mut refused) = (0, Vec::new());
        for entry in std::fs::read_dir(&fixtures).unwrap() {
            let src = entry.unwrap().path();
            if src.extension().is_none_or(|e| e != "db") {
                continue;
            }
            let dir = tempfile::tempdir().unwrap();
            let copy = dir.path().join(src.file_name().unwrap());
            std::fs::copy(&src, &copy).unwrap();
            if let Err(e) = db::open(copy.to_str().unwrap()) {
                refused.push(format!("{}: {e:#}", src.display()));
            }
            checked += 1;
        }
        assert_eq!(checked, 2, "expected both baseline fixtures");
        assert!(
            refused.is_empty(),
            "every deployed database would refuse to start too:\n{}",
            refused.join("\n\n")
        );
    }
}
