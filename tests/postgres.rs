//! The Postgres migrations (`migrations/postgres/`) against a real server.
//!
//! - **Idempotence:** applying the baseline, 0001, twice changes nothing.
//! - **Parity:** after every version N, Postgres (0001..N) has the shape of
//!   SQLite (`init_db`, then 0002..N), except for the differences `ALLOWED`
//!   names for N, and spells each expression or partial index as `TRANSLATED`
//!   pins it for N.
//! - **Round-trip and upgrade:** every baseline fixture's rows survive a copy
//!   into 0001 and every later version, unchanged.
//!
//! Every test that needs a server is ignored unless run with
//! `--include-ignored`, and then fails unless `PG_TEST_URL` names a server; a
//! run that ignores them all is not a pass. Each test works in a schema of its
//! own, dropped only when it passes.
//!
//! The SQL built at run time is wrapped in `AssertSqlSafe`: it interpolates
//! only this file's constants and names read from the catalog or a fixture.
//!
//! ```text
//! docker compose up -d --wait
//! PG_TEST_URL=postgres://explorer:explorer@localhost:5432/explorer \
//!     cargo test --test postgres -- --include-ignored
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use rusqlite::types::Value as Sql;
use rusqlite::Connection;
use sqlx::postgres::{PgArguments, PgConnection, Postgres};
use sqlx::query::Query;
use sqlx::{
    AssertSqlSafe, Connection as _, Either, Executor as _, Row as _, SqlSafeStr as _,
    Statement as _, TypeInfo as _,
};

#[path = "common/backend.rs"]
mod backend;
#[allow(dead_code)]
mod common;
use common::baseline::{
    columns, diff_rows, fixture_paths, open_fixture, pg_rows, quoted, row_key, rows, tables,
    to_json, Spec, SPECS,
};

use nvnmchain_explorer::db::migrations::{binary_version, Migration, MIGRATIONS};
use nvnmchain_explorer::db::{self, Role};

/// The baseline, version 1.
const SCHEMA: &str = include_str!("../migrations/postgres/0001_baseline.sql");

/// The differences between SQLite and Postgres that are intended:
/// (table, column, field, SQLite, Postgres), why, and `since`, the first
/// version it holds for. Each suppresses that one difference; one that does
/// not occur at a version it holds for fails the parity test. Entries have no
/// end version yet: the first migration that removes a difference here, or
/// changes or drops an index in `TRANSLATED`, adds one, so older versions keep
/// their own entries.
const ALLOWED: &[(&str, &str, &str, &str, &str, &str, i64)] = &[
    (
        "token_balances",
        "token_addr",
        "type",
        "bytes",
        "text",
        "declared BLOB on SQLite, but every row holds 0x text",
        1,
    ),
    (
        "token_balances",
        "holder_addr",
        "type",
        "bytes",
        "text",
        "declared BLOB on SQLite, but every row holds 0x text",
        1,
    ),
];

/// The expression and partial indexes, which the engines spell differently:
/// (index, its `sqlite_master.sql` on SQLite, its `pg_get_indexdef` on
/// Postgres, unqualified, and `since`, the first version it holds for).
/// Compared ignoring case and whitespace outside quotes, each side must match
/// its pin, so a change to either fails until it is ported to the other. A
/// comment or a redundant `ASC` in the definition counts as a change too.
const TRANSLATED: &[Pin] = &[(
    "idx_tb_holding",
    "CREATE INDEX idx_tb_holding
     ON token_balances(token_addr, LENGTH(balance) DESC, balance DESC)
     WHERE balance NOT LIKE '-%'",
    "CREATE INDEX idx_tb_holding ON token_balances USING btree
     (token_addr, length(balance) DESC, balance DESC, holder_addr)
     WHERE (balance !~~ '-%'::text)",
    1,
)];

type Pin = (&'static str, &'static str, &'static str, i64);

/// Row keys for the tables `SPECS` leaves out, which the round-trip copies too.
const UNINDEXED: &[Spec] = &[
    Spec {
        table: "kv",
        key: &["key"],
        skip: &[],
    },
    Spec {
        table: "selector_names",
        key: &["selector"],
        skip: &[],
    },
];

/// The schema a test's statements resolve in, as a `pg_namespace` oid.
const HERE: &str = "(SELECT oid FROM pg_namespace WHERE nspname = current_schema())";

// ---------------------------------------------------------------------------
// A scratch schema per test
// ---------------------------------------------------------------------------

fn pg_error(e: &sqlx::Error) -> String {
    match e.as_database_error() {
        Some(db) => format!("{} ({})", db.message(), db.code().unwrap_or_default()),
        None => e.to_string(),
    }
}

/// A fresh schema of the test's own, its URL, and a connection to it. Keep
/// the guard alive: it drops the schema once the test passes.
async fn scratch() -> (backend::Scratch, String, PgConnection) {
    let (guard, url) = backend::scratch_schema().await;
    let conn = PgConnection::connect(&url)
        .await
        .unwrap_or_else(|e| panic!("connect to the scratch schema: {}", pg_error(&e)));
    (guard, url, conn)
}

async fn apply_schema(conn: &mut PgConnection) {
    sqlx::raw_sql(SCHEMA)
        .execute(conn)
        .await
        .unwrap_or_else(|e| panic!("apply 0001_baseline.sql: {}", pg_error(&e)));
}

/// Apply the Postgres files of versions `from..=to`, in order.
async fn apply_versions(conn: &mut PgConnection, from: i64, to: i64) {
    for m in MIGRATIONS
        .iter()
        .filter(|m| (from..=to).contains(&m.version))
    {
        apply(conn, m).await;
    }
}

/// Apply one version's Postgres file as the runner does: in a transaction.
async fn apply(conn: &mut PgConnection, m: &Migration) {
    let failed =
        |e: sqlx::Error| -> ! { panic!("apply {:04}_{}.sql: {}", m.version, m.name, pg_error(&e)) };
    let mut tx = conn.begin().await.unwrap_or_else(|e| failed(e));
    sqlx::raw_sql(AssertSqlSafe(m.postgres))
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|e| failed(e));
    tx.commit().await.unwrap_or_else(|e| failed(e));
}

// ---------------------------------------------------------------------------
// Idempotence
// ---------------------------------------------------------------------------

/// Everything the schema holds, one line per column, constraint, relation and
/// index definition.
async fn catalog(conn: &mut PgConnection) -> Vec<String> {
    let sql = format!(
        "SELECT 'column ' || table_name || '.' || column_name || ' ' || data_type
                || ' ' || is_nullable || ' ' || coalesce(column_default, '-')
                || ' ' || is_identity || ' ' || coalesce(collation_name, '-')
           FROM information_schema.columns WHERE table_schema = current_schema()
         UNION ALL
         SELECT 'constraint ' || conname || ' ' || pg_get_constraintdef(oid)
           FROM pg_constraint WHERE connamespace = {HERE}
         UNION ALL
         SELECT 'relation ' || relname || ' ' || relkind::text
           FROM pg_class WHERE relnamespace = {HERE}
         UNION ALL
         SELECT 'index ' || indexdef FROM pg_indexes WHERE schemaname = current_schema()
         ORDER BY 1"
    );
    sqlx::query_scalar(AssertSqlSafe(sql))
        .fetch_all(conn)
        .await
        .unwrap_or_else(|e| panic!("read the catalog: {}", pg_error(&e)))
}

/// The explorer's own read of D: 0 on a database no migration has touched,
/// then the newest version once `schema_migrations` exists. It opens as a web
/// replica, which reads D and never migrates.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn the_schema_version_is_0_before_any_migration() {
    let (_scratch, url, mut conn) = scratch().await;
    let db = backend::open(&backend::pg_config(&url, Role::Web)).await;
    let version = db::schema_version(&db).await;
    assert_eq!(version.map_err(|e| format!("{e:#}")), Ok(0));

    apply_schema(&mut conn).await;
    sqlx::raw_sql(
        "CREATE TABLE schema_migrations (version BIGINT PRIMARY KEY); \
         INSERT INTO schema_migrations VALUES (1)",
    )
    .execute(&mut conn)
    .await
    .unwrap_or_else(|e| panic!("stamp version 1: {}", pg_error(&e)));
    let version = db::schema_version(&db).await;
    assert_eq!(version.map_err(|e| format!("{e:#}")), Ok(1));
}

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn applying_the_schema_twice_changes_nothing() {
    let (_scratch, _, mut conn) = scratch().await;
    apply_schema(&mut conn).await;
    let once = catalog(&mut conn).await;
    assert!(!once.is_empty(), "0001_baseline.sql created nothing");
    apply_schema(&mut conn).await;
    assert_eq!(catalog(&mut conn).await, once);
}

// ---------------------------------------------------------------------------
// Parity
// ---------------------------------------------------------------------------

/// A column default as a value, whichever engine spelled it.
#[derive(Clone, Debug, PartialEq)]
enum Lit {
    Int(i64),
    Text(String),
    Bytes(String),
    Other(String),
}

impl fmt::Display for Lit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Lit::Int(n) => write!(f, "{n}"),
            Lit::Text(s) => write!(f, "'{s}'"),
            Lit::Bytes(hex) => write!(f, "x'{hex}'"),
            Lit::Other(raw) => write!(f, "{raw}"),
        }
    }
}

fn unquote(s: &str) -> Option<String> {
    let inner = s.strip_prefix('\'')?.strip_suffix('\'')?;
    Some(inner.replace("''", "'"))
}

fn sqlite_lit(raw: &str) -> Lit {
    if let Some(hex) = raw
        .strip_prefix("X'")
        .or_else(|| raw.strip_prefix("x'"))
        .and_then(|s| s.strip_suffix('\''))
    {
        Lit::Bytes(hex.to_lowercase())
    } else if let Some(text) = unquote(raw) {
        Lit::Text(text)
    } else if let Ok(n) = raw.parse() {
        Lit::Int(n)
    } else {
        Lit::Other(raw.to_string())
    }
}

/// `'\x'::bytea`, `'0'::text` or `0`, with the casts stripped.
fn pg_lit(raw: &str) -> Lit {
    let mut s = raw.trim();
    let mut cast = None;
    while let Some(i) = s.rfind("::") {
        if s[i..].contains('\'') {
            break;
        }
        cast = Some(s[i + 2..].trim());
        s = s[..i].trim();
    }
    match (unquote(s), cast) {
        (Some(text), Some("bytea")) => match text.strip_prefix("\\x") {
            Some(hex) => Lit::Bytes(hex.to_lowercase()),
            None => Lit::Other(raw.to_string()),
        },
        (Some(text), _) => Lit::Text(text),
        (None, _) => s
            .parse()
            .map(Lit::Int)
            .unwrap_or_else(|_| Lit::Other(raw.to_string())),
    }
}

#[derive(Debug, PartialEq)]
struct Col {
    /// `int`, `text` or `bytes`; anything else by its own name.
    class: String,
    not_null: bool,
    default: Option<Lit>,
    /// Filled in by the database when left out: `AUTOINCREMENT` or an identity.
    auto: bool,
}

#[derive(Debug, PartialEq)]
struct Idx {
    table: String,
    unique: bool,
    partial: bool,
    /// Key columns with direction, or `None` for an expression or partial
    /// index, whose text differs between engines by design: `definition`
    /// holds it instead.
    keys: Option<Vec<String>>,
    /// The whole definition of an expression or partial index, as the engine
    /// reports it, for `TRANSLATED`.
    definition: Option<String>,
}

#[derive(Default)]
struct Shape {
    columns: BTreeMap<String, BTreeMap<String, Col>>,
    /// Per table: `primary key (a, b)` and `unique (a, b)`.
    keys: BTreeMap<String, BTreeSet<String>>,
    /// Indexes written as `CREATE INDEX`, by name.
    indexes: BTreeMap<String, Idx>,
}

fn key(kind: &str, cols: &[String]) -> String {
    format!("{kind} ({})", cols.join(", "))
}

fn sqlite_shape(conn: &Connection) -> Shape {
    let mut shape = Shape::default();
    for table in tables(conn)
        .into_iter()
        .filter(|t| !t.starts_with("sqlite_"))
    {
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [&table],
                |r| r.get(0),
            )
            .unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT name, type, \"notnull\", dflt_value, pk
                 FROM pragma_table_info(?1) ORDER BY cid",
            )
            .unwrap();
        let cols: Vec<(String, String, bool, Option<String>, i64)> = stmt
            .query_map([&table], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let mut primary: Vec<(i64, String)> = cols
            .iter()
            .filter(|c| c.4 > 0)
            .map(|c| (c.4, c.0.clone()))
            .collect();
        primary.sort();
        let sole_key = primary.len() == 1;
        let autoincrement = sql.to_uppercase().contains("AUTOINCREMENT");

        let table_cols = shape.columns.entry(table.clone()).or_default();
        for (name, decl, not_null, default, pk) in cols {
            let decl = decl.to_uppercase();
            let class = match decl.as_str() {
                "INTEGER" => "int".to_string(),
                "TEXT" => "text".to_string(),
                "BLOB" => "bytes".to_string(),
                other => other.to_lowercase(),
            };
            table_cols.insert(
                name,
                Col {
                    auto: autoincrement && sole_key && pk == 1 && class == "int",
                    class,
                    not_null: not_null || pk > 0,
                    default: default.as_deref().map(sqlite_lit),
                },
            );
        }

        let keys = shape.keys.entry(table.clone()).or_default();
        if !primary.is_empty() {
            let cols: Vec<String> = primary.into_iter().map(|(_, c)| c).collect();
            keys.insert(key("primary key", &cols));
        }
        let mut stmt = conn
            .prepare("SELECT name, \"unique\", origin, partial FROM pragma_index_list(?1)")
            .unwrap();
        let listed: Vec<(String, bool, String, bool)> = stmt
            .query_map([&table], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        for (index, unique, origin, partial) in listed {
            let mut stmt = conn
                .prepare(
                    "SELECT cid, name, \"desc\" FROM pragma_index_xinfo(?1)
                     WHERE key = 1 ORDER BY seqno",
                )
                .unwrap();
            let parts: Vec<(i64, Option<String>, bool)> = stmt
                .query_map([&index], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            let expression = parts.iter().any(|(cid, _, _)| *cid < 0);
            let names: Vec<String> = parts
                .into_iter()
                .map(|(_, name, desc)| {
                    let name = name.unwrap_or_else(|| "<expr>".into());
                    if desc {
                        format!("{name} DESC")
                    } else {
                        name
                    }
                })
                .collect();
            match origin.as_str() {
                "u" => {
                    keys.insert(key("unique", &names));
                }
                "c" => {
                    let definition = (expression || partial).then(|| {
                        conn.query_row(
                            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
                            [&index],
                            |r| r.get(0),
                        )
                        .unwrap()
                    });
                    shape.indexes.insert(
                        index,
                        Idx {
                            table: table.clone(),
                            unique,
                            partial,
                            keys: (!expression && !partial).then_some(names),
                            definition,
                        },
                    );
                }
                // A non-integer primary key's index: the key already says it.
                _ => {}
            }
        }
    }
    shape
}

async fn pg_shape(conn: &mut PgConnection) -> Shape {
    let mut shape = Shape::default();
    let cols = sqlx::query(
        "SELECT table_name::text, column_name::text, data_type::text,
                is_nullable = 'NO', column_default::text, is_identity = 'YES'
         FROM information_schema.columns WHERE table_schema = current_schema()
         ORDER BY table_name, ordinal_position",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap_or_else(|e| panic!("read columns: {}", pg_error(&e)));
    for row in cols {
        let class = match row.get::<String, _>(2).as_str() {
            "bigint" => "int".to_string(),
            "text" => "text".to_string(),
            "bytea" => "bytes".to_string(),
            other => other.to_string(),
        };
        shape.columns.entry(row.get(0)).or_default().insert(
            row.get(1),
            Col {
                class,
                not_null: row.get(3),
                default: row.get::<Option<String>, _>(4).as_deref().map(pg_lit),
                auto: row.get(5),
            },
        );
    }

    let constraints = sqlx::query(AssertSqlSafe(format!(
        "SELECT t.relname::text, c.contype = 'p',
                ARRAY(SELECT a.attname::text
                      FROM unnest(c.conkey) WITH ORDINALITY k(attnum, ord)
                      JOIN pg_attribute a
                        ON a.attrelid = c.conrelid AND a.attnum = k.attnum
                      ORDER BY k.ord)
         FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
         WHERE t.relnamespace = {HERE} AND c.contype IN ('p', 'u')"
    )))
    .fetch_all(&mut *conn)
    .await
    .unwrap_or_else(|e| panic!("read constraints: {}", pg_error(&e)));
    for row in constraints {
        let kind = if row.get(1) { "primary key" } else { "unique" };
        let cols: Vec<String> = row.get(2);
        shape
            .keys
            .entry(row.get(0))
            .or_default()
            .insert(key(kind, &cols));
    }

    let indexes = sqlx::query(AssertSqlSafe(format!(
        "SELECT ic.relname::text, tc.relname::text, i.indisunique,
                i.indpred IS NOT NULL, i.indexprs IS NOT NULL,
                ARRAY(SELECT coalesce(a.attname::text, '<expr>')
                             || CASE WHEN i.indoption[k] & 1 = 1
                                     THEN ' DESC' ELSE '' END
                      FROM generate_series(0, i.indnkeyatts - 1) k
                      LEFT JOIN pg_attribute a
                        ON a.attrelid = i.indrelid AND a.attnum = i.indkey[k]
                      ORDER BY k),
                -- Always qualified by the scratch schema, whose name is the
                -- test's own; drop it.
                replace(pg_get_indexdef(i.indexrelid),
                        quote_ident(current_schema()) || '.', '')
         FROM pg_index i
         JOIN pg_class ic ON ic.oid = i.indexrelid
         JOIN pg_class tc ON tc.oid = i.indrelid
         WHERE tc.relnamespace = {HERE}
           AND NOT EXISTS (SELECT 1 FROM pg_constraint c
                           WHERE c.conindid = i.indexrelid)"
    )))
    .fetch_all(&mut *conn)
    .await
    .unwrap_or_else(|e| panic!("read indexes: {}", pg_error(&e)));
    for row in indexes {
        let (partial, expression): (bool, bool) = (row.get(3), row.get(4));
        shape.indexes.insert(
            row.get(0),
            Idx {
                table: row.get(1),
                unique: row.get(2),
                partial,
                keys: (!expression && !partial).then(|| row.get(5)),
                definition: (expression || partial).then(|| row.get(6)),
            },
        );
    }
    shape
}

/// The `text` columns of the schema not collated `"C"`, which SQLite's
/// `BINARY` comparison matches whatever the server's locale.
async fn uncollated(conn: &mut PgConnection) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT table_name::text || '.' || column_name::text || ': collation '
                || coalesce(collation_name::text, 'default') || ', must be \"C\"'
         FROM information_schema.columns
         WHERE table_schema = current_schema() AND data_type = 'text'
           AND coalesce(collation_name::text, '') <> 'C'
         ORDER BY 1",
    )
    .fetch_all(conn)
    .await
    .unwrap_or_else(|e| panic!("read collations: {}", pg_error(&e)))
}

/// One difference between the two schemas.
#[derive(Debug, PartialEq)]
struct Diff {
    table: String,
    /// A column, key or index; empty for the table itself.
    item: String,
    field: &'static str,
    sqlite: String,
    pg: String,
}

impl Diff {
    fn tuple(&self) -> (&str, &str, &str, &str, &str) {
        (&self.table, &self.item, self.field, &self.sqlite, &self.pg)
    }
}

impl fmt::Display for Diff {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let what = if self.item.is_empty() {
            self.table.clone()
        } else {
            format!("{}.{}", self.table, self.item)
        };
        match (self.field, self.pg.as_str()) {
            ("presence", "missing") => write!(f, "{what}: missing on Postgres"),
            ("presence", _) => write!(f, "{what}: only on Postgres"),
            _ => write!(
                f,
                "{what}: {}: SQLite {}, Postgres {}",
                self.field, self.sqlite, self.pg
            ),
        }
    }
}

/// Push a presence difference for every name only one side has; return the
/// names both have.
fn present<'a, V>(
    out: &mut Vec<Diff>,
    table: &str,
    sqlite: &'a BTreeMap<String, V>,
    pg: &'a BTreeMap<String, V>,
    item: impl Fn(&str) -> (String, String),
) -> Vec<&'a str> {
    let mut both = Vec::new();
    for name in sqlite.keys().chain(pg.keys()).collect::<BTreeSet<_>>() {
        let (table_name, item_name) = item(name);
        let table_name = if table.is_empty() {
            table_name
        } else {
            table.to_string()
        };
        match (sqlite.contains_key(name), pg.contains_key(name)) {
            (true, true) => both.push(name.as_str()),
            (in_sqlite, _) => out.push(Diff {
                table: table_name,
                item: item_name,
                field: "presence",
                sqlite: if in_sqlite { "present" } else { "missing" }.into(),
                pg: if in_sqlite { "missing" } else { "present" }.into(),
            }),
        }
    }
    both
}

fn compare(sqlite: &Shape, pg: &Shape) -> Vec<Diff> {
    let mut out = Vec::new();
    let tables = present(&mut out, "", &sqlite.columns, &pg.columns, |t| {
        (t.to_string(), String::new())
    });
    for table in tables {
        let (s_cols, p_cols) = (&sqlite.columns[table], &pg.columns[table]);
        for col in present(&mut out, table, s_cols, p_cols, |c| {
            (String::new(), c.into())
        }) {
            let (s, p) = (&s_cols[col], &p_cols[col]);
            let show = |d: &Option<Lit>| d.as_ref().map_or("none".into(), Lit::to_string);
            for (field, a, b) in [
                ("type", s.class.clone(), p.class.clone()),
                ("not null", s.not_null.to_string(), p.not_null.to_string()),
                ("default", show(&s.default), show(&p.default)),
                ("auto", s.auto.to_string(), p.auto.to_string()),
            ] {
                if a != b {
                    out.push(Diff {
                        table: table.into(),
                        item: col.into(),
                        field,
                        sqlite: a,
                        pg: b,
                    });
                }
            }
        }
        let empty = BTreeSet::new();
        let as_map = |keys: Option<&BTreeSet<String>>| -> BTreeMap<String, ()> {
            keys.unwrap_or(&empty)
                .iter()
                .map(|k| (k.clone(), ()))
                .collect()
        };
        let (s_keys, p_keys) = (as_map(sqlite.keys.get(table)), as_map(pg.keys.get(table)));
        present(&mut out, table, &s_keys, &p_keys, |k| {
            (String::new(), k.into())
        });
    }
    for name in present(&mut out, "", &sqlite.indexes, &pg.indexes, |i| {
        let table = sqlite.indexes.get(i).or(pg.indexes.get(i)).unwrap();
        (table.table.clone(), format!("index {i}"))
    }) {
        let (s, p) = (&sqlite.indexes[name], &pg.indexes[name]);
        let show = |keys: &Option<Vec<String>>| {
            keys.as_ref().map_or("(expression or partial)".into(), |k| {
                format!("({})", k.join(", "))
            })
        };
        for (field, a, b) in [
            ("table", s.table.clone(), p.table.clone()),
            ("unique", s.unique.to_string(), p.unique.to_string()),
            ("partial", s.partial.to_string(), p.partial.to_string()),
            ("keys", show(&s.keys), show(&p.keys)),
        ] {
            if a != b {
                out.push(Diff {
                    table: s.table.clone(),
                    item: format!("index {name}"),
                    field,
                    sqlite: a,
                    pg: b,
                });
            }
        }
    }
    out
}

/// Lowercase, with whitespace dropped outside string literals, as
/// `src/db/schema_check.rs` compares definitions.
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

/// At version `n`: every expression or partial index whose definition on
/// either side is not the one its pin for `n` holds, or that has no pin, and
/// every pin for `n` that names none.
fn untranslated(sqlite: &Shape, pg: &Shape, pins: &[Pin], n: i64) -> Vec<String> {
    let definition = |shape: &Shape, name: &str| -> Option<String> {
        shape.indexes.get(name)?.definition.clone()
    };
    let pins: Vec<&Pin> = pins.iter().filter(|p| p.3 <= n).collect();
    let mut out = Vec::new();
    let names: BTreeSet<&String> = sqlite
        .indexes
        .iter()
        .chain(&pg.indexes)
        .filter(|(_, i)| i.definition.is_some())
        .map(|(name, _)| name)
        .collect();
    for name in names {
        let table = &sqlite
            .indexes
            .get(name)
            .or(pg.indexes.get(name))
            .unwrap()
            .table;
        let what = format!("{table}.index {name}");
        let Some(&&(_, s_pin, p_pin, _)) = pins.iter().find(|p| p.0 == name) else {
            let show = |shape| definition(shape, name).unwrap_or_else(|| "none".into());
            out.push(format!(
                "{what}: an expression or partial index; pin it in TRANSLATED: SQLite {}, \
                 Postgres {}",
                show(sqlite),
                show(pg)
            ));
            continue;
        };
        if let Some(s) = definition(sqlite, name).filter(|s| normalize(s) != normalize(s_pin)) {
            out.push(format!(
                "{what}: the SQLite definition differs from TRANSLATED; a migration pair \
                 changes both sides (docs/database.md) and pins the new definition from \
                 its version: {s}"
            ));
        }
        if let Some(p) = definition(pg, name).filter(|p| normalize(p) != normalize(p_pin)) {
            out.push(format!(
                "{what}: the Postgres definition differs from TRANSLATED: {p}"
            ));
        }
    }
    for p in pins {
        if definition(sqlite, p.0).is_none() && definition(pg, p.0).is_none() {
            out.push(format!(
                "TRANSLATED entry {:?} names no expression or partial index; \
                 start it (`since`) at the version that added the index",
                p.0
            ));
        }
    }
    out
}

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn every_version_has_the_same_shape_on_both_backends() {
    for n in 1..=binary_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("parity.db");
        let conn = db::init_db(path.to_str().unwrap()).unwrap();
        for m in MIGRATIONS.iter().filter(|m| (2..=n).contains(&m.version)) {
            conn.execute_batch(m.sqlite.expect("a SQLite twin"))
                .unwrap_or_else(|e| panic!("apply sqlite {:04}_{}.sql: {e}", m.version, m.name));
        }
        let sqlite = sqlite_shape(&conn);

        let (_scratch, _, mut pg) = scratch().await;
        apply_versions(&mut pg, 1, n).await;
        let postgres = pg_shape(&mut pg).await;
        let diffs = compare(&sqlite, &postgres);

        let allowed: Vec<_> = ALLOWED.iter().filter(|a| a.6 <= n).collect();
        let mut problems: Vec<String> = diffs
            .iter()
            .filter(|d| {
                !allowed
                    .iter()
                    .any(|a| (a.0, a.1, a.2, a.3, a.4) == d.tuple())
            })
            .map(ToString::to_string)
            .collect();
        for a in allowed {
            if !diffs.iter().any(|d| d.tuple() == (a.0, a.1, a.2, a.3, a.4)) {
                problems.push(format!(
                    "ALLOWED entry {:?} matches no difference; start it (`since`) at the \
                     version that made the difference",
                    (a.0, a.1, a.2, a.3, a.4)
                ));
            }
        }
        problems.extend(untranslated(&sqlite, &postgres, TRANSLATED, n));
        problems.extend(uncollated(&mut pg).await);
        assert!(
            problems.is_empty(),
            "version {n}: SQLite and Postgres differ; fix the migration pair using the type \
             rules in docs/superpowers/specs/2026-10-02-schema-two-dialects-design.md:\n{}",
            problems.join("\n")
        );
    }
}

// ---------------------------------------------------------------------------
// Baseline round-trip
// ---------------------------------------------------------------------------

fn spec(table: &str) -> &'static Spec {
    SPECS
        .iter()
        .chain(UNINDEXED)
        .find(|s| s.table == table)
        .unwrap_or_else(|| panic!("{table}: no row key; add it to UNINDEXED in tests/postgres.rs"))
}

type PgQuery<'q> = Query<'q, Postgres, PgArguments>;

/// `query` with a SQLite value bound as a parameter of Postgres type `ty`
/// (`INT8`, `TEXT` or `BYTEA`), typed by the column even when it is `NULL`;
/// `None` if the stored value does not fit. sqlx does not check a bound value
/// against the statement's parameter types, so this match is the only check.
fn bind<'q>(query: PgQuery<'q>, ty: &str, value: &Sql) -> Option<PgQuery<'q>> {
    Some(match (ty, value) {
        ("INT8", Sql::Integer(i)) => query.bind(*i),
        ("INT8", Sql::Null) => query.bind(None::<i64>),
        ("TEXT", Sql::Text(s)) => query.bind(s.clone()),
        ("TEXT", Sql::Null) => query.bind(None::<String>),
        ("BYTEA", Sql::Blob(b)) => query.bind(b.clone()),
        ("BYTEA", Sql::Null) => query.bind(None::<Vec<u8>>),
        _ => return None,
    })
}

/// Copy every row of `table` in one transaction; returns how many.
async fn copy_table(
    conn: &mut PgConnection,
    sqlite: &Connection,
    table: &str,
    cols: &[String],
) -> usize {
    let list = quoted(cols);
    let source: Vec<Vec<Sql>> = {
        let mut stmt = sqlite
            .prepare(&format!("SELECT {list} FROM {table}"))
            .unwrap();
        stmt.query_map([], |r| (0..cols.len()).map(|i| r.get(i)).collect())
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    let spec = spec(table);
    let placeholders = (1..=cols.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut tx = conn
        .begin()
        .await
        .unwrap_or_else(|e| panic!("{table}: begin: {}", pg_error(&e)));
    let sql = format!("INSERT INTO {table} ({list}) VALUES ({placeholders})");
    let insert = (&mut *tx)
        .prepare(AssertSqlSafe(sql).into_sql_str())
        .await
        .unwrap_or_else(|e| panic!("{table}: prepare insert: {}", pg_error(&e)));
    let Some(Either::Left(types)) = insert.parameters() else {
        panic!("{table}: Postgres gave no parameter types for the insert")
    };
    for row in &source {
        let row_id = || {
            let fields = cols
                .iter()
                .zip(row)
                .map(|(c, v)| (c.clone(), to_json(c, v.clone())))
                .collect();
            row_key(spec, &fields)
        };
        let mut query = insert.query();
        for ((col, ty), value) in cols.iter().zip(types).zip(row) {
            let ty = ty.name();
            query = bind(query, ty, value).unwrap_or_else(|| {
                panic!(
                    "{table} {}: {col} holds {value:?}, which a {ty} column cannot",
                    row_id()
                )
            });
        }
        query
            .execute(&mut *tx)
            .await
            .unwrap_or_else(|e| panic!("{table} {}: insert: {}", row_id(), pg_error(&e)));
    }
    tx.commit()
        .await
        .unwrap_or_else(|e| panic!("{table}: commit: {}", pg_error(&e)));
    source.len()
}

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn every_baseline_round_trips_through_postgres() {
    let mut failed = Vec::new();
    // A failing fixture's schema, kept to inspect.
    let mut kept = Vec::new();
    for path in fixture_paths() {
        let (guard, _, mut pg) = scratch().await;
        apply_schema(&mut pg).await;
        let sqlite = open_fixture(&path);

        let mut copied = Vec::new();
        for table in tables(&sqlite)
            .into_iter()
            .filter(|t| t != "sqlite_sequence")
        {
            let cols = columns(&sqlite, &table);
            let n = copy_table(&mut pg, &sqlite, &table, &cols).await;
            copied.push((table, cols, n));
        }
        // The data upgrade: every later version applies over the copied rows,
        // and they read back unchanged.
        apply_versions(&mut pg, 2, binary_version()).await;

        let mut diffs = Vec::new();
        for (table, cols, n) in &copied {
            let (table, n) = (table.as_str(), *n);
            let spec = spec(table);
            let (base, back) = (
                rows(&sqlite, spec, cols),
                pg_rows(&mut pg, spec, cols).await,
            );
            assert_eq!(base.len(), n, "{table}: duplicate row keys in the fixture");
            diffs.extend(diff_rows(table, &base, &back, ("fixture", "Postgres copy")));
        }

        let total: usize = copied.iter().map(|c| c.2).sum();
        eprintln!("{}: {total} row(s) copied", path.display());
        if !diffs.is_empty() {
            for d in diffs.iter().take(25) {
                eprintln!("  {d}");
            }
            failed.push(format!("{}: {} difference(s)", path.display(), diffs.len()));
            kept.push(guard);
        }
    }
    assert!(failed.is_empty(), "round-trip differs: {failed:?}");
}
