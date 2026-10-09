//! What this process reports about its database, for `/readyz` and metrics.
//!
//! The database layer publishes it on a `watch` channel, so a probe reads the
//! latest value and never waits on, or takes, a database connection.

use serde::Serialize;

use super::migrations;
use super::Role;

#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub role: Role,
    pub schema: SchemaVersions,
    /// The writer's state, where this process has one on Postgres.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer: Option<WriterState>,
    /// Whether the indexer's lock-free preflight has passed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preflight: Option<Preflight>,
    /// The indexer's sync progress, for the cutover.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync: Option<Sync>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WriterState {
    Candidate,
    Leader,
    Reacquiring,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Preflight {
    Pending,
    Passed,
}

/// How far the indexer has come, read from what its loops last saw, never
/// from the database.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Sync {
    pub lowest_block: Option<i64>,
    pub tip_lag: Option<i64>,
    pub genesis: bool,
    pub anchoring: bool,
    pub complete: bool,
}

/// D, the database's version (unknown until it has been read), and B, this
/// binary's.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct SchemaVersions {
    pub db: Option<i64>,
    pub binary: i64,
}

impl Status {
    /// Before the database has been opened.
    pub fn starting(role: Role) -> Self {
        Status {
            role,
            schema: SchemaVersions {
                db: None,
                binary: migrations::binary_version(),
            },
            writer: None,
            preflight: (role == Role::Indexer).then_some(Preflight::Pending),
            sync: None,
        }
    }

    /// Whether this process should get traffic. Sync progress and database
    /// health never count: outages are the 503 middleware's to answer.
    pub fn ready(&self) -> bool {
        match self.role {
            // Whether a candidate, the leader or re-acquiring: a new pod must
            // turn Ready while it waits for the lock, or a rollout deadlocks.
            Role::Indexer => self.preflight == Some(Preflight::Passed),
            Role::All | Role::Web => self.schema.db.is_some_and(migrations::web_ready),
        }
    }
}
