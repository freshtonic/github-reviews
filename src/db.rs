//! Durable, transactional state for `github-reviews`.
//!
//! [`Database`] deliberately owns a single SQLite connection. Callers which need
//! to share it between worker threads should place it behind a mutex, or open a
//! connection per worker. Mutating operations which coordinate work use
//! `BEGIN IMMEDIATE`, so claiming an action and enforcing concurrency limits is
//! atomic across processes.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, TimeDelta, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{
    Notification, PullRequestState, RegisteredRepository, RepositoryIdentity, ReviewEnvelope,
};

const SCHEMA_VERSION: i64 = 5;
const MAX_ACTION_ATTEMPTS: u32 = 3;

/// The shared application database.
pub struct Database {
    connection: Connection,
    path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredRepositoryRecord {
    pub host: String,
    pub repository: RegisteredRepository,
    pub registered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PollState {
    pub host: String,
    pub viewer_id: i64,
    pub last_modified: Option<String>,
    pub high_water: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct StoredNotification {
    pub host: String,
    pub viewer_id: i64,
    pub notification: Notification,
    /// The unmodified API representation supplied to review commands.
    pub raw: Value,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiscoveryStatus {
    Pending,
    Running,
    Completed,
}

impl DiscoveryStatus {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            other => bail!("invalid notification discovery status in database: {other}"),
        }
    }
}

/// A notification awaiting enrichment into a tracked pull request. The raw
/// notification is joined from durable notification storage, so advancing the
/// HTTP validator never loses discovery work.
#[derive(Clone, Debug)]
pub struct NotificationDiscovery {
    pub host: String,
    pub viewer_id: i64,
    pub notification_id: String,
    pub repository_id: i64,
    pub notification_updated_at: DateTime<Utc>,
    pub notification: Notification,
    pub raw: Value,
    pub status: DiscoveryStatus,
    pub attempts: u32,
    pub due_at: DateTime<Utc>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct TrackedPullRequest {
    pub host: String,
    pub viewer_id: i64,
    pub repository_id: i64,
    pub repository_full_name: String,
    pub pull_request: PullRequestState,
    pub notification_id: String,
    pub notification_updated_at: DateTime<Utc>,
    pub notification: Value,
    pub active: bool,
    pub next_check_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Immutable identity and payload for a newly actionable review request.
#[derive(Clone, Debug)]
pub struct NewReviewAction {
    pub host: String,
    pub viewer_id: i64,
    pub repository_id: i64,
    pub repository_full_name: String,
    pub pull_request_id: i64,
    pub pull_request_number: u64,
    pub head_sha: String,
    /// Stable GitHub request/review identity. Combined with the head SHA, this
    /// prevents a successful action from being emitted twice.
    pub trigger_key: String,
    pub reason: String,
    pub envelope: ReviewEnvelope,
    pub due_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionStatus {
    Pending,
    Running,
    Succeeded,
    Parked,
    Cancelled,
}

impl ActionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Parked => "parked",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "parked" => Ok(Self::Parked),
            "cancelled" => Ok(Self::Cancelled),
            other => bail!("invalid review action status in database: {other}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReviewAction {
    pub id: i64,
    pub host: String,
    pub viewer_id: i64,
    pub repository_id: i64,
    pub repository_full_name: String,
    pub pull_request_id: i64,
    pub pull_request_number: u64,
    pub head_sha: String,
    pub base_sha: String,
    pub trigger_key: String,
    pub reason: String,
    pub envelope: ReviewEnvelope,
    pub status: ActionStatus,
    pub attempts: u32,
    pub due_at: DateTime<Utc>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnqueueResult {
    pub action_id: i64,
    pub inserted: bool,
    pub superseded: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonLease {
    pub host: String,
    pub viewer_id: i64,
    pub viewer_login: String,
    pub owner: String,
    pub expires_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActionCounts {
    pub pending: u64,
    pub running: u64,
    pub succeeded: u64,
    pub parked: u64,
    pub cancelled: u64,
}

#[derive(Clone, Debug)]
pub struct StatusSnapshot {
    pub repositories: Vec<RegisteredRepositoryRecord>,
    pub leases: Vec<DaemonLease>,
    pub poll_states: Vec<PollState>,
    pub action_counts: ActionCounts,
    pub pending_and_failed_actions: Vec<ReviewAction>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PruneResult {
    pub notifications: usize,
    pub tracked_pull_requests: usize,
    pub actions: usize,
}

impl Database {
    /// Returns the fixed `~/.config/github-reviews/state.sqlite3` path.
    pub fn default_path() -> Result<PathBuf> {
        let home = dirs::home_dir().ok_or_else(|| anyhow!("no home directory is available"))?;
        Ok(home
            .join(".config")
            .join("github-reviews")
            .join("state.sqlite3"))
    }

    /// Opens the database, creates and migrates it, and restricts the direct
    /// state directory/database permissions to 0700/0600 on Unix.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| anyhow!("database path must have a parent directory"))?;
        fs::create_dir_all(parent)
            .with_context(|| format!("create state directory {}", parent.display()))?;
        set_directory_permissions(parent)?;

        let mut connection = Connection::open(&path)
            .with_context(|| format!("open state database {}", path.display()))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        set_file_permissions(&path)?;
        migrate(&mut connection)?;

        Ok(Self { connection, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Inserts or replaces the local clone registered for a GitHub repository.
    /// A replacement marks the repository for bootstrap again when its path or
    /// canonical identity changed.
    pub fn upsert_registration(
        &mut self,
        host: &str,
        repository: &RegisteredRepository,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction
            .query_row(
                "SELECT full_name, html_url, origin_url, local_path FROM registered_repositories
                 WHERE host = ?1 AND repository_id = ?2",
                params![host, repository.repository.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?
            .is_none_or(|(name, html, origin, local)| {
                name != repository.repository.full_name
                    || html != repository.repository.html_url
                    || origin != repository.origin_url
                    || Path::new(&local) != repository.local_path
            });
        let now = millis(now);
        transaction.execute(
            "INSERT INTO registered_repositories
                (host, repository_id, full_name, html_url, origin_url, local_path,
                 bootstrap_pending, registered_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
             ON CONFLICT(host, repository_id) DO UPDATE SET
                full_name = excluded.full_name,
                html_url = excluded.html_url,
                origin_url = excluded.origin_url,
                local_path = excluded.local_path,
                bootstrap_pending = CASE
                    WHEN ?9 THEN 1 ELSE registered_repositories.bootstrap_pending END,
                updated_at = excluded.updated_at",
            params![
                host,
                repository.repository.id,
                repository.repository.full_name,
                repository.repository.html_url,
                repository.origin_url,
                repository.local_path.to_string_lossy(),
                repository.bootstrap_pending || changed,
                now,
                changed,
            ],
        )?;
        if changed {
            transaction.execute(
                "UPDATE repository_bootstraps SET pending = 1, updated_at = ?3
                 WHERE host = ?1 AND repository_id = ?2",
                params![host, repository.repository.id, now],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn list_registrations(&self) -> Result<Vec<RegisteredRepositoryRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT host, repository_id, full_name, html_url, origin_url, local_path,
                    bootstrap_pending, registered_at, updated_at
             FROM registered_repositories ORDER BY full_name",
        )?;
        statement
            .query_map([], map_registration)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn get_registration(
        &self,
        host: &str,
        repository_id: i64,
    ) -> Result<Option<RegisteredRepositoryRecord>> {
        self.connection
            .query_row(
                "SELECT host, repository_id, full_name, html_url, origin_url, local_path,
                        bootstrap_pending, registered_at, updated_at
                 FROM registered_repositories WHERE host = ?1 AND repository_id = ?2",
                params![host, repository_id],
                map_registration,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn get_registration_by_name(
        &self,
        host: &str,
        full_name: &str,
    ) -> Result<Option<RegisteredRepositoryRecord>> {
        self.connection
            .query_row(
                "SELECT host, repository_id, full_name, html_url, origin_url, local_path,
                        bootstrap_pending, registered_at, updated_at
                 FROM registered_repositories WHERE host = ?1 AND full_name = ?2",
                params![host, full_name],
                map_registration,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn remove_registration(&mut self, host: &str, repository_id: i64) -> Result<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE tracked_pull_requests SET active = 0, updated_at = ?3
             WHERE host = ?1 AND repository_id = ?2",
            params![host, repository_id, millis(Utc::now())],
        )?;
        transaction.execute(
            "UPDATE review_actions SET status = 'cancelled', updated_at = ?3
             WHERE host = ?1 AND repository_id = ?2 AND status IN ('pending', 'parked')",
            params![host, repository_id, millis(Utc::now())],
        )?;
        transaction.execute(
            "DELETE FROM notification_discoveries
             WHERE host = ?1 AND repository_id = ?2 AND status <> 'completed'",
            params![host, repository_id],
        )?;
        let removed = transaction.execute(
            "DELETE FROM registered_repositories WHERE host = ?1 AND repository_id = ?2",
            params![host, repository_id],
        )?;
        transaction.commit()?;
        Ok(removed != 0)
    }

    /// Returns whether a repository needs its one-time bootstrap for this
    /// viewer. A missing viewer row is deliberately interpreted as pending.
    pub fn bootstrap_pending(
        &self,
        host: &str,
        viewer_id: i64,
        repository_id: i64,
    ) -> Result<Option<bool>> {
        self.connection
            .query_row(
                "SELECT COALESCE((
                     SELECT pending FROM repository_bootstraps bootstrap
                     WHERE bootstrap.host = repository.host
                       AND bootstrap.repository_id = repository.repository_id
                       AND bootstrap.viewer_id = ?2
                 ), 1)
                 FROM registered_repositories repository
                 WHERE repository.host = ?1 AND repository.repository_id = ?3",
                params![host, viewer_id, repository_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    /// Marks repository bootstrap complete for one authenticated viewer. Other
    /// viewers retain independent pending state.
    pub fn mark_bootstrap_complete(
        &mut self,
        host: &str,
        viewer_id: i64,
        repository_id: i64,
    ) -> Result<bool> {
        Ok(self.connection.execute(
            "INSERT INTO repository_bootstraps
                 (host, viewer_id, repository_id, pending, updated_at)
             SELECT host, ?2, repository_id, 0, ?4
             FROM registered_repositories WHERE host = ?1 AND repository_id = ?3
             ON CONFLICT(host, viewer_id, repository_id) DO UPDATE SET
                 pending = 0, updated_at = excluded.updated_at",
            params![host, viewer_id, repository_id, millis(Utc::now())],
        )? != 0)
    }

    pub fn get_metadata(&self, key: &str) -> Result<Option<String>> {
        self.connection
            .query_row("SELECT value FROM metadata WHERE key = ?1", [key], |row| {
                row.get(0)
            })
            .optional()
            .map_err(Into::into)
    }

    pub fn set_metadata(&mut self, key: &str, value: &str) -> Result<()> {
        self.connection.execute(
            "INSERT INTO metadata(key, value, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            params![key, value, millis(Utc::now())],
        )?;
        Ok(())
    }

    pub fn poll_state(&self, host: &str, viewer_id: i64) -> Result<Option<PollState>> {
        self.connection
            .query_row(
                "SELECT host, viewer_id, last_modified, high_water, updated_at
                 FROM poll_state WHERE host = ?1 AND viewer_id = ?2",
                params![host, viewer_id],
                map_poll_state,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Stores the conditional validator and high-water mark in one statement.
    /// Call [`Self::commit_poll`] when notification rows must be committed in
    /// the same transaction as the polling metadata.
    pub fn set_poll_state(
        &mut self,
        host: &str,
        viewer_id: i64,
        last_modified: Option<&str>,
        high_water: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        upsert_poll_state(
            &self.connection,
            host,
            viewer_id,
            last_modified,
            high_water,
            now,
        )
    }

    /// Atomically persists a fully fetched notification snapshot and advances
    /// the conditional-poll state. Callers must not call this with partial
    /// pagination results.
    pub fn commit_poll(
        &mut self,
        host: &str,
        viewer_id: i64,
        notifications: &[(Notification, Value)],
        last_modified: Option<&str>,
        high_water: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (notification, raw) in notifications {
            upsert_notification(&transaction, host, viewer_id, notification, raw, now)?;
        }
        upsert_poll_state(
            &transaction,
            host,
            viewer_id,
            last_modified,
            high_water,
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically stores a repository bootstrap page, enqueues its actionable
    /// notification-shaped discovery work, and marks bootstrap complete for
    /// this viewer. A crash cannot acknowledge bootstrap without preserving
    /// the work it discovered.
    pub fn commit_bootstrap(
        &mut self,
        host: &str,
        viewer_id: i64,
        repository_id: i64,
        notifications: &[(Notification, Value)],
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (notification, raw) in notifications {
            upsert_notification(&transaction, host, viewer_id, notification, raw, now)?;
        }
        let marked = transaction.execute(
            "INSERT INTO repository_bootstraps
                 (host, viewer_id, repository_id, pending, updated_at)
             SELECT host, ?2, repository_id, 0, ?4
             FROM registered_repositories WHERE host = ?1 AND repository_id = ?3
             ON CONFLICT(host, viewer_id, repository_id) DO UPDATE SET
                 pending = 0, updated_at = excluded.updated_at",
            params![host, viewer_id, repository_id, millis(now)],
        )? != 0;
        transaction.commit()?;
        Ok(marked)
    }

    /// Lists due notification discoveries without claiming them.
    pub fn list_pending_discoveries(
        &self,
        host: &str,
        viewer_id: i64,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<NotificationDiscovery>> {
        let mut statement = self.connection.prepare(&format!(
            "{} WHERE discovery.host = ?1 AND discovery.viewer_id = ?2
                 AND discovery.status = 'pending' AND discovery.due_at <= ?3
             ORDER BY discovery.due_at, discovery.created_at, discovery.notification_id
             LIMIT ?4",
            discovery_select()
        ))?;
        statement
            .query_map(
                params![
                    host,
                    viewer_id,
                    millis(now),
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
                map_discovery,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn due_discovery_count(
        &self,
        host: &str,
        viewer_id: i64,
        now: DateTime<Utc>,
    ) -> Result<usize> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM notification_discoveries
             WHERE host = ?1 AND viewer_id = ?2
               AND status = 'pending' AND due_at <= ?3",
            params![host, viewer_id, millis(now)],
            |row| row.get(0),
        )?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    /// Claims one due notification for enrichment. Poll validators may already
    /// have advanced; the joined raw notification remains durable until this
    /// item is completed.
    pub fn claim_pending_discovery(
        &mut self,
        host: &str,
        viewer_id: i64,
        now: DateTime<Utc>,
    ) -> Result<Option<NotificationDiscovery>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let notification_id: Option<String> = transaction
            .query_row(
                "SELECT notification_id FROM notification_discoveries
                 WHERE host = ?1 AND viewer_id = ?2
                   AND status = 'pending' AND due_at <= ?3
                 ORDER BY due_at, created_at, notification_id LIMIT 1",
                params![host, viewer_id, millis(now)],
                |row| row.get(0),
            )
            .optional()?;
        let Some(notification_id) = notification_id else {
            transaction.commit()?;
            return Ok(None);
        };
        transaction.execute(
            "UPDATE notification_discoveries
             SET status = 'running', attempts = attempts + 1,
                 updated_at = ?4, last_error = NULL
             WHERE host = ?1 AND viewer_id = ?2 AND notification_id = ?3",
            params![host, viewer_id, notification_id, millis(now)],
        )?;
        let discovery = transaction.query_row(
            &format!(
                "{} WHERE discovery.host = ?1 AND discovery.viewer_id = ?2
                       AND discovery.notification_id = ?3",
                discovery_select()
            ),
            params![host, viewer_id, notification_id],
            map_discovery,
        )?;
        transaction.commit()?;
        Ok(Some(discovery))
    }

    pub fn complete_discovery(
        &mut self,
        host: &str,
        viewer_id: i64,
        notification_id: &str,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self.connection.execute(
            "UPDATE notification_discoveries
             SET status = 'completed', updated_at = ?4, completed_at = ?4,
                 last_error = NULL
             WHERE host = ?1 AND viewer_id = ?2 AND notification_id = ?3
               AND status = 'running'",
            params![host, viewer_id, notification_id, millis(now)],
        )? != 0)
    }

    pub fn defer_discovery(
        &mut self,
        host: &str,
        viewer_id: i64,
        notification_id: &str,
        error: &str,
        retry_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self.connection.execute(
            "UPDATE notification_discoveries
             SET status = 'pending', due_at = ?5, updated_at = ?6,
                 completed_at = NULL, last_error = ?4
             WHERE host = ?1 AND viewer_id = ?2 AND notification_id = ?3
               AND status = 'running'",
            params![
                host,
                viewer_id,
                notification_id,
                error,
                millis(retry_at),
                millis(now),
            ],
        )? != 0)
    }

    pub fn recover_running_discoveries(
        &mut self,
        host: &str,
        viewer_id: i64,
        retry_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<usize> {
        Ok(self.connection.execute(
            "UPDATE notification_discoveries
             SET status = 'pending', due_at = ?3, updated_at = ?4,
                 last_error = 'daemon stopped while notification discovery was running'
             WHERE host = ?1 AND viewer_id = ?2 AND status = 'running'",
            params![host, viewer_id, millis(retry_at), millis(now)],
        )?)
    }

    pub fn get_notification(
        &self,
        host: &str,
        viewer_id: i64,
        notification_id: &str,
    ) -> Result<Option<StoredNotification>> {
        self.connection
            .query_row(
                "SELECT host, viewer_id, notification_json, raw_json, first_seen_at, last_seen_at
                 FROM notifications WHERE host = ?1 AND viewer_id = ?2 AND notification_id = ?3",
                params![host, viewer_id, notification_id],
                |row| {
                    Ok(StoredNotification {
                        host: row.get(0)?,
                        viewer_id: row.get(1)?,
                        notification: from_json(row.get_ref(2)?)?,
                        raw: from_json(row.get_ref(3)?)?,
                        first_seen_at: from_millis(row.get(4)?)?,
                        last_seen_at: from_millis(row.get(5)?)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn upsert_tracked_pull_request(&mut self, tracked: &TrackedPullRequest) -> Result<()> {
        self.connection.execute(
            "INSERT INTO tracked_pull_requests
                (host, viewer_id, repository_id, repository_full_name, pull_request_id,
                 pull_request_number, pull_request_json, notification_id,
                 notification_updated_at, notification_json, active, next_check_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
             ON CONFLICT(host, viewer_id, repository_id, pull_request_id) DO UPDATE SET
                 repository_full_name = excluded.repository_full_name,
                 pull_request_number = excluded.pull_request_number,
                 pull_request_json = excluded.pull_request_json,
                 notification_id = excluded.notification_id,
                 notification_updated_at = excluded.notification_updated_at,
                 notification_json = excluded.notification_json,
                 active = excluded.active,
                 next_check_at = excluded.next_check_at,
                 updated_at = excluded.updated_at",
            params![
                tracked.host,
                tracked.viewer_id,
                tracked.repository_id,
                tracked.repository_full_name,
                tracked.pull_request.id,
                i64_from_u64(tracked.pull_request.number)?,
                to_json(&tracked.pull_request)?,
                tracked.notification_id,
                millis(tracked.notification_updated_at),
                to_json(&tracked.notification)?,
                tracked.active,
                millis(tracked.next_check_at),
                millis(tracked.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn due_tracked_pull_requests(
        &self,
        host: &str,
        viewer_id: i64,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<TrackedPullRequest>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut statement = self.connection.prepare(
            "SELECT host, viewer_id, repository_id, repository_full_name, pull_request_json,
                    notification_id, notification_updated_at, notification_json,
                    active, next_check_at, updated_at
             FROM tracked_pull_requests
             WHERE host = ?1 AND viewer_id = ?2 AND active = 1 AND next_check_at <= ?3
             ORDER BY next_check_at, pull_request_id LIMIT ?4",
        )?;
        statement
            .query_map(
                params![host, viewer_id, millis(now), limit],
                map_tracked_pull_request,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn deactivate_tracked_pull_request(
        &mut self,
        host: &str,
        viewer_id: i64,
        repository_id: i64,
        pull_request_id: i64,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self.connection.execute(
            "UPDATE tracked_pull_requests SET active = 0, updated_at = ?5
             WHERE host = ?1 AND viewer_id = ?2 AND repository_id = ?3 AND pull_request_id = ?4",
            params![host, viewer_id, repository_id, pull_request_id, millis(now)],
        )? != 0)
    }

    /// Creates an action idempotently and cancels older undispatched/parked
    /// actions for the same pull request. A currently running action is allowed
    /// to finish; the per-repository claim guard serializes the newer action.
    pub fn enqueue_action(
        &mut self,
        action: &NewReviewAction,
        now: DateTime<Utc>,
    ) -> Result<EnqueueResult> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let base_sha = &action.envelope.pull_request.base_sha;
        let superseded = transaction.execute(
            "UPDATE review_actions SET status = 'cancelled', updated_at = ?6,
                    last_error = 'superseded by newer review action'
             WHERE host = ?1 AND viewer_id = ?2 AND repository_id = ?3
               AND pull_request_id = ?4 AND head_sha <> ?5
               AND status IN ('pending', 'parked')",
            params![
                action.host,
                action.viewer_id,
                action.repository_id,
                action.pull_request_id,
                action.head_sha,
                millis(now),
            ],
        )?;
        let existing: Option<i64> = transaction
            .query_row(
                "SELECT id FROM review_actions
                 WHERE host = ?1 AND viewer_id = ?2 AND repository_id = ?3
                   AND pull_request_id = ?4 AND head_sha = ?5 AND trigger_key = ?6",
                params![
                    action.host,
                    action.viewer_id,
                    action.repository_id,
                    action.pull_request_id,
                    action.head_sha,
                    action.trigger_key,
                ],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(action_id) = existing {
            transaction.execute(
                "UPDATE review_actions SET
                     repository_full_name = ?2, pull_request_number = ?3,
                     base_sha = ?4, reason = ?5, envelope_json = ?6, updated_at = ?7
                 WHERE id = ?1 AND status IN ('pending', 'parked')",
                params![
                    action_id,
                    action.repository_full_name,
                    i64_from_u64(action.pull_request_number)?,
                    base_sha,
                    action.reason,
                    to_json(&action.envelope)?,
                    millis(now),
                ],
            )?;
            transaction.commit()?;
            return Ok(EnqueueResult {
                action_id,
                inserted: false,
                superseded,
            });
        }
        transaction.execute(
            "INSERT INTO review_actions
                (host, viewer_id, repository_id, repository_full_name, pull_request_id,
                 pull_request_number, head_sha, base_sha, trigger_key, reason, envelope_json,
                 status, attempts, due_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                     'pending', 0, ?12, ?13, ?13)",
            params![
                action.host,
                action.viewer_id,
                action.repository_id,
                action.repository_full_name,
                action.pull_request_id,
                i64_from_u64(action.pull_request_number)?,
                action.head_sha,
                base_sha,
                action.trigger_key,
                action.reason,
                to_json(&action.envelope)?,
                millis(action.due_at),
                millis(now),
            ],
        )?;
        let action_id = transaction.query_row(
            "SELECT id FROM review_actions
             WHERE host = ?1 AND viewer_id = ?2 AND repository_id = ?3
               AND pull_request_id = ?4 AND head_sha = ?5 AND trigger_key = ?6",
            params![
                action.host,
                action.viewer_id,
                action.repository_id,
                action.pull_request_id,
                action.head_sha,
                action.trigger_key,
            ],
            |row| row.get(0),
        )?;
        transaction.commit()?;
        Ok(EnqueueResult {
            action_id,
            inserted: true,
            superseded,
        })
    }

    pub fn get_action(&self, action_id: i64) -> Result<Option<ReviewAction>> {
        self.connection
            .query_row(
                &format!("{} WHERE id = ?1", action_select()),
                [action_id],
                map_action,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Atomically claims the oldest due action while enforcing a global
    /// running limit and at most one running action per registered repository.
    /// The attempt is counted at claim time.
    pub fn claim_due_action(
        &mut self,
        host: &str,
        viewer_id: i64,
        global_limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Option<ReviewAction>> {
        if global_limit == 0 {
            return Ok(None);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let running: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM review_actions
             WHERE host = ?1 AND viewer_id = ?2 AND status = 'running'",
            params![host, viewer_id],
            |row| row.get(0),
        )?;
        if running >= i64::try_from(global_limit).unwrap_or(i64::MAX) {
            transaction.commit()?;
            return Ok(None);
        }
        let id: Option<i64> = transaction
            .query_row(
                "SELECT candidate.id FROM review_actions candidate
                 WHERE candidate.host = ?1 AND candidate.viewer_id = ?2
                   AND candidate.status = 'pending' AND candidate.due_at <= ?3
                   AND candidate.attempts < ?4
                   AND NOT EXISTS (
                       SELECT 1 FROM review_actions running
                       WHERE running.host = candidate.host
                         AND running.repository_id = candidate.repository_id
                         AND running.status = 'running')
                 ORDER BY candidate.due_at, candidate.created_at, candidate.id LIMIT 1",
                params![host, viewer_id, millis(now), MAX_ACTION_ATTEMPTS],
                |row| row.get(0),
            )
            .optional()?;
        let Some(id) = id else {
            transaction.commit()?;
            return Ok(None);
        };
        transaction.execute(
            "UPDATE review_actions SET status = 'running', attempts = attempts + 1,
                    updated_at = ?2, last_error = NULL WHERE id = ?1",
            params![id, millis(now)],
        )?;
        let action = transaction.query_row(
            &format!("{} WHERE id = ?1", action_select()),
            [id],
            map_action,
        )?;
        transaction.commit()?;
        Ok(Some(action))
    }

    pub fn complete_action(&mut self, action_id: i64, now: DateTime<Utc>) -> Result<bool> {
        Ok(self.connection.execute(
            "UPDATE review_actions SET status = 'succeeded', updated_at = ?2,
                    completed_at = ?2, last_error = NULL
             WHERE id = ?1 AND status = 'running'",
            params![action_id, millis(now)],
        )? != 0)
    }

    /// Cancels work which has become invalid (for example, because its
    /// repository was unregistered between claim and dispatch). Succeeded and
    /// already-cancelled actions are immutable.
    pub fn cancel_action(
        &mut self,
        action_id: i64,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self.connection.execute(
            "UPDATE review_actions SET status = 'cancelled', last_error = ?2,
                    updated_at = ?3, completed_at = ?3
             WHERE id = ?1 AND status IN ('pending', 'running', 'parked')",
            params![action_id, reason, millis(now)],
        )? != 0)
    }

    /// Records a failed command attempt. The third failure parks the action;
    /// earlier failures become due at `retry_at`.
    pub fn fail_action(
        &mut self,
        action_id: i64,
        error: &str,
        retry_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<ActionStatus>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let attempts: Option<u32> = transaction
            .query_row(
                "SELECT attempts FROM review_actions WHERE id = ?1 AND status = 'running'",
                [action_id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(attempts) = attempts else {
            transaction.commit()?;
            return Ok(None);
        };
        let status = if attempts >= MAX_ACTION_ATTEMPTS {
            ActionStatus::Parked
        } else {
            ActionStatus::Pending
        };
        transaction.execute(
            "UPDATE review_actions SET status = ?2, due_at = ?3, updated_at = ?4,
                    completed_at = CASE WHEN ?2 = 'parked' THEN ?4 ELSE NULL END,
                    last_error = ?5 WHERE id = ?1",
            params![
                action_id,
                status.as_str(),
                millis(retry_at),
                millis(now),
                error
            ],
        )?;
        transaction.commit()?;
        Ok(Some(status))
    }

    /// Defers a claimed action after an infrastructure/preparation failure.
    /// Unlike [`Self::fail_action`], this refunds the attempt counted by
    /// [`Self::claim_due_action`], because only actual review-command launches
    /// count toward the three-attempt limit.
    pub fn defer_action(
        &mut self,
        action_id: i64,
        error: &str,
        retry_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self.connection.execute(
            "UPDATE review_actions SET status = 'pending',
                    attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                    due_at = ?3, updated_at = ?4, completed_at = NULL, last_error = ?2
             WHERE id = ?1 AND status = 'running'",
            params![action_id, error, millis(retry_at), millis(now)],
        )? != 0)
    }

    /// Moves actions left running by an interrupted daemon back to pending, or
    /// parks them if the interrupted attempt was their third.
    pub fn recover_running_actions(
        &mut self,
        host: &str,
        viewer_id: i64,
        retry_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<usize> {
        Ok(self.connection.execute(
            "UPDATE review_actions SET
                 status = CASE WHEN attempts >= ?3 THEN 'parked' ELSE 'pending' END,
                 due_at = ?4, updated_at = ?5,
                 completed_at = CASE WHEN attempts >= ?3 THEN ?5 ELSE NULL END,
                 last_error = 'daemon stopped while review command was running'
             WHERE host = ?1 AND viewer_id = ?2 AND status = 'running'",
            params![
                host,
                viewer_id,
                MAX_ACTION_ATTEMPTS,
                millis(retry_at),
                millis(now)
            ],
        )?)
    }

    /// Resets the newest parked action for `OWNER/REPO#N`. This is the storage
    /// operation behind `github-reviews retry OWNER/REPO#N`.
    pub fn retry_newest_parked(
        &mut self,
        host: &str,
        viewer_id: i64,
        repository_full_name: &str,
        pull_request_number: u64,
        now: DateTime<Utc>,
    ) -> Result<Option<i64>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id: Option<i64> = transaction
            .query_row(
                "SELECT id FROM review_actions
                 WHERE host = ?1 AND viewer_id = ?2 AND repository_full_name = ?3
                   AND pull_request_number = ?4 AND status = 'parked'
                 ORDER BY created_at DESC, id DESC LIMIT 1",
                params![
                    host,
                    viewer_id,
                    repository_full_name,
                    i64_from_u64(pull_request_number)?
                ],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(id) = id {
            transaction.execute(
                "UPDATE review_actions SET status = 'pending', attempts = 0, due_at = ?2,
                        updated_at = ?2, completed_at = NULL, last_error = NULL WHERE id = ?1",
                params![id, millis(now)],
            )?;
        }
        transaction.commit()?;
        Ok(id)
    }

    /// Acquires an expired/new lease, or refreshes a lease already owned by
    /// `owner`. A live lease held by another process is never stolen.
    pub fn acquire_lease(
        &mut self,
        host: &str,
        viewer_id: i64,
        viewer_login: &str,
        owner: &str,
        now: DateTime<Utc>,
        duration: TimeDelta,
    ) -> Result<bool> {
        if duration <= TimeDelta::zero() {
            bail!("lease duration must be positive");
        }
        let expires = now
            .checked_add_signed(duration)
            .ok_or_else(|| anyhow!("lease expiry overflow"))?;
        let changed = self.connection.execute(
            "INSERT INTO daemon_leases
                (host, viewer_id, viewer_login, owner, expires_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(host, viewer_id) DO UPDATE SET
                 viewer_login = excluded.viewer_login,
                 owner = excluded.owner,
                 expires_at = excluded.expires_at,
                 updated_at = excluded.updated_at
             WHERE daemon_leases.expires_at <= ?6 OR daemon_leases.owner = excluded.owner",
            params![
                host,
                viewer_id,
                viewer_login,
                owner,
                millis(expires),
                millis(now)
            ],
        )?;
        Ok(changed != 0)
    }

    pub fn renew_lease(
        &mut self,
        host: &str,
        viewer_id: i64,
        owner: &str,
        now: DateTime<Utc>,
        duration: TimeDelta,
    ) -> Result<bool> {
        if duration <= TimeDelta::zero() {
            bail!("lease duration must be positive");
        }
        let expires = now
            .checked_add_signed(duration)
            .ok_or_else(|| anyhow!("lease expiry overflow"))?;
        Ok(self.connection.execute(
            "UPDATE daemon_leases SET expires_at = ?5, updated_at = ?4
             WHERE host = ?1 AND viewer_id = ?2 AND owner = ?3 AND expires_at > ?4",
            params![host, viewer_id, owner, millis(now), millis(expires)],
        )? != 0)
    }

    pub fn release_lease(&mut self, host: &str, viewer_id: i64, owner: &str) -> Result<bool> {
        Ok(self.connection.execute(
            "DELETE FROM daemon_leases WHERE host = ?1 AND viewer_id = ?2 AND owner = ?3",
            params![host, viewer_id, owner],
        )? != 0)
    }

    pub fn status_snapshot(&self) -> Result<StatusSnapshot> {
        let repositories = self.list_registrations()?;
        let leases = {
            let mut statement = self.connection.prepare(
                "SELECT host, viewer_id, viewer_login, owner, expires_at, updated_at
                 FROM daemon_leases ORDER BY host, viewer_login",
            )?;
            statement
                .query_map([], map_lease)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let poll_states = {
            let mut statement = self.connection.prepare(
                "SELECT host, viewer_id, last_modified, high_water, updated_at
                 FROM poll_state ORDER BY host, viewer_id",
            )?;
            statement
                .query_map([], map_poll_state)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut counts = ActionCounts::default();
        {
            let mut statement = self
                .connection
                .prepare("SELECT status, COUNT(*) FROM review_actions GROUP BY status")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let count: u64 = row.get(1)?;
                match ActionStatus::parse(&row.get::<_, String>(0)?)? {
                    ActionStatus::Pending => counts.pending = count,
                    ActionStatus::Running => counts.running = count,
                    ActionStatus::Succeeded => counts.succeeded = count,
                    ActionStatus::Parked => counts.parked = count,
                    ActionStatus::Cancelled => counts.cancelled = count,
                }
            }
        }
        let pending_and_failed_actions = {
            let mut statement = self.connection.prepare(&format!(
                "{} WHERE status IN ('pending', 'running', 'parked') ORDER BY due_at, id",
                action_select()
            ))?;
            statement
                .query_map([], map_action)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        Ok(StatusSnapshot {
            repositories,
            leases,
            poll_states,
            action_counts: counts,
            pending_and_failed_actions,
        })
    }

    /// Deletes successful/cancelled history, old notifications, and inactive
    /// tracked PRs older than 90 days. Parked and active work is never pruned.
    pub fn prune_older_than(&mut self, cutoff: DateTime<Utc>) -> Result<PruneResult> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actions = transaction.execute(
            "DELETE FROM review_actions
             WHERE status IN ('succeeded', 'cancelled') AND updated_at < ?1",
            [millis(cutoff)],
        )?;
        transaction.execute(
            "DELETE FROM notification_discoveries
             WHERE status = 'completed' AND updated_at < ?1",
            [millis(cutoff)],
        )?;
        let tracked_pull_requests = transaction.execute(
            "DELETE FROM tracked_pull_requests WHERE active = 0 AND updated_at < ?1",
            [millis(cutoff)],
        )?;
        let notifications = transaction.execute(
            "DELETE FROM notifications WHERE last_seen_at < ?1
             AND NOT EXISTS (
                 SELECT 1 FROM tracked_pull_requests tracked
                 WHERE tracked.host = notifications.host
                   AND tracked.viewer_id = notifications.viewer_id
                   AND tracked.notification_id = notifications.notification_id)
             AND NOT EXISTS (
                 SELECT 1 FROM notification_discoveries discovery
                 WHERE discovery.host = notifications.host
                   AND discovery.viewer_id = notifications.viewer_id
                   AND discovery.notification_id = notifications.notification_id)",
            [millis(cutoff)],
        )?;
        transaction.commit()?;
        Ok(PruneResult {
            notifications,
            tracked_pull_requests,
            actions,
        })
    }

    pub fn prune_90_days(&mut self, now: DateTime<Utc>) -> Result<PruneResult> {
        self.prune_older_than(now - TimeDelta::days(90))
    }
}

fn migrate(connection: &mut Connection) -> Result<()> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        bail!("database schema version {version} is newer than supported version {SCHEMA_VERSION}");
    }
    if version == 0 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE registered_repositories (
                host TEXT NOT NULL,
                repository_id INTEGER NOT NULL,
                full_name TEXT NOT NULL,
                html_url TEXT NOT NULL,
                origin_url TEXT NOT NULL,
                local_path TEXT NOT NULL,
                bootstrap_pending INTEGER NOT NULL CHECK (bootstrap_pending IN (0, 1)),
                registered_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (host, repository_id),
                UNIQUE (host, full_name)
             );
             CREATE TABLE metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
             );
             CREATE TABLE poll_state (
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                last_modified TEXT,
                high_water TEXT,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (host, viewer_id)
             );
             CREATE TABLE repository_bootstraps (
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                repository_id INTEGER NOT NULL,
                pending INTEGER NOT NULL CHECK (pending IN (0, 1)),
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (host, viewer_id, repository_id),
                FOREIGN KEY (host, repository_id)
                    REFERENCES registered_repositories(host, repository_id)
                    ON DELETE CASCADE
             );
             CREATE TABLE daemon_leases (
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                viewer_login TEXT NOT NULL,
                owner TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (host, viewer_id)
             );
             CREATE TABLE notifications (
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                notification_id TEXT NOT NULL,
                repository_id INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                notification_json TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                first_seen_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                PRIMARY KEY (host, viewer_id, notification_id)
             );
             CREATE INDEX notifications_repository
                ON notifications(host, viewer_id, repository_id, updated_at);
             CREATE TABLE notification_discoveries (
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                notification_id TEXT NOT NULL,
                repository_id INTEGER NOT NULL,
                notification_updated_at INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending','running','completed')),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                due_at INTEGER NOT NULL,
                last_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                PRIMARY KEY (host, viewer_id, notification_id)
             );
             CREATE INDEX notification_discoveries_due
                ON notification_discoveries(host, viewer_id, status, due_at, created_at);
             CREATE TABLE tracked_pull_requests (
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                repository_id INTEGER NOT NULL,
                repository_full_name TEXT NOT NULL,
                pull_request_id INTEGER NOT NULL,
                pull_request_number INTEGER NOT NULL,
                pull_request_json TEXT NOT NULL,
                notification_id TEXT NOT NULL,
                notification_updated_at INTEGER NOT NULL,
                notification_json TEXT NOT NULL,
                active INTEGER NOT NULL CHECK (active IN (0, 1)),
                next_check_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (host, viewer_id, repository_id, pull_request_id)
             );
             CREATE INDEX tracked_pull_requests_due
                ON tracked_pull_requests(host, viewer_id, active, next_check_at);
             CREATE TABLE review_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                repository_id INTEGER NOT NULL,
                repository_full_name TEXT NOT NULL,
                pull_request_id INTEGER NOT NULL,
                pull_request_number INTEGER NOT NULL,
                head_sha TEXT NOT NULL,
                base_sha TEXT NOT NULL,
                trigger_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                envelope_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','parked','cancelled')),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                due_at INTEGER NOT NULL,
                last_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                UNIQUE (host, viewer_id, repository_id, pull_request_id,
                        head_sha, trigger_key)
             );
             CREATE INDEX review_actions_due
                ON review_actions(host, viewer_id, status, due_at, created_at);
             CREATE INDEX review_actions_repository_running
                ON review_actions(host, repository_id, status);
             CREATE INDEX review_actions_retry
                ON review_actions(repository_full_name, pull_request_number, status, created_at);",
        )?;
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.commit()?;
    }
    if version == 3 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "ALTER TABLE review_actions RENAME TO review_actions_v3;
             CREATE TABLE review_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                repository_id INTEGER NOT NULL,
                repository_full_name TEXT NOT NULL,
                pull_request_id INTEGER NOT NULL,
                pull_request_number INTEGER NOT NULL,
                head_sha TEXT NOT NULL,
                base_sha TEXT NOT NULL,
                trigger_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                envelope_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','parked','cancelled')),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                due_at INTEGER NOT NULL,
                last_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                UNIQUE (host, viewer_id, repository_id, pull_request_id,
                        head_sha, trigger_key)
             );
             INSERT INTO review_actions (
                id, host, viewer_id, repository_id, repository_full_name,
                pull_request_id, pull_request_number, head_sha, base_sha,
                trigger_key, reason, envelope_json, status, attempts, due_at,
                last_error, created_at, updated_at, completed_at)
             SELECT candidate.id, candidate.host, candidate.viewer_id,
                    candidate.repository_id, candidate.repository_full_name,
                    candidate.pull_request_id, candidate.pull_request_number,
                    candidate.head_sha, candidate.base_sha, candidate.trigger_key,
                    candidate.reason, candidate.envelope_json, candidate.status,
                    candidate.attempts, candidate.due_at, candidate.last_error,
                    candidate.created_at, candidate.updated_at, candidate.completed_at
             FROM review_actions_v3 candidate
             WHERE candidate.id = (
                SELECT contender.id FROM review_actions_v3 contender
                WHERE contender.host = candidate.host
                  AND contender.viewer_id = candidate.viewer_id
                  AND contender.repository_id = candidate.repository_id
                  AND contender.pull_request_id = candidate.pull_request_id
                  AND contender.head_sha = candidate.head_sha
                  AND contender.trigger_key = candidate.trigger_key
                ORDER BY CASE contender.status
                    WHEN 'succeeded' THEN 0
                    WHEN 'running' THEN 1
                    WHEN 'pending' THEN 2
                    WHEN 'parked' THEN 3
                    ELSE 4 END,
                    contender.updated_at DESC, contender.id DESC
                LIMIT 1
             );
             DROP TABLE review_actions_v3;
             CREATE INDEX review_actions_due
                ON review_actions(host, viewer_id, status, due_at, created_at);
             CREATE INDEX review_actions_repository_running
                ON review_actions(host, repository_id, status);
             CREATE INDEX review_actions_retry
                ON review_actions(repository_full_name, pull_request_number, status, created_at);",
        )?;
        transaction.pragma_update(None, "user_version", 4)?;
        transaction.commit()?;
    }
    if version == 1 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "ALTER TABLE review_actions RENAME TO review_actions_v1;
             CREATE TABLE review_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                repository_id INTEGER NOT NULL,
                repository_full_name TEXT NOT NULL,
                pull_request_id INTEGER NOT NULL,
                pull_request_number INTEGER NOT NULL,
                head_sha TEXT NOT NULL,
                base_sha TEXT NOT NULL,
                trigger_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                envelope_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','parked','cancelled')),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                due_at INTEGER NOT NULL,
                last_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                UNIQUE (host, viewer_id, repository_id, pull_request_id,
                        head_sha, base_sha, trigger_key)
             );
             INSERT INTO review_actions (
                id, host, viewer_id, repository_id, repository_full_name,
                pull_request_id, pull_request_number, head_sha, base_sha,
                trigger_key, reason, envelope_json, status, attempts, due_at,
                last_error, created_at, updated_at, completed_at)
             SELECT id, host, viewer_id, repository_id, repository_full_name,
                    pull_request_id, pull_request_number, head_sha,
                    COALESCE(json_extract(envelope_json, '$.pull_request.base_sha'), ''),
                    trigger_key, reason, envelope_json, status, attempts, due_at,
                    last_error, created_at, updated_at, completed_at
             FROM review_actions_v1;
             DROP TABLE review_actions_v1;
             CREATE INDEX review_actions_due
                ON review_actions(host, viewer_id, status, due_at, created_at);
             CREATE INDEX review_actions_repository_running
                ON review_actions(host, repository_id, status);
             CREATE INDEX review_actions_retry
                ON review_actions(repository_full_name, pull_request_number, status, created_at);",
        )?;
        transaction.pragma_update(None, "user_version", 2)?;
        transaction.commit()?;
    }
    if version == 1 || version == 2 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE repository_bootstraps (
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                repository_id INTEGER NOT NULL,
                pending INTEGER NOT NULL CHECK (pending IN (0, 1)),
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (host, viewer_id, repository_id),
                FOREIGN KEY (host, repository_id)
                    REFERENCES registered_repositories(host, repository_id)
                    ON DELETE CASCADE
             );",
        )?;
        transaction.pragma_update(None, "user_version", 3)?;
        transaction.commit()?;
    }
    if version == 1 || version == 2 {
        return migrate(connection);
    }
    if version == 3 || version == 4 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE notification_discoveries (
                host TEXT NOT NULL,
                viewer_id INTEGER NOT NULL,
                notification_id TEXT NOT NULL,
                repository_id INTEGER NOT NULL,
                notification_updated_at INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending','running','completed')),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                due_at INTEGER NOT NULL,
                last_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                PRIMARY KEY (host, viewer_id, notification_id)
             );
             CREATE INDEX notification_discoveries_due
                ON notification_discoveries(host, viewer_id, status, due_at, created_at);",
        )?;
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.commit()?;
    }
    Ok(())
}

fn upsert_poll_state(
    connection: &Connection,
    host: &str,
    viewer_id: i64,
    last_modified: Option<&str>,
    high_water: Option<&str>,
    now: DateTime<Utc>,
) -> Result<()> {
    connection.execute(
        "INSERT INTO poll_state(host, viewer_id, last_modified, high_water, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(host, viewer_id) DO UPDATE SET
             last_modified = excluded.last_modified,
             high_water = excluded.high_water,
             updated_at = excluded.updated_at",
        params![host, viewer_id, last_modified, high_water, millis(now)],
    )?;
    Ok(())
}

fn upsert_notification(
    transaction: &Transaction<'_>,
    host: &str,
    viewer_id: i64,
    notification: &Notification,
    raw: &Value,
    now: DateTime<Utc>,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO notifications
            (host, viewer_id, notification_id, repository_id, updated_at,
             notification_json, raw_json, first_seen_at, last_seen_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
         ON CONFLICT(host, viewer_id, notification_id) DO UPDATE SET
             repository_id = CASE WHEN excluded.updated_at >= notifications.updated_at
                THEN excluded.repository_id ELSE notifications.repository_id END,
             updated_at = MAX(excluded.updated_at, notifications.updated_at),
             notification_json = CASE WHEN excluded.updated_at >= notifications.updated_at
                THEN excluded.notification_json ELSE notifications.notification_json END,
             raw_json = CASE WHEN excluded.updated_at >= notifications.updated_at
                THEN excluded.raw_json ELSE notifications.raw_json END,
             last_seen_at = excluded.last_seen_at",
        params![
            host,
            viewer_id,
            notification.id,
            notification.repository.id,
            millis(notification.updated_at),
            to_json(notification)?,
            to_json(raw)?,
            millis(now),
        ],
    )?;
    if notification.reason == "review_requested" && notification.subject.kind == "PullRequest" {
        transaction.execute(
            "INSERT INTO notification_discoveries
                (host, viewer_id, notification_id, repository_id,
                 notification_updated_at, status, attempts, due_at,
                 created_at, updated_at)
             SELECT ?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?6, ?6
             WHERE EXISTS (
                 SELECT 1 FROM registered_repositories
                 WHERE host = ?1 AND repository_id = ?4)
             ON CONFLICT(host, viewer_id, notification_id) DO UPDATE SET
                 repository_id = excluded.repository_id,
                 notification_updated_at = excluded.notification_updated_at,
                 status = CASE
                    WHEN excluded.notification_updated_at > notification_discoveries.notification_updated_at
                    THEN 'pending' ELSE notification_discoveries.status END,
                 attempts = CASE
                    WHEN excluded.notification_updated_at > notification_discoveries.notification_updated_at
                    THEN 0 ELSE notification_discoveries.attempts END,
                 due_at = CASE
                    WHEN excluded.notification_updated_at > notification_discoveries.notification_updated_at
                    THEN excluded.due_at ELSE notification_discoveries.due_at END,
                 last_error = CASE
                    WHEN excluded.notification_updated_at > notification_discoveries.notification_updated_at
                    THEN NULL ELSE notification_discoveries.last_error END,
                 completed_at = CASE
                    WHEN excluded.notification_updated_at > notification_discoveries.notification_updated_at
                    THEN NULL ELSE notification_discoveries.completed_at END,
                 updated_at = excluded.updated_at",
            params![
                host,
                viewer_id,
                notification.id,
                notification.repository.id,
                millis(notification.updated_at),
                millis(now),
            ],
        )?;
    }
    Ok(())
}

fn discovery_select() -> &'static str {
    "SELECT discovery.host, discovery.viewer_id, discovery.notification_id,
            discovery.repository_id, discovery.notification_updated_at,
            notification.notification_json, notification.raw_json,
            discovery.status, discovery.attempts, discovery.due_at,
            discovery.last_error, discovery.created_at, discovery.updated_at
     FROM notification_discoveries discovery
     JOIN notifications notification
       ON notification.host = discovery.host
      AND notification.viewer_id = discovery.viewer_id
      AND notification.notification_id = discovery.notification_id"
}

fn action_select() -> &'static str {
    "SELECT id, host, viewer_id, repository_id, repository_full_name,
            pull_request_id, pull_request_number, head_sha, base_sha, trigger_key, reason,
            envelope_json, status, attempts, due_at, last_error, created_at, updated_at
     FROM review_actions"
}

fn map_registration(row: &rusqlite::Row<'_>) -> rusqlite::Result<RegisteredRepositoryRecord> {
    Ok(RegisteredRepositoryRecord {
        host: row.get(0)?,
        repository: RegisteredRepository {
            repository: RepositoryIdentity {
                id: row.get(1)?,
                full_name: row.get(2)?,
                html_url: row.get(3)?,
            },
            origin_url: row.get(4)?,
            local_path: PathBuf::from(row.get::<_, String>(5)?),
            bootstrap_pending: row.get(6)?,
        },
        registered_at: from_millis(row.get(7)?)?,
        updated_at: from_millis(row.get(8)?)?,
    })
}

fn map_poll_state(row: &rusqlite::Row<'_>) -> rusqlite::Result<PollState> {
    Ok(PollState {
        host: row.get(0)?,
        viewer_id: row.get(1)?,
        last_modified: row.get(2)?,
        high_water: row.get(3)?,
        updated_at: from_millis(row.get(4)?)?,
    })
}

fn map_tracked_pull_request(row: &rusqlite::Row<'_>) -> rusqlite::Result<TrackedPullRequest> {
    Ok(TrackedPullRequest {
        host: row.get(0)?,
        viewer_id: row.get(1)?,
        repository_id: row.get(2)?,
        repository_full_name: row.get(3)?,
        pull_request: from_json(row.get_ref(4)?)?,
        notification_id: row.get(5)?,
        notification_updated_at: from_millis(row.get(6)?)?,
        notification: from_json(row.get_ref(7)?)?,
        active: row.get(8)?,
        next_check_at: from_millis(row.get(9)?)?,
        updated_at: from_millis(row.get(10)?)?,
    })
}

fn map_action(row: &rusqlite::Row<'_>) -> rusqlite::Result<ReviewAction> {
    let number: i64 = row.get(6)?;
    Ok(ReviewAction {
        id: row.get(0)?,
        host: row.get(1)?,
        viewer_id: row.get(2)?,
        repository_id: row.get(3)?,
        repository_full_name: row.get(4)?,
        pull_request_id: row.get(5)?,
        pull_request_number: u64::try_from(number).map_err(invalid_data)?,
        head_sha: row.get(7)?,
        base_sha: row.get(8)?,
        trigger_key: row.get(9)?,
        reason: row.get(10)?,
        envelope: from_json(row.get_ref(11)?)?,
        status: ActionStatus::parse(&row.get::<_, String>(12)?).map_err(invalid_data)?,
        attempts: row.get(13)?,
        due_at: from_millis(row.get(14)?)?,
        last_error: row.get(15)?,
        created_at: from_millis(row.get(16)?)?,
        updated_at: from_millis(row.get(17)?)?,
    })
}

fn map_discovery(row: &rusqlite::Row<'_>) -> rusqlite::Result<NotificationDiscovery> {
    Ok(NotificationDiscovery {
        host: row.get(0)?,
        viewer_id: row.get(1)?,
        notification_id: row.get(2)?,
        repository_id: row.get(3)?,
        notification_updated_at: from_millis(row.get(4)?)?,
        notification: from_json(row.get_ref(5)?)?,
        raw: from_json(row.get_ref(6)?)?,
        status: DiscoveryStatus::parse(&row.get::<_, String>(7)?).map_err(invalid_data)?,
        attempts: row.get(8)?,
        due_at: from_millis(row.get(9)?)?,
        last_error: row.get(10)?,
        created_at: from_millis(row.get(11)?)?,
        updated_at: from_millis(row.get(12)?)?,
    })
}

fn map_lease(row: &rusqlite::Row<'_>) -> rusqlite::Result<DaemonLease> {
    Ok(DaemonLease {
        host: row.get(0)?,
        viewer_id: row.get(1)?,
        viewer_login: row.get(2)?,
        owner: row.get(3)?,
        expires_at: from_millis(row.get(4)?)?,
        updated_at: from_millis(row.get(5)?)?,
    })
}

fn millis(value: DateTime<Utc>) -> i64 {
    value.timestamp_millis()
}

fn from_millis(value: i64) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value)
        .ok_or_else(|| invalid_data(format!("invalid UTC timestamp in database: {value}")))
}

fn to_json<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("serialize database JSON")
}

fn from_json<T: for<'de> Deserialize<'de>>(
    value: rusqlite::types::ValueRef<'_>,
) -> rusqlite::Result<T> {
    let text = value.as_str()?;
    serde_json::from_str(text).map_err(invalid_data)
}

fn invalid_data(error: impl std::fmt::Display) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )),
    )
}

fn i64_from_u64(value: u64) -> Result<i64> {
    i64::try_from(value).context("GitHub number exceeds SQLite integer range")
}

#[cfg(unix)]
fn set_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("secure state directory {}", path.display()))
}

#[cfg(not(unix))]
fn set_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("secure state database {}", path.display()))
}

#[cfg(not(unix))]
fn set_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use chrono::TimeZone;
    use serde_json::json;
    use tempfile::TempDir;

    use crate::model::{
        ActionReason, EnvelopePullRequest, EnvelopeRepository, EnvelopeRequest,
        NotificationRepository, NotificationSubject, Viewer,
    };

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    fn database() -> (TempDir, Database) {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("state/state.sqlite3")).unwrap();
        (directory, database)
    }

    fn registered(repository_id: i64, name: &str, path: &str) -> RegisteredRepository {
        RegisteredRepository {
            repository: RepositoryIdentity {
                id: repository_id,
                full_name: name.into(),
                html_url: format!("https://github.com/{name}"),
            },
            origin_url: format!("git@github.com:{name}.git"),
            local_path: PathBuf::from(path),
            bootstrap_pending: true,
        }
    }

    fn notification(repository_id: i64, id: &str, updated_at: DateTime<Utc>) -> Notification {
        Notification {
            id: id.into(),
            reason: "review_requested".into(),
            unread: true,
            updated_at,
            repository: NotificationRepository {
                id: repository_id,
                full_name: format!("acme/repo-{repository_id}"),
                extra: Default::default(),
            },
            subject: NotificationSubject {
                title: "Please review".into(),
                kind: "PullRequest".into(),
                url: Some("https://api.github.com/repos/acme/repo/pulls/7".into()),
                extra: Default::default(),
            },
            extra: Default::default(),
        }
    }

    fn pull(id: i64, number: u64, head: &str) -> PullRequestState {
        PullRequestState {
            id,
            number,
            url: format!("https://github.com/acme/repo/pull/{number}"),
            title: "Improve the thing".into(),
            author: Viewer {
                id: 22,
                login: "author".into(),
            },
            state: "open".into(),
            draft: false,
            merged: false,
            head_sha: head.into(),
            head_ref: "feature".into(),
            base_sha: "base".into(),
            base_ref: "main".into(),
            directly_requested: true,
            requested_team_ids: vec![],
            latest_review: None,
            latest_request: None,
            non_opinionated_review_after_request: false,
        }
    }

    fn envelope(repository_id: i64, pull: &PullRequestState) -> ReviewEnvelope {
        ReviewEnvelope {
            schema_version: 1,
            reason: ActionReason::ReviewRequested,
            viewer: Viewer {
                id: 11,
                login: "reviewer".into(),
            },
            repository: EnvelopeRepository {
                id: repository_id,
                full_name: format!("acme/repo-{repository_id}"),
                local_path: PathBuf::from(format!("/work/repo-{repository_id}")),
            },
            request: EnvelopeRequest {
                kind: "user".into(),
                event_id: "request-1".into(),
            },
            pull_request: EnvelopePullRequest {
                id: pull.id,
                number: pull.number,
                url: pull.url.clone(),
                title: pull.title.clone(),
                author: pull.author.clone(),
                head_sha: pull.head_sha.clone(),
                base_sha: pull.base_sha.clone(),
                base_ref: pull.base_ref.clone(),
                head_ref: pull.head_ref.clone(),
            },
            review: None,
            notification: json!({"id": "notification-1"}),
        }
    }

    fn new_action(
        repository_id: i64,
        pull_id: i64,
        pull_number: u64,
        head: &str,
        trigger: &str,
        due_at: DateTime<Utc>,
    ) -> NewReviewAction {
        let pull = pull(pull_id, pull_number, head);
        NewReviewAction {
            host: "github.com".into(),
            viewer_id: 11,
            repository_id,
            repository_full_name: format!("acme/repo-{repository_id}"),
            pull_request_id: pull_id,
            pull_request_number: pull_number,
            head_sha: head.into(),
            trigger_key: trigger.into(),
            reason: "review_requested".into(),
            envelope: envelope(repository_id, &pull),
            due_at,
        }
    }

    #[test]
    fn registration_is_upserted_by_immutable_repository_identity() {
        let (_directory, mut database) = database();
        database
            .upsert_registration(
                "github.com",
                &registered(7, "acme/widgets", "/first"),
                at(10),
            )
            .unwrap();
        assert_eq!(
            database.bootstrap_pending("github.com", 11, 7).unwrap(),
            Some(true)
        );
        database
            .mark_bootstrap_complete("github.com", 11, 7)
            .unwrap();
        assert_eq!(
            database.bootstrap_pending("github.com", 11, 7).unwrap(),
            Some(false)
        );
        assert_eq!(
            database.bootstrap_pending("github.com", 12, 7).unwrap(),
            Some(true)
        );
        database
            .upsert_registration(
                "github.com",
                &registered(7, "renamed/widgets", "/second"),
                at(20),
            )
            .unwrap();

        let record = database.get_registration("github.com", 7).unwrap().unwrap();
        assert_eq!(record.repository.repository.full_name, "renamed/widgets");
        assert_eq!(record.repository.local_path, Path::new("/second"));
        assert!(record.repository.bootstrap_pending);
        assert_eq!(
            database.bootstrap_pending("github.com", 11, 7).unwrap(),
            Some(true),
            "a changed registration must invalidate each viewer's bootstrap"
        );
        assert_eq!(record.registered_at, at(10));
        assert_eq!(record.updated_at, at(20));
        assert_eq!(database.list_registrations().unwrap().len(), 1);
        assert!(database.remove_registration("github.com", 7).unwrap());
        assert!(!database.remove_registration("github.com", 7).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn database_and_state_directory_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let (directory, database) = database();
        let state_directory = directory.path().join("state");
        assert_eq!(
            fs::metadata(state_directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(database.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn migrates_v1_actions_and_adds_viewer_scoped_bootstrap_state() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state");
        fs::create_dir_all(&state_directory).unwrap();
        let path = state_directory.join("state.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE registered_repositories (
                    host TEXT NOT NULL,
                    repository_id INTEGER NOT NULL,
                    full_name TEXT NOT NULL,
                    html_url TEXT NOT NULL,
                    origin_url TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    bootstrap_pending INTEGER NOT NULL,
                    registered_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (host, repository_id),
                    UNIQUE (host, full_name)
                 );
                 CREATE TABLE review_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host TEXT NOT NULL,
                    viewer_id INTEGER NOT NULL,
                    repository_id INTEGER NOT NULL,
                    repository_full_name TEXT NOT NULL,
                    pull_request_id INTEGER NOT NULL,
                    pull_request_number INTEGER NOT NULL,
                    head_sha TEXT NOT NULL,
                    trigger_key TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    due_at INTEGER NOT NULL,
                    last_error TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    completed_at INTEGER,
                    UNIQUE (host, viewer_id, repository_id, pull_request_id,
                            head_sha, trigger_key)
                 );
                 CREATE INDEX review_actions_due
                    ON review_actions(host, viewer_id, status, due_at, created_at);
                 CREATE INDEX review_actions_repository_running
                    ON review_actions(host, viewer_id, repository_id, status);
                 CREATE INDEX review_actions_retry
                    ON review_actions(repository_full_name, pull_request_number, status, created_at);
                 PRAGMA user_version = 1;",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO registered_repositories VALUES
                 ('github.com', 7, 'acme/repo-7', 'https://github.com/acme/repo-7',
                  'git@github.com:acme/repo-7.git', '/work/repo-7', 1, 1, 1)",
                [],
            )
            .unwrap();
        let envelope = envelope(7, &pull(70, 7, "head"));
        connection
            .execute(
                "INSERT INTO review_actions
                    (host, viewer_id, repository_id, repository_full_name,
                     pull_request_id, pull_request_number, head_sha, trigger_key,
                     reason, envelope_json, status, attempts, due_at, created_at, updated_at)
                 VALUES ('github.com', 11, 7, 'acme/repo-7', 70, 7, 'head',
                         'request-1', 'review_requested', ?1, 'pending', 0, 1, 1, 1)",
                [to_json(&envelope).unwrap()],
            )
            .unwrap();
        drop(connection);

        let mut database = Database::open(&path).unwrap();
        let migrated = database.get_action(1).unwrap().unwrap();
        assert_eq!(migrated.base_sha, "base");
        assert_eq!(
            database.bootstrap_pending("github.com", 11, 7).unwrap(),
            Some(true)
        );
        database
            .mark_bootstrap_complete("github.com", 11, 7)
            .unwrap();
        assert_eq!(
            database.bootstrap_pending("github.com", 11, 7).unwrap(),
            Some(false)
        );
        let version: i64 = database
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn v4_identity_migration_collapses_base_only_duplicates_without_replaying_success() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state");
        fs::create_dir_all(&state_directory).unwrap();
        let path = state_directory.join("state.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE review_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host TEXT NOT NULL,
                    viewer_id INTEGER NOT NULL,
                    repository_id INTEGER NOT NULL,
                    repository_full_name TEXT NOT NULL,
                    pull_request_id INTEGER NOT NULL,
                    pull_request_number INTEGER NOT NULL,
                    head_sha TEXT NOT NULL,
                    base_sha TEXT NOT NULL,
                    trigger_key TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    due_at INTEGER NOT NULL,
                    last_error TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    completed_at INTEGER,
                    UNIQUE (host, viewer_id, repository_id, pull_request_id,
                            head_sha, base_sha, trigger_key)
                 );
                 CREATE INDEX review_actions_due
                    ON review_actions(host, viewer_id, status, due_at, created_at);
                 CREATE INDEX review_actions_repository_running
                    ON review_actions(host, repository_id, status);
                 CREATE INDEX review_actions_retry
                    ON review_actions(repository_full_name, pull_request_number, status, created_at);
                 PRAGMA user_version = 3;",
            )
            .unwrap();
        let old_envelope = envelope(7, &pull(70, 7, "head"));
        let mut new_envelope = old_envelope.clone();
        new_envelope.pull_request.base_sha = "new-base".into();
        for (base, payload, status, created) in [
            ("base", old_envelope, "succeeded", 1_i64),
            ("new-base", new_envelope, "pending", 2_i64),
        ] {
            connection
                .execute(
                    "INSERT INTO review_actions
                        (host, viewer_id, repository_id, repository_full_name,
                         pull_request_id, pull_request_number, head_sha, base_sha,
                         trigger_key, reason, envelope_json, status, attempts,
                         due_at, created_at, updated_at)
                     VALUES ('github.com', 11, 7, 'acme/repo-7', 70, 7, 'head',
                             ?1, 'request-1', 'review_requested', ?2, ?3, 1, 1, ?4, ?4)",
                    params![base, to_json(&payload).unwrap(), status, created],
                )
                .unwrap();
        }
        drop(connection);

        let database = Database::open(&path).unwrap();
        let actions: i64 = database
            .connection
            .query_row("SELECT COUNT(*) FROM review_actions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(actions, 1);
        let retained = database.get_action(1).unwrap().unwrap();
        assert_eq!(retained.status, ActionStatus::Succeeded);
        assert_eq!(retained.base_sha, "base");
        let version: i64 = database
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn a_complete_poll_stores_raw_notifications_and_watermark_together() {
        let (_directory, mut database) = database();
        database
            .upsert_registration(
                "github.com",
                &registered(7, "acme/repo-7", "/work/repo-7"),
                at(40),
            )
            .unwrap();
        let item = notification(7, "100", at(50));
        let raw = json!({"id": "100", "unknown": {"preserved": true}});
        database
            .commit_poll(
                "github.com",
                11,
                &[(item, raw.clone())],
                Some("Wed, 01 Jan 2025 00:00:00 GMT"),
                Some("100@50"),
                at(60),
            )
            .unwrap();

        let stored = database
            .get_notification("github.com", 11, "100")
            .unwrap()
            .unwrap();
        assert_eq!(stored.raw, raw);
        assert_eq!(stored.notification.updated_at, at(50));
        let state = database.poll_state("github.com", 11).unwrap().unwrap();
        assert_eq!(state.high_water.as_deref(), Some("100@50"));
        assert_eq!(
            state.last_modified.as_deref(),
            Some("Wed, 01 Jan 2025 00:00:00 GMT")
        );
        let discoveries = database
            .list_pending_discoveries("github.com", 11, at(60), 10)
            .unwrap();
        assert_eq!(discoveries.len(), 1);
        assert_eq!(discoveries[0].notification_id, "100");
        assert_eq!(discoveries[0].raw, raw);
        assert_eq!(
            database
                .due_discovery_count("github.com", 11, at(60))
                .unwrap(),
            1
        );
    }

    #[test]
    fn discovery_queue_retries_independently_and_new_notification_revision_reopens_it() {
        let (_directory, mut database) = database();
        database
            .upsert_registration(
                "github.com",
                &registered(7, "acme/repo-7", "/work/repo-7"),
                at(40),
            )
            .unwrap();
        let item = notification(7, "100", at(50));
        database
            .commit_poll(
                "github.com",
                11,
                &[(item.clone(), json!({"id": "100", "revision": 1}))],
                None,
                Some("100@50"),
                at(60),
            )
            .unwrap();

        let claimed = database
            .claim_pending_discovery("github.com", 11, at(60))
            .unwrap()
            .unwrap();
        assert_eq!(claimed.attempts, 1);
        assert!(
            database
                .defer_discovery(
                    "github.com",
                    11,
                    "100",
                    "temporary GitHub failure",
                    at(80),
                    at(61),
                )
                .unwrap()
        );
        assert!(
            database
                .list_pending_discoveries("github.com", 11, at(79), 10)
                .unwrap()
                .is_empty()
        );
        let retried = database
            .claim_pending_discovery("github.com", 11, at(80))
            .unwrap()
            .unwrap();
        assert_eq!(retried.attempts, 2);
        database
            .complete_discovery("github.com", 11, "100", at(81))
            .unwrap();

        // Seeing the same notification revision again is idempotent.
        database
            .commit_poll(
                "github.com",
                11,
                &[(item, json!({"id": "100", "revision": 1}))],
                None,
                Some("100@50"),
                at(82),
            )
            .unwrap();
        assert!(
            database
                .list_pending_discoveries("github.com", 11, at(82), 10)
                .unwrap()
                .is_empty()
        );

        let newer = notification(7, "100", at(90));
        database
            .commit_poll(
                "github.com",
                11,
                &[(newer, json!({"id": "100", "revision": 2}))],
                None,
                Some("100@90"),
                at(91),
            )
            .unwrap();
        let reopened = database
            .claim_pending_discovery("github.com", 11, at(91))
            .unwrap()
            .unwrap();
        assert_eq!(reopened.attempts, 1);
        assert_eq!(reopened.notification_updated_at, at(90));
        assert_eq!(reopened.raw["revision"], 2);
    }

    #[test]
    fn bootstrap_storage_queue_and_acknowledgement_are_atomic() {
        let (_directory, mut database) = database();
        database
            .upsert_registration(
                "github.com",
                &registered(7, "acme/repo-7", "/work/repo-7"),
                at(40),
            )
            .unwrap();
        let item = notification(7, "100", at(50));
        assert!(
            database
                .commit_bootstrap("github.com", 11, 7, &[(item, json!({"id": "100"}))], at(60),)
                .unwrap()
        );
        assert_eq!(
            database.bootstrap_pending("github.com", 11, 7).unwrap(),
            Some(false)
        );
        assert_eq!(
            database
                .list_pending_discoveries("github.com", 11, at(60), 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn daemon_lease_cannot_be_stolen_before_expiry() {
        let (_directory, mut database) = database();
        assert!(
            database
                .acquire_lease(
                    "github.com",
                    11,
                    "reviewer",
                    "daemon-a",
                    at(100),
                    TimeDelta::seconds(30),
                )
                .unwrap()
        );
        assert!(
            !database
                .acquire_lease(
                    "github.com",
                    11,
                    "reviewer",
                    "daemon-b",
                    at(120),
                    TimeDelta::seconds(30),
                )
                .unwrap()
        );
        assert!(
            database
                .renew_lease(
                    "github.com",
                    11,
                    "daemon-a",
                    at(120),
                    TimeDelta::seconds(30),
                )
                .unwrap()
        );
        assert!(
            database
                .acquire_lease(
                    "github.com",
                    11,
                    "reviewer",
                    "daemon-b",
                    at(51 + 100),
                    TimeDelta::seconds(30),
                )
                .unwrap()
        );
        assert!(
            !database
                .release_lease("github.com", 11, "daemon-a")
                .unwrap()
        );
        assert!(
            database
                .release_lease("github.com", 11, "daemon-b")
                .unwrap()
        );
    }

    #[test]
    fn tracked_pull_request_keeps_notification_and_is_scheduled() {
        let (_directory, mut database) = database();
        let tracked = TrackedPullRequest {
            host: "github.com".into(),
            viewer_id: 11,
            repository_id: 7,
            repository_full_name: "acme/widgets".into(),
            pull_request: pull(70, 7, "head"),
            notification_id: "100".into(),
            notification_updated_at: at(50),
            notification: json!({"id": "100", "raw": true}),
            active: true,
            next_check_at: at(70),
            updated_at: at(60),
        };
        database.upsert_tracked_pull_request(&tracked).unwrap();
        assert!(
            database
                .due_tracked_pull_requests("github.com", 11, at(69), 10)
                .unwrap()
                .is_empty()
        );
        let due = database
            .due_tracked_pull_requests("github.com", 11, at(70), 10)
            .unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].notification, tracked.notification);
        assert_eq!(due[0].notification_updated_at, at(50));
    }

    #[test]
    fn actions_are_idempotent_and_new_heads_supersede_undispatched_work() {
        let (_directory, mut database) = database();
        let old = new_action(7, 70, 7, "old", "request-1", at(100));
        let first = database.enqueue_action(&old, at(90)).unwrap();
        let duplicate = database.enqueue_action(&old, at(91)).unwrap();
        assert!(first.inserted);
        assert!(!duplicate.inserted);
        assert_eq!(duplicate.action_id, first.action_id);

        let new = new_action(7, 70, 7, "new", "request-1", at(100));
        let second = database.enqueue_action(&new, at(92)).unwrap();
        assert_eq!(second.superseded, 1);
        assert_eq!(
            database
                .get_action(first.action_id)
                .unwrap()
                .unwrap()
                .status,
            ActionStatus::Cancelled
        );
        assert_eq!(
            database
                .get_action(second.action_id)
                .unwrap()
                .unwrap()
                .status,
            ActionStatus::Pending
        );
    }

    #[test]
    fn a_new_base_refreshes_pending_payload_without_creating_a_new_action() {
        let (_directory, mut database) = database();
        let old = new_action(7, 70, 7, "head", "request-1", at(100));
        let old_id = database.enqueue_action(&old, at(90)).unwrap().action_id;
        database
            .claim_due_action("github.com", 11, 4, at(100))
            .unwrap()
            .unwrap();
        database
            .fail_action(old_id, "command failed", at(130), at(101))
            .unwrap();

        let mut rebased = new_action(7, 70, 7, "head", "request-1", at(100));
        rebased.envelope.pull_request.base_sha = "new-base".into();
        let result = database.enqueue_action(&rebased, at(110)).unwrap();

        assert!(!result.inserted);
        assert_eq!(result.superseded, 0);
        assert_eq!(result.action_id, old_id);
        let refreshed = database.get_action(old_id).unwrap().unwrap();
        assert_eq!(refreshed.status, ActionStatus::Pending);
        assert_eq!(refreshed.attempts, 1);
        assert_eq!(refreshed.due_at, at(130));
        assert_eq!(refreshed.base_sha, "new-base");
        assert_eq!(refreshed.envelope.pull_request.base_sha, "new-base");

        database
            .claim_due_action("github.com", 11, 4, at(130))
            .unwrap()
            .unwrap();
        database.complete_action(old_id, at(131)).unwrap();
        let mut later_base = rebased;
        later_base.envelope.pull_request.base_sha = "even-newer-base".into();
        let succeeded = database.enqueue_action(&later_base, at(140)).unwrap();
        assert!(!succeeded.inserted);
        let preserved = database.get_action(old_id).unwrap().unwrap();
        assert_eq!(preserved.status, ActionStatus::Succeeded);
        assert_eq!(preserved.base_sha, "new-base");
        assert_eq!(preserved.envelope.pull_request.base_sha, "new-base");
    }

    #[test]
    fn claims_enforce_global_and_per_repository_concurrency() {
        let (_directory, mut database) = database();
        for action in [
            new_action(7, 70, 7, "a", "request-a", at(100)),
            new_action(7, 71, 8, "b", "request-b", at(101)),
            new_action(8, 80, 9, "c", "request-c", at(102)),
        ] {
            database.enqueue_action(&action, at(90)).unwrap();
        }

        let first = database
            .claim_due_action("github.com", 11, 2, at(110))
            .unwrap()
            .unwrap();
        let second = database
            .claim_due_action("github.com", 11, 2, at(110))
            .unwrap()
            .unwrap();
        assert_eq!(first.repository_id, 7);
        assert_eq!(second.repository_id, 8);
        assert_eq!(first.attempts, 1);
        assert!(
            database
                .claim_due_action("github.com", 11, 2, at(110))
                .unwrap()
                .is_none()
        );
        database.complete_action(first.id, at(120)).unwrap();
        let third = database
            .claim_due_action("github.com", 11, 2, at(121))
            .unwrap()
            .unwrap();
        assert_eq!(third.repository_id, 7);
        assert_ne!(third.pull_request_id, first.pull_request_id);
    }

    #[test]
    fn repository_concurrency_is_enforced_across_viewers() {
        let (_directory, mut database) = database();
        let first = new_action(7, 70, 7, "a", "request-a", at(100));
        database.enqueue_action(&first, at(90)).unwrap();
        database
            .claim_due_action("github.com", 11, 4, at(100))
            .unwrap()
            .unwrap();

        let mut same_repository = new_action(7, 71, 8, "b", "request-b", at(100));
        same_repository.viewer_id = 12;
        same_repository.envelope.viewer = Viewer {
            id: 12,
            login: "another-reviewer".into(),
        };
        database.enqueue_action(&same_repository, at(90)).unwrap();
        let mut another_repository = new_action(8, 80, 9, "c", "request-c", at(100));
        another_repository.viewer_id = 12;
        another_repository.envelope.viewer = Viewer {
            id: 12,
            login: "another-reviewer".into(),
        };
        database
            .enqueue_action(&another_repository, at(90))
            .unwrap();

        let claimed = database
            .claim_due_action("github.com", 12, 4, at(100))
            .unwrap()
            .unwrap();
        assert_eq!(claimed.repository_id, 8);
    }

    #[test]
    fn cancel_action_handles_undispatched_running_and_parked_work() {
        let (_directory, mut database) = database();
        let mut ids = Vec::new();
        for (index, status) in [
            ActionStatus::Pending,
            ActionStatus::Running,
            ActionStatus::Parked,
        ]
        .into_iter()
        .enumerate()
        {
            let action = new_action(
                7 + index as i64,
                70 + index as i64,
                7 + index as u64,
                "head",
                &format!("request-{index}"),
                at(100),
            );
            let id = database.enqueue_action(&action, at(90)).unwrap().action_id;
            database
                .connection
                .execute(
                    "UPDATE review_actions SET status = ?2 WHERE id = ?1",
                    params![id, status.as_str()],
                )
                .unwrap();
            ids.push(id);
        }

        for id in &ids {
            assert!(
                database
                    .cancel_action(*id, "repository was unregistered", at(110))
                    .unwrap()
            );
            let action = database.get_action(*id).unwrap().unwrap();
            assert_eq!(action.status, ActionStatus::Cancelled);
            assert_eq!(
                action.last_error.as_deref(),
                Some("repository was unregistered")
            );
        }
        assert!(!database.cancel_action(ids[0], "again", at(120)).unwrap());
    }

    #[test]
    fn third_command_failure_parks_action_and_manual_retry_resets_it() {
        let (_directory, mut database) = database();
        let action = new_action(7, 70, 7, "head", "request-1", at(100));
        let action_id = database.enqueue_action(&action, at(90)).unwrap().action_id;
        for attempt in 1..=3 {
            let claimed = database
                .claim_due_action("github.com", 11, 4, at(100 + attempt))
                .unwrap()
                .unwrap();
            assert_eq!(claimed.attempts, attempt as u32);
            let status = database
                .fail_action(
                    action_id,
                    "review command failed",
                    at(101 + attempt),
                    at(100 + attempt),
                )
                .unwrap()
                .unwrap();
            assert_eq!(
                status,
                if attempt == 3 {
                    ActionStatus::Parked
                } else {
                    ActionStatus::Pending
                }
            );
        }
        assert!(
            database
                .claim_due_action("github.com", 11, 4, at(200))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            database
                .retry_newest_parked("github.com", 11, "acme/repo-7", 7, at(201))
                .unwrap(),
            Some(action_id)
        );
        let retried = database
            .claim_due_action("github.com", 11, 4, at(201))
            .unwrap()
            .unwrap();
        assert_eq!(retried.attempts, 1);
    }

    #[test]
    fn infrastructure_failure_defers_without_consuming_command_attempt() {
        let (_directory, mut database) = database();
        let action = new_action(7, 70, 7, "head", "request-1", at(100));
        let action_id = database.enqueue_action(&action, at(90)).unwrap().action_id;

        let claimed = database
            .claim_due_action("github.com", 11, 4, at(100))
            .unwrap()
            .unwrap();
        assert_eq!(claimed.attempts, 1);
        assert!(
            database
                .defer_action(action_id, "git fetch failed", at(120), at(101))
                .unwrap()
        );
        let deferred = database.get_action(action_id).unwrap().unwrap();
        assert_eq!(deferred.status, ActionStatus::Pending);
        assert_eq!(deferred.attempts, 0);
        assert_eq!(deferred.due_at, at(120));
        assert_eq!(deferred.last_error.as_deref(), Some("git fetch failed"));

        let claimed_again = database
            .claim_due_action("github.com", 11, 4, at(120))
            .unwrap()
            .unwrap();
        assert_eq!(claimed_again.attempts, 1);
    }

    #[test]
    fn status_and_pruning_keep_active_and_parked_work() {
        let (_directory, mut database) = database();
        let old = at(1_000);
        let now = old + TimeDelta::days(100);
        let succeeded = new_action(7, 70, 7, "done", "request-done", old);
        let succeeded_id = database.enqueue_action(&succeeded, old).unwrap().action_id;
        let claimed = database
            .claim_due_action("github.com", 11, 4, old)
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, succeeded_id);
        database.complete_action(succeeded_id, old).unwrap();

        let parked = new_action(8, 80, 8, "parked", "request-parked", old);
        let parked_id = database.enqueue_action(&parked, old).unwrap().action_id;
        for attempt in 0..3 {
            database
                .claim_due_action("github.com", 11, 4, old + TimeDelta::seconds(attempt))
                .unwrap()
                .unwrap();
            database
                .fail_action(
                    parked_id,
                    "failed",
                    old + TimeDelta::seconds(attempt + 1),
                    old + TimeDelta::seconds(attempt),
                )
                .unwrap();
        }

        let prune = database.prune_90_days(now).unwrap();
        assert_eq!(prune.actions, 1);
        let snapshot = database.status_snapshot().unwrap();
        assert_eq!(snapshot.action_counts.parked, 1);
        let ids: BTreeSet<_> = snapshot
            .pending_and_failed_actions
            .iter()
            .map(|action| action.id)
            .collect();
        assert_eq!(ids, BTreeSet::from([parked_id]));
    }
}
