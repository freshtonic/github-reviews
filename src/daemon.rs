use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{TimeDelta, Utc};
use rand::Rng;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::cli::{Cli, Command, Mode, PullRequestSelector, RunArgs};
use crate::db::{
    ActionStatus, Database, NewReviewAction, RegisteredRepositoryRecord, ReviewAction,
    TrackedPullRequest,
};
use crate::git::{FetchSpec, GitError, GitTools};
use crate::github::{GhClient, GhError, NotificationPoll, parse_pull_request_api_url};
use crate::model::{
    ActionReason, Actionability, EnvelopePullRequest, EnvelopeRepository, EnvelopeRequest,
    Notification, RegisteredRepository, ReviewEnvelope, ReviewRequestKind, ReviewState, Viewer,
    classify_actionability,
};
use crate::runner::{ReviewCommand, RunOutcome};

const HOST: &str = "github.com";
const LEASE_DURATION: TimeDelta = TimeDelta::seconds(90);
const LEASE_HEARTBEAT: Duration = Duration::from_secs(30);
const DISCOVERY_BATCH_LIMIT: usize = 100;
const DISCOVERY_PROGRESS_INTERVAL: usize = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
struct InProgressReview {
    repository: String,
    pull: u64,
    title: String,
}

#[derive(Default)]
struct ShutdownState {
    ctrl_c_received: bool,
    reviews: HashMap<i64, InProgressReview>,
}

impl ShutdownState {
    fn track(&mut self, action: &ReviewAction) {
        self.reviews.insert(
            action.id,
            InProgressReview {
                repository: action.repository_full_name.clone(),
                pull: action.pull_request_number,
                title: action.envelope.pull_request.title.clone(),
            },
        );
    }

    fn review_list(&self) -> String {
        let mut reviews: Vec<_> = self
            .reviews
            .values()
            .map(|review| format!("{}#{} {:?}", review.repository, review.pull, review.title))
            .collect();
        reviews.sort();
        reviews.join(", ")
    }

    fn waiting_message(&self) -> String {
        let waiting = self.reviews.len();
        if waiting == 0 {
            "waiting for 0 in-progress reviews to complete".into()
        } else {
            format!(
                "waiting for {waiting} in-progress reviews to complete: {}",
                self.review_list()
            )
        }
    }
}

struct ReviewCompletionGuard {
    action_id: i64,
    shutdown: Arc<Mutex<ShutdownState>>,
}

impl Drop for ReviewCompletionGuard {
    fn drop(&mut self) {
        let mut shutdown = lock_shutdown(&self.shutdown);
        shutdown.reviews.remove(&self.action_id);
        if shutdown.ctrl_c_received {
            let remaining = shutdown.reviews.len();
            info!("review completed; {remaining} in-progress reviews remaining");
            if remaining == 0 {
                info!("all reviews completed, quitting");
            }
        }
    }
}

fn lock_shutdown(shutdown: &Mutex<ShutdownState>) -> MutexGuard<'_, ShutdownState> {
    shutdown
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn execute(cli: Cli) -> Result<()> {
    let path = state_path()?;
    match cli.command {
        Command::Register => register(&path),
        Command::Unregister => unregister(&path),
        Command::Run(args) => run(&path, args),
        Command::Status => status(&path),
        Command::Retry { selector } => retry(&path, selector.parse()?),
    }
}

fn state_path() -> Result<PathBuf> {
    match env::var_os("GITHUB_REVIEWS_STATE_PATH") {
        Some(path) if !path.is_empty() => Ok(PathBuf::from(path)),
        _ => Database::default_path(),
    }
}

fn register(path: &Path) -> Result<()> {
    let current_dir = env::current_dir().context("resolve current working directory")?;
    let local = GitTools::default()
        .discover(&current_dir)
        .with_context(|| format!("discover current repository from {}", current_dir.display()))?;
    let repository = GhClient::new()
        .resolve_repository(&local.origin.full_name())
        .context("resolve origin through GitHub")?;
    let registration = RegisteredRepository {
        repository,
        origin_url: local.origin_url,
        local_path: local.worktree_root.canonicalize()?,
        bootstrap_pending: true,
    };
    Database::open(path)?.upsert_registration(HOST, &registration, Utc::now())?;
    println!(
        "registered {} at {}",
        registration.repository.full_name,
        registration.local_path.display()
    );
    Ok(())
}

fn unregister(path: &Path) -> Result<()> {
    let current_dir = env::current_dir().context("resolve current working directory")?;
    let local = GitTools::default()
        .discover(&current_dir)
        .with_context(|| format!("discover current repository from {}", current_dir.display()))?;
    let repository = GhClient::new()
        .resolve_repository(&local.origin.full_name())
        .context("resolve origin through GitHub")?;
    let mut db = Database::open(path)?;
    if db.remove_registration(HOST, repository.id)? {
        println!("unregistered {}", repository.full_name);
    } else {
        println!("{} was not registered", repository.full_name);
    }
    Ok(())
}

fn retry(path: &Path, selector: PullRequestSelector) -> Result<()> {
    let viewer = GhClient::new()
        .viewer()
        .context("resolve authenticated GitHub user")?;
    let mut db = Database::open(path)?;
    match db.retry_newest_parked(
        HOST,
        viewer.id,
        &selector.full_name,
        selector.number,
        Utc::now(),
    )? {
        Some(id) => println!(
            "queued action {id} for {}#{}",
            selector.full_name, selector.number
        ),
        None => bail!(
            "no parked action found for {}#{}",
            selector.full_name,
            selector.number
        ),
    }
    Ok(())
}

fn status(path: &Path) -> Result<()> {
    let db = Database::open(path)?;
    let snapshot = db.status_snapshot()?;
    println!("state: {}", db.path().display());
    println!("registered repositories: {}", snapshot.repositories.len());
    for record in snapshot.repositories {
        println!(
            "  {} -> {}",
            record.repository.repository.full_name,
            record.repository.local_path.display()
        );
    }
    for lease in snapshot.leases {
        println!(
            "daemon: {}@{} lease expires {}",
            lease.viewer_login, lease.host, lease.expires_at
        );
    }
    for poll in snapshot.poll_states {
        println!(
            "last poll: {} (viewer {}, validator {})",
            poll.updated_at,
            poll.viewer_id,
            poll.last_modified.as_deref().unwrap_or("none")
        );
    }
    let counts = snapshot.action_counts;
    println!(
        "actions: {} pending, {} running, {} parked, {} succeeded, {} cancelled",
        counts.pending, counts.running, counts.parked, counts.succeeded, counts.cancelled
    );
    for action in snapshot.pending_and_failed_actions {
        println!(
            "  #{} {}#{} {} (attempts: {}{})",
            action.id,
            action.repository_full_name,
            action.pull_request_number,
            action.status.as_str(),
            action.attempts,
            action
                .last_error
                .as_deref()
                .map(|error| format!(", error: {error}"))
                .unwrap_or_default()
        );
    }
    Ok(())
}

fn run(path: &Path, args: RunArgs) -> Result<()> {
    let gh = GhClient::new();
    let viewer = gh.viewer().context("resolve authenticated GitHub user")?;
    let (viewer_team_ids, trust_notification_for_teams) = match gh.viewer_team_ids() {
        Ok(teams) => (teams, false),
        Err(error) => {
            warn!(%error, "could not enumerate viewer teams; trusting review-request notifications");
            (Vec::new(), true)
        }
    };
    let review_command = ReviewCommand::new(args.review_command.clone())?;
    let lease_owner = format!("{}:{}", process::id(), Uuid::new_v4());
    let now = Utc::now();
    let mut db = Database::open(path)?;
    if !db.acquire_lease(
        HOST,
        viewer.id,
        &viewer.login,
        &lease_owner,
        now,
        LEASE_DURATION,
    )? {
        bail!(
            "another github-reviews daemon is already running for {}",
            viewer.login
        );
    }
    db.recover_running_actions(HOST, viewer.id, now, now)?;
    db.recover_running_discoveries(HOST, viewer.id, now, now)?;
    db.prune_90_days(now)?;
    drop(db);

    let heartbeat_stop = Arc::new(AtomicBool::new(false));
    let lease_lost = Arc::new(AtomicBool::new(false));
    let forced_abort = Arc::new(AtomicBool::new(false));
    let signals = Arc::new(AtomicUsize::new(0));
    let signal_counter = Arc::clone(&signals);
    let shutdown = Arc::new(Mutex::new(ShutdownState::default()));
    let signal_shutdown = Arc::clone(&shutdown);
    let signal_cancellation = Arc::clone(&lease_lost);
    let signal_forced_abort = Arc::clone(&forced_abort);
    ctrlc::set_handler(move || {
        if signal_counter.fetch_add(1, Ordering::SeqCst) > 0 {
            info!(
                "CTRL-C received again; immediately aborting and killing all in-progress reviews"
            );
            signal_forced_abort.store(true, Ordering::SeqCst);
            signal_cancellation.store(true, Ordering::SeqCst);
            return;
        }
        let mut shutdown = lock_shutdown(&signal_shutdown);
        shutdown.ctrl_c_received = true;
        info!("CTRL-C received; immediately stopping acceptance of new review requests");
        info!("press CTRL-C again to immediately abort and kill all in-progress reviews");
        let waiting_message = shutdown.waiting_message();
        info!("{waiting_message}");
        if shutdown.reviews.is_empty() {
            info!("all reviews completed, quitting");
        }
    })?;
    let heartbeat = start_lease_heartbeat(
        path.to_path_buf(),
        viewer.clone(),
        lease_owner.clone(),
        Arc::clone(&heartbeat_stop),
        Arc::clone(&lease_lost),
    );
    let daemon_gh = gh.with_cancellation(Arc::clone(&lease_lost));

    info!(viewer = %viewer.login, "daemon started");
    let result = run_loop(
        path,
        &args,
        &daemon_gh,
        &viewer,
        &viewer_team_ids,
        trust_notification_for_teams,
        &review_command,
        &signals,
        &lease_lost,
        &shutdown,
        &forced_abort,
    );
    heartbeat_stop.store(true, Ordering::SeqCst);
    let _ = heartbeat.join();
    Database::open(path)?.release_lease(HOST, viewer.id, &lease_owner)?;
    result
}

#[allow(clippy::too_many_arguments)]
fn run_loop(
    path: &Path,
    args: &RunArgs,
    gh: &GhClient,
    viewer: &Viewer,
    viewer_team_ids: &[i64],
    trust_notification_for_teams: bool,
    review_command: &ReviewCommand,
    signals: &AtomicUsize,
    lease_lost: &Arc<AtomicBool>,
    shutdown: &Arc<Mutex<ShutdownState>>,
    forced_abort: &AtomicBool,
) -> Result<()> {
    let git = GitTools::default();
    let mut workers: Vec<JoinHandle<()>> = Vec::new();

    while signals.load(Ordering::SeqCst) == 0 && !lease_lost.load(Ordering::SeqCst) {
        reap_workers(&mut workers);
        let cycle_started = Instant::now();
        let now = Utc::now();
        let mut db = Database::open(path)?;
        let registrations = db.list_registrations()?;
        if registrations.is_empty() {
            drop(db);
            sleep_interruptibly(Duration::from_secs(1), signals, lease_lost);
            continue;
        }
        let by_id: HashMap<i64, RegisteredRepositoryRecord> = registrations
            .iter()
            .cloned()
            .map(|record| (record.repository.repository.id, record))
            .collect();

        let mut bootstrap_rate_delay = None;
        for registration in &registrations {
            if lease_lost.load(Ordering::SeqCst) {
                break;
            }
            if db.bootstrap_pending(HOST, viewer.id, registration.repository.repository.id)?
                != Some(true)
            {
                continue;
            }
            match gh.bootstrap_notifications(&registration.repository.repository.full_name) {
                Ok(poll) => {
                    let values = notification_values(&poll.notifications)?;
                    db.commit_bootstrap(
                        HOST,
                        viewer.id,
                        registration.repository.repository.id,
                        &values,
                        now,
                    )?;
                    info!(repository = %registration.repository.repository.full_name, "repository bootstrap complete");
                }
                Err(error) => {
                    warn!(repository = %registration.repository.repository.full_name, %error, "repository bootstrap failed");
                    if is_rate_limited(&error) {
                        bootstrap_rate_delay =
                            Some(retry_delay_for_gh_error(&error).unwrap_or(args.interval));
                        break;
                    }
                }
            }
        }

        let poll_state = db.poll_state(HOST, viewer.id)?;
        let high_water = poll_state
            .as_ref()
            .and_then(|state| state.high_water.as_deref())
            .and_then(|value| value.parse().ok());
        let (server_interval, mut api_available) = if bootstrap_rate_delay.is_some() {
            (bootstrap_rate_delay, false)
        } else {
            match gh.poll_global_notifications(
                poll_state
                    .as_ref()
                    .and_then(|state| state.last_modified.as_deref()),
                high_water,
            ) {
                Ok(poll) => {
                    let server_interval = poll.metadata.poll_interval;
                    if !poll.not_modified {
                        commit_notification_poll(
                            &mut db,
                            viewer,
                            &poll,
                            poll_state
                                .as_ref()
                                .and_then(|state| state.last_modified.as_deref()),
                            poll_state
                                .as_ref()
                                .and_then(|state| state.high_water.as_deref()),
                            now,
                        )?;
                        (server_interval, true)
                    } else {
                        db.set_poll_state(
                            HOST,
                            viewer.id,
                            poll.metadata.last_modified.as_deref().or_else(|| {
                                poll_state
                                    .as_ref()
                                    .and_then(|state| state.last_modified.as_deref())
                            }),
                            poll_state
                                .as_ref()
                                .and_then(|state| state.high_water.as_deref()),
                            now,
                        )?;
                        (server_interval, true)
                    }
                }
                Err(error) => {
                    let retry_delay = retry_delay_for_gh_error(&error);
                    warn!(%error, "notification poll failed");
                    (retry_delay, false)
                }
            }
        };

        if api_available && !lease_lost.load(Ordering::SeqCst) {
            api_available = process_discoveries(
                &mut db,
                gh,
                viewer,
                viewer_team_ids,
                trust_notification_for_teams,
                &by_id,
            )?;
        }
        if api_available && !lease_lost.load(Ordering::SeqCst) {
            api_available = refresh_tracked(
                &mut db,
                gh,
                viewer,
                viewer_team_ids,
                trust_notification_for_teams,
                &by_id,
                args.interval,
            )?;
        }

        if api_available && !lease_lost.load(Ordering::SeqCst) {
            match args.mode {
                Mode::Sync => {
                    if let Some(action) =
                        claim_review_for_processing(&mut db, viewer.id, 1, signals, shutdown)?
                    {
                        drop(db);
                        let _completion = ReviewCompletionGuard {
                            action_id: action.id,
                            shutdown: Arc::clone(shutdown),
                        };
                        process_action(
                            path,
                            &git,
                            gh,
                            viewer,
                            viewer_team_ids,
                            trust_notification_for_teams,
                            lease_lost,
                            review_command,
                            args.review_timeout,
                            action,
                        );
                    }
                }
                Mode::Async => {
                    while workers.len() < args.max_concurrency {
                        let Some(action) = claim_review_for_processing(
                            &mut db,
                            viewer.id,
                            args.max_concurrency,
                            signals,
                            shutdown,
                        )?
                        else {
                            break;
                        };
                        let state_path = path.to_path_buf();
                        let command = review_command.clone();
                        let git = git.clone();
                        let gh = gh.clone();
                        let viewer = viewer.clone();
                        let viewer_team_ids = viewer_team_ids.to_vec();
                        let lease_lost = Arc::clone(lease_lost);
                        let shutdown = Arc::clone(shutdown);
                        let timeout = args.review_timeout;
                        workers.push(thread::spawn(move || {
                            let _completion = ReviewCompletionGuard {
                                action_id: action.id,
                                shutdown,
                            };
                            process_action(
                                &state_path,
                                &git,
                                &gh,
                                &viewer,
                                &viewer_team_ids,
                                trust_notification_for_teams,
                                &lease_lost,
                                &command,
                                timeout,
                                action,
                            );
                        }));
                    }
                }
            }
        }

        let delay = server_interval.map_or(args.interval, |server| args.interval.max(server));
        sleep_interruptibly(
            delay.saturating_sub(cycle_started.elapsed()),
            signals,
            lease_lost,
        );
    }

    for worker in workers {
        let _ = worker.join();
    }
    if lease_lost.load(Ordering::SeqCst) && !forced_abort.load(Ordering::SeqCst) {
        bail!("daemon lease was lost; stopped before another daemon could take over");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn process_discoveries(
    db: &mut Database,
    gh: &GhClient,
    viewer: &Viewer,
    viewer_team_ids: &[i64],
    trust_notification_for_teams: bool,
    registrations: &HashMap<i64, RegisteredRepositoryRecord>,
) -> Result<bool> {
    let started = Instant::now();
    let pending = db.due_discovery_count(HOST, viewer.id, Utc::now())?;
    if pending == 0 {
        return Ok(true);
    }
    info!(
        pending,
        batch_limit = DISCOVERY_BATCH_LIMIT,
        "processing pull request discovery backlog; new repository registrations load next polling cycle"
    );
    let mut processed = 0;
    let mut api_available = true;
    let mut reported_registrations = HashSet::new();
    for _ in 0..DISCOVERY_BATCH_LIMIT {
        if processed > 0 && processed % DISCOVERY_PROGRESS_INTERVAL == 0 {
            let remaining = db.due_discovery_count(HOST, viewer.id, Utc::now())?;
            info!(
                processed,
                remaining,
                elapsed_seconds = started.elapsed().as_secs(),
                "pull request discovery backlog progress"
            );
            log_registrations_waiting_for_next_cycle(
                db,
                registrations,
                &mut reported_registrations,
            )?;
        }
        let now = Utc::now();
        let Some(discovery) = db.claim_pending_discovery(HOST, viewer.id, now)? else {
            break;
        };
        processed += 1;
        let notification = &discovery.notification;
        let Some(registration) = registrations.get(&discovery.repository_id) else {
            db.complete_discovery(HOST, viewer.id, &discovery.notification_id, now)?;
            continue;
        };
        let Some(url) = notification.subject.url.as_deref() else {
            warn!(notification = %notification.id, "review notification has no pull request URL");
            db.complete_discovery(HOST, viewer.id, &discovery.notification_id, now)?;
            continue;
        };
        let (_, number) = match parse_pull_request_api_url(url) {
            Ok(identity) => identity,
            Err(error) => {
                warn!(notification = %notification.id, %error, "ignoring malformed pull request notification");
                db.complete_discovery(HOST, viewer.id, &discovery.notification_id, now)?;
                continue;
            }
        };
        let full_name = &registration.repository.repository.full_name;
        let pull = match gh.pull_request_state(
            full_name,
            number,
            viewer,
            viewer_team_ids,
            trust_notification_for_teams,
        ) {
            Ok(pull) => pull,
            Err(error)
                if error
                    .metadata()
                    .is_some_and(|metadata| metadata.status == 404) =>
            {
                db.complete_discovery(HOST, viewer.id, &discovery.notification_id, now)?;
                continue;
            }
            Err(error) => {
                let delay = retry_delay_for_gh_error(&error).unwrap_or_else(|| {
                    let exponent = discovery.attempts.saturating_sub(1).min(5);
                    Duration::from_secs(30_u64.saturating_mul(1_u64 << exponent))
                });
                warn!(notification = %notification.id, %error, "pull request discovery deferred");
                db.defer_discovery(
                    HOST,
                    viewer.id,
                    &discovery.notification_id,
                    &error.to_string(),
                    now + chrono_duration(delay),
                    now,
                )?;
                if is_rate_limited(&error) {
                    api_available = false;
                    break;
                }
                continue;
            }
        };
        // The notification is historical evidence that the viewer or one of
        // their teams was requested. GitHub removes reviewers from the active
        // request set after any submitted review, so requiring current request
        // membership here would lose CHANGES_REQUESTED follow-up tracking.
        if pull.author.id == viewer.id {
            db.complete_discovery(HOST, viewer.id, &discovery.notification_id, now)?;
            continue;
        }
        db.upsert_tracked_pull_request(&TrackedPullRequest {
            host: HOST.into(),
            viewer_id: viewer.id,
            repository_id: discovery.repository_id,
            repository_full_name: full_name.clone(),
            pull_request: pull,
            notification_id: discovery.notification_id.clone(),
            notification_updated_at: discovery.notification_updated_at,
            notification: discovery.raw,
            active: true,
            next_check_at: now,
            updated_at: now,
        })?;
        db.complete_discovery(HOST, viewer.id, &discovery.notification_id, now)?;
    }
    let remaining = db.due_discovery_count(HOST, viewer.id, Utc::now())?;
    info!(
        processed,
        remaining,
        elapsed_seconds = started.elapsed().as_secs(),
        "pull request discovery batch complete; repository registrations will reload next polling cycle"
    );
    log_registrations_waiting_for_next_cycle(db, registrations, &mut reported_registrations)?;
    Ok(api_available)
}

fn log_registrations_waiting_for_next_cycle(
    db: &Database,
    registrations: &HashMap<i64, RegisteredRepositoryRecord>,
    reported: &mut HashSet<String>,
) -> Result<()> {
    let mut waiting: Vec<_> = db
        .list_registrations()?
        .into_iter()
        .filter(|registration| !registrations.contains_key(&registration.repository.repository.id))
        .map(|registration| registration.repository.repository.full_name)
        .filter(|full_name| reported.insert(full_name.clone()))
        .collect();
    waiting.sort();
    if !waiting.is_empty() {
        warn!(
            repositories = %waiting.join(", "),
            "new repository registration detected during pull request discovery; bootstrap delayed until next polling cycle"
        );
    }
    Ok(())
}

fn notification_values(
    notifications: &[Notification],
) -> Result<Vec<(Notification, serde_json::Value)>> {
    notifications
        .iter()
        .cloned()
        .map(|notification| {
            let raw = serde_json::to_value(&notification)?;
            Ok((notification, raw))
        })
        .collect()
}

fn commit_notification_poll(
    db: &mut Database,
    viewer: &Viewer,
    poll: &NotificationPoll,
    previous_last_modified: Option<&str>,
    previous_high_water: Option<&str>,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    let values = notification_values(&poll.notifications)?;
    let observed_high_water = poll
        .notifications
        .iter()
        .map(|notification| notification.updated_at)
        .max();
    let previous_high_water = previous_high_water.and_then(|value| value.parse().ok());
    let high_water = observed_high_water
        .into_iter()
        .chain(previous_high_water)
        .max()
        .map(|time| time.to_rfc3339());
    db.commit_poll(
        HOST,
        viewer.id,
        &values,
        poll.metadata
            .last_modified
            .as_deref()
            .or(previous_last_modified),
        high_water.as_deref(),
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn refresh_tracked(
    db: &mut Database,
    gh: &GhClient,
    viewer: &Viewer,
    viewer_team_ids: &[i64],
    trust_notification_for_teams: bool,
    registrations: &HashMap<i64, RegisteredRepositoryRecord>,
    interval: Duration,
) -> Result<bool> {
    let now = Utc::now();
    for mut tracked in db.due_tracked_pull_requests(HOST, viewer.id, now, 1000)? {
        let Some(registration) = registrations.get(&tracked.repository_id) else {
            db.deactivate_tracked_pull_request(
                HOST,
                viewer.id,
                tracked.repository_id,
                tracked.pull_request.id,
                now,
            )?;
            continue;
        };
        let canonical_name = &registration.repository.repository.full_name;
        // CHANGES_REQUESTED follow-up tracking needs only the PR's core state
        // and head/base revisions on frequent polls. Reuse the cached review
        // authority here; discovery and dispatch still perform full reads.
        let authoritative_refresh_due = tracked.updated_at.timestamp().div_euclid(15 * 60)
            != now.timestamp().div_euclid(15 * 60);
        let refresh = if !authoritative_refresh_due
            && tracked
                .pull_request
                .latest_review
                .as_ref()
                .is_some_and(|review| review.state == ReviewState::ChangesRequested)
        {
            gh.refresh_pull_request_core(canonical_name, &tracked.pull_request)
        } else {
            gh.pull_request_state(
                canonical_name,
                tracked.pull_request.number,
                viewer,
                viewer_team_ids,
                trust_notification_for_teams,
            )
        };
        let pull = match refresh {
            Ok(pull) => pull,
            Err(error)
                if error
                    .metadata()
                    .is_some_and(|metadata| metadata.status == 404) =>
            {
                db.deactivate_tracked_pull_request(
                    HOST,
                    viewer.id,
                    tracked.repository_id,
                    tracked.pull_request.id,
                    now,
                )?;
                continue;
            }
            Err(error) => {
                warn!(repository = %tracked.repository_full_name, pull = tracked.pull_request.number, %error, "could not refresh pull request");
                if is_rate_limited(&error) {
                    return Ok(false);
                }
                continue;
            }
        };
        let effective_teams = if trust_notification_for_teams {
            pull.requested_team_ids.as_slice()
        } else {
            viewer_team_ids
        };
        let actionability = classify_actionability(viewer, effective_teams, &pull);
        tracked.repository_full_name = canonical_name.clone();
        tracked.pull_request = pull.clone();
        tracked.active = match &actionability {
            Actionability::Inactive(_) => false,
            Actionability::Dormant(_) => pull
                .latest_review
                .as_ref()
                .is_some_and(|review| review.state == ReviewState::ChangesRequested),
            Actionability::Actionable(_) => true,
        };
        tracked.next_check_at = now + chrono_duration(interval);
        tracked.updated_at = now;
        db.upsert_tracked_pull_request(&tracked)?;

        let Actionability::Actionable(reason) = actionability else {
            continue;
        };
        let fallback_event_id = format!(
            "{}@{}",
            tracked.notification_id,
            tracked.notification_updated_at.timestamp_millis()
        );
        let (request_kind, event_id) = pull.latest_request.as_ref().map_or_else(
            || {
                (
                    if pull.directly_requested {
                        "user"
                    } else {
                        "team"
                    }
                    .to_owned(),
                    fallback_event_id,
                )
            },
            |request| {
                (
                    match request.kind {
                        ReviewRequestKind::User => "user",
                        ReviewRequestKind::Team => "team",
                    }
                    .to_owned(),
                    request.event_id.clone(),
                )
            },
        );
        let envelope = ReviewEnvelope {
            schema_version: 1,
            reason,
            viewer: viewer.clone(),
            repository: EnvelopeRepository {
                id: tracked.repository_id,
                full_name: canonical_name.clone(),
                local_path: registration.repository.local_path.clone(),
            },
            request: EnvelopeRequest {
                kind: request_kind,
                event_id: event_id.clone(),
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
            review: pull.latest_review.clone(),
            notification: tracked.notification.clone(),
        };
        let trigger_key = match reason {
            ActionReason::HeadChangedAfterChangesRequested => format!(
                "review:{}",
                pull.latest_review.as_ref().map_or(0, |review| review.id)
            ),
            _ => event_id,
        };
        let result = db.enqueue_action(
            &NewReviewAction {
                host: HOST.into(),
                viewer_id: viewer.id,
                repository_id: tracked.repository_id,
                repository_full_name: canonical_name.clone(),
                pull_request_id: pull.id,
                pull_request_number: pull.number,
                head_sha: pull.head_sha.clone(),
                trigger_key,
                reason: reason.as_str().into(),
                envelope,
                due_at: now,
            },
            now,
        )?;
        if result.inserted {
            info!(repository = %tracked.repository_full_name, pull = pull.number, action = result.action_id, reason = reason.as_str(), "review action queued");
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn process_action(
    state_path: &Path,
    git: &GitTools,
    gh: &GhClient,
    viewer: &Viewer,
    viewer_team_ids: &[i64],
    trust_notification_for_teams: bool,
    cancelled: &AtomicBool,
    command: &ReviewCommand,
    timeout: Option<Duration>,
    action: ReviewAction,
) {
    if let Err(error) = process_action_inner(
        state_path,
        git,
        gh,
        viewer,
        viewer_team_ids,
        trust_notification_for_teams,
        cancelled,
        command,
        timeout,
        &action,
    ) {
        error!(action = action.id, %error, "review action processing failed");
        if let Ok(mut db) = Database::open(state_path) {
            let _ = db.defer_action(
                action.id,
                &error.to_string(),
                Utc::now() + TimeDelta::minutes(1),
                Utc::now(),
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn process_action_inner(
    state_path: &Path,
    git: &GitTools,
    gh: &GhClient,
    viewer: &Viewer,
    viewer_team_ids: &[i64],
    trust_notification_for_teams: bool,
    cancelled: &AtomicBool,
    command: &ReviewCommand,
    timeout: Option<Duration>,
    action: &ReviewAction,
) -> Result<()> {
    let mut db = Database::open(state_path)?;
    let Some(registration) = db.get_registration(HOST, action.repository_id)? else {
        db.cancel_action(action.id, "repository is no longer registered", Utc::now())?;
        return Ok(());
    };
    let canonical_name = &registration.repository.repository.full_name;
    let mut current_pull = match gh.pull_request_state(
        canonical_name,
        action.pull_request_number,
        viewer,
        viewer_team_ids,
        trust_notification_for_teams,
    ) {
        Ok(pull) => pull,
        Err(error)
            if error
                .metadata()
                .is_some_and(|metadata| metadata.status == 404) =>
        {
            db.cancel_action(action.id, "pull request no longer exists", Utc::now())?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let effective_teams = if trust_notification_for_teams {
        current_pull.requested_team_ids.as_slice()
    } else {
        viewer_team_ids
    };
    let current_reason = match classify_actionability(viewer, effective_teams, &current_pull) {
        Actionability::Actionable(reason) => reason,
        Actionability::Dormant(reason) | Actionability::Inactive(reason) => {
            db.cancel_action(action.id, reason, Utc::now())?;
            return Ok(());
        }
    };
    if current_pull.id != action.pull_request_id
        || current_pull.head_sha != action.head_sha
        || current_reason != action.envelope.reason
        || (current_reason == ActionReason::HeadChangedAfterChangesRequested
            && current_pull
                .latest_review
                .as_ref()
                .is_some_and(|review| action.trigger_key != format!("review:{}", review.id)))
        || (current_reason != ActionReason::HeadChangedAfterChangesRequested
            && current_pull
                .latest_request
                .as_ref()
                .is_some_and(|request| request.event_id != action.envelope.request.event_id))
    {
        db.cancel_action(
            action.id,
            "pull request changed after this action was queued",
            Utc::now(),
        )?;
        return Ok(());
    }

    if cancelled.load(Ordering::SeqCst) {
        db.defer_action(action.id, "daemon lease was lost", Utc::now(), Utc::now())?;
        return Ok(());
    }
    let local = match git.discover_with_cancel(&registration.repository.local_path, cancelled) {
        Ok(local) => local,
        Err(error) => {
            db.defer_action(
                action.id,
                &error.to_string(),
                Utc::now() + TimeDelta::minutes(5),
                Utc::now(),
            )?;
            return Ok(());
        }
    };
    if cancelled.load(Ordering::SeqCst) {
        db.defer_action(action.id, "daemon lease was lost", Utc::now(), Utc::now())?;
        return Ok(());
    }
    let expected_path = fs::canonicalize(&registration.repository.local_path)?;
    let actual_path = fs::canonicalize(&local.worktree_root)?;
    if expected_path != actual_path {
        db.defer_action(
            action.id,
            &GitError::WorktreeMismatch {
                expected: expected_path,
                actual: actual_path,
            }
            .to_string(),
            Utc::now() + TimeDelta::minutes(5),
            Utc::now(),
        )?;
        return Ok(());
    }
    let local_repository = gh.resolve_repository(&local.origin.full_name())?;
    if cancelled.load(Ordering::SeqCst) {
        db.defer_action(action.id, "daemon lease was lost", Utc::now(), Utc::now())?;
        return Ok(());
    }
    if local_repository.id != action.repository_id {
        db.defer_action(
            action.id,
            "registered path now points at a different GitHub repository",
            Utc::now() + TimeDelta::minutes(5),
            Utc::now(),
        )?;
        return Ok(());
    }
    let fetched_base_sha = current_pull.base_sha.clone();
    let pull = &current_pull;
    if let Err(error) = git.fetch_pull_request_with_cancel(
        &local.worktree_root,
        &FetchSpec {
            pull_number: pull.number,
            base_ref: pull.base_ref.clone(),
            expected_base_oid: pull.base_sha.clone(),
            expected_head_oid: pull.head_sha.clone(),
        },
        cancelled,
    ) {
        if matches!(error, GitError::Cancelled { .. }) {
            db.defer_action(action.id, &error.to_string(), Utc::now(), Utc::now())?;
            return Ok(());
        }
        let delay = if matches!(error, GitError::StaleRevision { .. }) {
            TimeDelta::seconds(5)
        } else {
            TimeDelta::minutes(1)
        };
        db.defer_action(
            action.id,
            &error.to_string(),
            Utc::now() + delay,
            Utc::now(),
        )?;
        return Ok(());
    }

    if cancelled.load(Ordering::SeqCst) {
        db.defer_action(
            action.id,
            "daemon lease was lost while preparing the review command",
            Utc::now(),
            Utc::now(),
        )?;
        return Ok(());
    }
    current_pull = match gh.pull_request_state(
        canonical_name,
        action.pull_request_number,
        viewer,
        viewer_team_ids,
        trust_notification_for_teams,
    ) {
        Ok(pull) => pull,
        Err(error)
            if error
                .metadata()
                .is_some_and(|metadata| metadata.status == 404) =>
        {
            db.cancel_action(action.id, "pull request no longer exists", Utc::now())?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let effective_teams = if trust_notification_for_teams {
        current_pull.requested_team_ids.as_slice()
    } else {
        viewer_team_ids
    };
    let prepared_reason = match classify_actionability(viewer, effective_teams, &current_pull) {
        Actionability::Actionable(reason) => reason,
        Actionability::Dormant(reason) | Actionability::Inactive(reason) => {
            db.cancel_action(action.id, reason, Utc::now())?;
            return Ok(());
        }
    };
    if current_pull.base_sha != fetched_base_sha {
        db.defer_action(
            action.id,
            "pull request base changed while its revisions were being fetched",
            Utc::now() + TimeDelta::seconds(5),
            Utc::now(),
        )?;
        return Ok(());
    }
    if current_pull.id != action.pull_request_id
        || current_pull.head_sha != action.head_sha
        || prepared_reason != action.envelope.reason
        || (prepared_reason == ActionReason::HeadChangedAfterChangesRequested
            && current_pull
                .latest_review
                .as_ref()
                .is_some_and(|review| action.trigger_key != format!("review:{}", review.id)))
        || (prepared_reason != ActionReason::HeadChangedAfterChangesRequested
            && current_pull
                .latest_request
                .as_ref()
                .is_some_and(|request| request.event_id != action.envelope.request.event_id))
    {
        db.cancel_action(
            action.id,
            "pull request changed while its revisions were being fetched",
            Utc::now(),
        )?;
        return Ok(());
    }

    let Some(current_registration) = db.get_registration(HOST, action.repository_id)? else {
        db.cancel_action(
            action.id,
            "repository was unregistered during preparation",
            Utc::now(),
        )?;
        return Ok(());
    };
    if current_registration.repository.local_path != registration.repository.local_path {
        db.defer_action(
            action.id,
            "registered repository path changed during preparation",
            Utc::now(),
            Utc::now(),
        )?;
        return Ok(());
    }

    let mut envelope = action.envelope.clone();
    envelope.repository.full_name = canonical_name.clone();
    envelope.repository.local_path = local.worktree_root.clone();
    envelope.pull_request.url = current_pull.url.clone();
    envelope.pull_request.title = current_pull.title.clone();
    envelope.pull_request.author = current_pull.author.clone();
    envelope.pull_request.head_sha = current_pull.head_sha.clone();
    envelope.pull_request.base_sha = current_pull.base_sha.clone();
    envelope.pull_request.head_ref = current_pull.head_ref.clone();
    envelope.pull_request.base_ref = current_pull.base_ref.clone();
    envelope.review = current_pull.latest_review.clone();
    if let Some(request) = &current_pull.latest_request {
        envelope.request.kind = match request.kind {
            ReviewRequestKind::User => "user",
            ReviewRequestKind::Team => "team",
        }
        .to_owned();
        envelope.request.event_id = request.event_id.clone();
    }

    info!(
        action = action.id,
        repository = %canonical_name,
        "launching review command for (#{}) {}",
        action.pull_request_number,
        current_pull.title
    );
    match command.execute_with_cancel(&local.worktree_root, &envelope, timeout, cancelled) {
        Ok(RunOutcome::Succeeded) => {
            db.complete_action(action.id, Utc::now())?;
            info!(action = action.id, repository = %canonical_name, pull = action.pull_request_number, "review command succeeded");
        }
        Ok(RunOutcome::Failed { status }) => record_command_failure(
            &mut db,
            action,
            &format!("review command exited with {status}"),
        )?,
        Ok(RunOutcome::TimedOut) => {
            record_command_failure(&mut db, action, "review command timed out")?
        }
        Ok(RunOutcome::Cancelled) => {
            db.defer_action(
                action.id,
                "daemon lease was lost while review command was running",
                Utc::now(),
                Utc::now(),
            )?;
        }
        Err(error) => record_command_failure(&mut db, action, &error.to_string())?,
    }
    Ok(())
}

fn record_command_failure(db: &mut Database, action: &ReviewAction, error: &str) -> Result<()> {
    let exponent = action.attempts.saturating_sub(1).min(4);
    let base_seconds = 30_u64.saturating_mul(1_u64 << exponent).min(300);
    let jitter = rand::rng().random_range(0..=base_seconds / 5 + 1);
    let retry_at =
        Utc::now() + TimeDelta::seconds(i64::try_from(base_seconds + jitter).unwrap_or(300));
    match db.fail_action(action.id, error, retry_at, Utc::now())? {
        Some(ActionStatus::Parked) => {
            error!(action = action.id, %error, "review action parked after three attempts")
        }
        Some(_) => {
            warn!(action = action.id, %error, retry_at = %retry_at, "review command failed; retry scheduled")
        }
        None => warn!(action = action.id, "review action was no longer running"),
    }
    Ok(())
}

fn start_lease_heartbeat(
    path: PathBuf,
    viewer: Viewer,
    owner: String,
    stop: Arc<AtomicBool>,
    lease_lost: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut next_heartbeat = Instant::now() + LEASE_HEARTBEAT;
        while !stop.load(Ordering::SeqCst) {
            let remaining = next_heartbeat.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                thread::sleep(remaining.min(Duration::from_millis(200)));
                continue;
            }
            match Database::open(&path).and_then(|mut db| {
                db.renew_lease(HOST, viewer.id, &owner, Utc::now(), LEASE_DURATION)
            }) {
                Ok(true) => {}
                Ok(false) => {
                    error!("daemon lease could not be renewed; stopping daemon");
                    lease_lost.store(true, Ordering::SeqCst);
                }
                Err(error) => {
                    error!(%error, "daemon lease heartbeat failed; stopping daemon");
                    lease_lost.store(true, Ordering::SeqCst);
                }
            }
            next_heartbeat = Instant::now() + LEASE_HEARTBEAT;
        }
    })
}

fn retry_delay_for_gh_error(error: &GhError) -> Option<Duration> {
    let metadata = error.metadata()?;
    metadata.retry_after.or_else(|| {
        if metadata.rate_limit_remaining == Some(0) {
            metadata.rate_limit_reset.map(|reset| {
                let now = Utc::now().timestamp().max(0) as u64;
                Duration::from_secs(reset.saturating_sub(now))
            })
        } else {
            None
        }
    })
}

fn is_rate_limited(error: &GhError) -> bool {
    let headers_signal_limit = error.metadata().is_some_and(|metadata| {
        metadata.rate_limit_remaining == Some(0)
            || metadata.retry_after.is_some()
            || matches!(metadata.status, 429)
    });
    let body_signals_secondary_limit = match error {
        GhError::Status { metadata, body, .. } if metadata.status == 403 => {
            let body = body.to_ascii_lowercase();
            body.contains("secondary rate limit") || body.contains("abuse detection")
        }
        _ => false,
    };
    headers_signal_limit || body_signals_secondary_limit
}

fn chrono_duration(duration: Duration) -> TimeDelta {
    TimeDelta::from_std(duration).unwrap_or(TimeDelta::MAX)
}

fn claim_review_for_processing(
    db: &mut Database,
    viewer_id: i64,
    max_concurrency: usize,
    signals: &AtomicUsize,
    shutdown: &Arc<Mutex<ShutdownState>>,
) -> Result<Option<ReviewAction>> {
    let mut shutdown = lock_shutdown(shutdown);
    if signals.load(Ordering::SeqCst) > 0 {
        return Ok(None);
    }
    let Some(action) = db.claim_due_action(HOST, viewer_id, max_concurrency, Utc::now())? else {
        return Ok(None);
    };
    if signals.load(Ordering::SeqCst) > 0 {
        let now = Utc::now();
        db.defer_action(
            action.id,
            "daemon shutdown requested before review started",
            now,
            now,
        )?;
        return Ok(None);
    }
    shutdown.track(&action);
    Ok(Some(action))
}

fn reap_workers(workers: &mut Vec<JoinHandle<()>>) {
    let mut index = 0;
    while index < workers.len() {
        if workers[index].is_finished() {
            let worker = workers.swap_remove(index);
            if worker.join().is_err() {
                error!("review worker panicked");
            }
        } else {
            index += 1;
        }
    }
}

fn sleep_interruptibly(duration: Duration, signals: &AtomicUsize, lease_lost: &AtomicBool) {
    let deadline = Instant::now() + duration;
    while signals.load(Ordering::SeqCst) == 0 && !lease_lost.load(Ordering::SeqCst) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        thread::sleep(remaining.min(Duration::from_millis(200)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_wait_message_lists_in_progress_reviews_in_stable_order() {
        let mut shutdown = ShutdownState::default();
        shutdown.reviews.insert(
            2,
            InProgressReview {
                repository: "zeta/widgets".into(),
                pull: 9,
                title: "Second".into(),
            },
        );
        shutdown.reviews.insert(
            1,
            InProgressReview {
                repository: "acme/rockets".into(),
                pull: 42,
                title: "First".into(),
            },
        );

        assert_eq!(
            shutdown.waiting_message(),
            "waiting for 2 in-progress reviews to complete: acme/rockets#42 \"First\", zeta/widgets#9 \"Second\""
        );
    }

    #[test]
    fn shutdown_wait_message_reports_zero_reviews() {
        assert_eq!(
            ShutdownState::default().waiting_message(),
            "waiting for 0 in-progress reviews to complete"
        );
    }

    #[test]
    fn recognizes_secondary_rate_limit_from_error_body() {
        let error = GhError::Status {
            endpoint: "/test".into(),
            metadata: Box::new(crate::github::ResponseMetadata {
                status: 403,
                rate_limit_remaining: Some(100),
                ..Default::default()
            }),
            body: r#"{"message":"You have exceeded a secondary rate limit."}"#.into(),
            stderr: String::new(),
        };
        assert!(is_rate_limited(&error));
    }
}
