//! GitHub API access through the authenticated `gh` CLI.
//!
//! Keeping the process boundary in this module is deliberate: `gh` owns
//! authentication, while the rest of the application gets typed values and
//! never needs to handle credentials.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fmt;
use std::io::{self, Read};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use wait_timeout::ChildExt;

use crate::model::{
    Notification, OpinionatedReview, PullRequestState, RepositoryIdentity, ReviewRequestEvent,
    ReviewRequestKind, ReviewState, Viewer,
};

const API_HOST: &str = "github.com";
const API_URL_PREFIX: &str = "https://api.github.com";
const ACCEPT_HEADER: &str = "Accept: application/vnd.github+json";
const API_VERSION_HEADER: &str = "X-GitHub-Api-Version: 2022-11-28";
const GH_COMMAND_TIMEOUT: Duration = Duration::from_secs(45);

pub type Result<T> = std::result::Result<T, GhError>;

/// Rate-limit and polling information returned alongside an API response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResponseMetadata {
    pub status: u16,
    pub last_modified: Option<String>,
    pub poll_interval: Option<Duration>,
    pub retry_after: Option<Duration>,
    pub rate_limit_remaining: Option<u64>,
    pub rate_limit_reset: Option<u64>,
    pub next: Option<String>,
}

/// A complete notification poll. A 304 is a successful, unchanged poll.
#[derive(Debug)]
pub struct NotificationPoll {
    pub not_modified: bool,
    pub notifications: Vec<Notification>,
    pub metadata: ResponseMetadata,
}

#[derive(Debug)]
pub enum GhError {
    Spawn {
        executable: PathBuf,
        source: io::Error,
    },
    Protocol(String),
    Decode {
        endpoint: String,
        source: serde_json::Error,
        body: String,
    },
    Status {
        endpoint: String,
        metadata: Box<ResponseMetadata>,
        body: String,
        stderr: String,
    },
    Timeout {
        endpoint: String,
    },
    Cancelled {
        endpoint: String,
    },
}

impl GhError {
    /// HTTP metadata is retained on status errors so callers can honour
    /// `Retry-After` and inspect rate-limit exhaustion.
    pub fn metadata(&self) -> Option<&ResponseMetadata> {
        match self {
            Self::Status { metadata, .. } => Some(metadata.as_ref()),
            _ => None,
        }
    }
}

impl fmt::Display for GhError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawn { executable, source } => {
                write!(f, "could not run {}: {source}", executable.display())
            }
            Self::Protocol(message) => write!(f, "invalid response from gh: {message}"),
            Self::Decode {
                endpoint, source, ..
            } => {
                write!(
                    f,
                    "could not decode GitHub response from {endpoint}: {source}"
                )
            }
            Self::Status {
                endpoint,
                metadata,
                stderr,
                ..
            } => {
                write!(
                    f,
                    "GitHub API request to {endpoint} returned HTTP {}",
                    metadata.status
                )?;
                if !stderr.trim().is_empty() {
                    write!(f, ": {}", stderr.trim())?;
                }
                Ok(())
            }
            Self::Timeout { endpoint } => {
                write!(f, "gh API request {endpoint} timed out after 45 seconds")
            }
            Self::Cancelled { endpoint } => write!(f, "gh API request {endpoint} was cancelled"),
        }
    }
}

impl std::error::Error for GhError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Spawn { source, .. } => Some(source),
            Self::Decode { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// An injectable wrapper around the `gh` executable.
#[derive(Clone, Debug)]
pub struct GhClient {
    executable: OsString,
    cancelled: Option<Arc<AtomicBool>>,
}

impl Default for GhClient {
    fn default() -> Self {
        Self::new()
    }
}

impl GhClient {
    pub fn new() -> Self {
        Self::with_executable("gh")
    }

    pub fn with_executable(executable: impl Into<OsString>) -> Self {
        Self {
            executable: executable.into(),
            cancelled: None,
        }
    }

    pub fn with_cancellation(&self, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            executable: self.executable.clone(),
            cancelled: Some(cancelled),
        }
    }

    pub fn viewer(&self) -> Result<Viewer> {
        self.get_json("/user")
    }

    /// Return IDs for all teams visible to the authenticated user.
    pub fn viewer_team_ids(&self) -> Result<Vec<i64>> {
        #[derive(Deserialize)]
        struct Team {
            id: i64,
        }

        let teams: Vec<Team> = self.get_all_pages("/user/teams?per_page=100", &[])?;
        Ok(teams.into_iter().map(|team| team.id).collect())
    }

    pub fn resolve_repository(&self, full_name: &str) -> Result<RepositoryIdentity> {
        validate_full_name(full_name)?;

        #[derive(Deserialize)]
        struct RepositoryResponse {
            id: i64,
            full_name: String,
            html_url: String,
        }

        let repo: RepositoryResponse = self.get_json(&format!("/repos/{full_name}"))?;
        Ok(RepositoryIdentity {
            id: repo.id,
            full_name: repo.full_name,
            html_url: repo.html_url,
        })
    }

    /// Poll the stable global representation. `last_modified` is sent exactly
    /// as GitHub returned it on the preceding successful complete poll.
    pub fn poll_global_notifications(
        &self,
        last_modified: Option<&str>,
        high_water: Option<DateTime<Utc>>,
    ) -> Result<NotificationPoll> {
        let headers = last_modified
            .map(|value| vec![format!("If-Modified-Since: {value}")])
            .unwrap_or_default();
        self.poll_notifications(
            "/notifications?all=true&per_page=50",
            &headers,
            true,
            high_water,
        )
    }

    /// Perform the one-time, repository-scoped notification bootstrap.
    pub fn bootstrap_notifications(&self, full_name: &str) -> Result<NotificationPoll> {
        validate_full_name(full_name)?;
        self.poll_notifications(
            &format!("/repos/{full_name}/notifications?all=true&per_page=100"),
            &[],
            false,
            None,
        )
    }

    /// Read the current pull request, requested reviewers/teams, and the
    /// viewer's latest effective opinionated review.
    pub fn pull_request_state(
        &self,
        full_name: &str,
        number: u64,
        viewer: &Viewer,
        viewer_team_ids: &[i64],
        trust_team_notification: bool,
    ) -> Result<PullRequestState> {
        validate_full_name(full_name)?;
        let prefix = format!("/repos/{full_name}/pulls/{number}");
        let pull: PullResponse = self.get_json(&prefix)?;
        let requests: RequestedReviewers =
            self.get_json(&format!("{prefix}/requested_reviewers"))?;
        let reviews: Vec<ReviewResponse> =
            self.get_all_pages(&format!("{prefix}/reviews?per_page=100"), &[])?;
        let timeline: Vec<TimelineEvent> = self.get_all_pages(
            &format!("/repos/{full_name}/issues/{number}/timeline?per_page=100"),
            &[],
        )?;

        let latest_review = reviews
            .iter()
            .filter(|review| review.user.id == viewer.id)
            .cloned()
            .filter_map(ReviewResponse::into_opinionated)
            .max_by(|left, right| {
                left.submitted_at
                    .cmp(&right.submitted_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
        let currently_requested_team_ids: HashSet<i64> =
            requests.teams.iter().map(|team| team.id).collect();
        let latest_request = timeline
            .into_iter()
            .filter_map(|event| {
                event.into_review_request(
                    viewer,
                    viewer_team_ids,
                    trust_team_notification,
                    &currently_requested_team_ids,
                )
            })
            .max_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.event_id.cmp(&right.event_id))
            });
        let non_opinionated_review_after_request = latest_request.as_ref().is_some_and(|request| {
            reviews.iter().any(|review| {
                review.user.id == viewer.id
                    && !matches!(review.state.as_str(), "APPROVED" | "CHANGES_REQUESTED")
                    && review
                        .submitted_at
                        .is_some_and(|submitted| submitted >= request.created_at)
            })
        });

        Ok(PullRequestState {
            id: pull.id,
            number: pull.number,
            url: pull.html_url,
            title: pull.title,
            author: pull.user,
            state: pull.state,
            draft: pull.draft,
            merged: pull.merged,
            head_sha: pull.head.sha,
            head_ref: pull.head.reference,
            base_sha: pull.base.sha,
            base_ref: pull.base.reference,
            directly_requested: requests.users.iter().any(|user| user.id == viewer.id),
            requested_team_ids: requests.teams.into_iter().map(|team| team.id).collect(),
            latest_review,
            latest_request,
            non_opinionated_review_after_request,
        })
    }

    /// Refresh the inexpensive pull-request core in one REST request while
    /// retaining authoritative review and request state from the last full
    /// hydration.
    ///
    /// This is intended for frequent tracked-PR checks. It detects lifecycle,
    /// draft, head/base, author, title, and URL changes without re-fetching the
    /// requested-reviewer, review, and issue-timeline collections. Call
    /// [`Self::pull_request_state`] after notification discovery and before a
    /// dispatch that needs freshly authoritative review/request state.
    pub fn refresh_pull_request_core(
        &self,
        full_name: &str,
        cached: &PullRequestState,
    ) -> Result<PullRequestState> {
        validate_full_name(full_name)?;
        let endpoint = format!("/repos/{full_name}/pulls/{}", cached.number);
        let pull: PullResponse = self.get_json(&endpoint)?;
        if pull.id != cached.id || pull.number != cached.number {
            return Err(GhError::Protocol(format!(
                "pull request identity changed while refreshing {full_name}#{} \
                 (expected id {}, received id {} and number {})",
                cached.number, cached.id, pull.id, pull.number
            )));
        }

        Ok(PullRequestState {
            id: pull.id,
            number: pull.number,
            url: pull.html_url,
            title: pull.title,
            author: pull.user,
            state: pull.state,
            draft: pull.draft,
            merged: pull.merged,
            head_sha: pull.head.sha,
            head_ref: pull.head.reference,
            base_sha: pull.base.sha,
            base_ref: pull.base.reference,
            directly_requested: cached.directly_requested,
            requested_team_ids: cached.requested_team_ids.clone(),
            latest_review: cached.latest_review.clone(),
            latest_request: cached.latest_request.clone(),
            non_opinionated_review_after_request: cached.non_opinionated_review_after_request,
        })
    }

    fn poll_notifications(
        &self,
        first_endpoint: &str,
        extra_headers: &[String],
        allow_not_modified: bool,
        high_water: Option<DateTime<Utc>>,
    ) -> Result<NotificationPoll> {
        let first = self.request(first_endpoint, extra_headers)?;
        if first.metadata.status == 304 && allow_not_modified {
            return Ok(NotificationPoll {
                not_modified: true,
                notifications: Vec::new(),
                metadata: first.metadata,
            });
        }
        ensure_success(first_endpoint, &first)?;

        let mut notifications: Vec<Notification> = decode_body(first_endpoint, &first.body)?;
        let mut metadata = first.metadata;
        let mut next = if page_crosses_overlap(&notifications, high_water) {
            None
        } else {
            metadata.next.clone()
        };
        let mut visited = HashSet::new();
        visited.insert(first_endpoint.to_owned());

        while let Some(link) = next {
            let endpoint = endpoint_from_link(&link)?;
            if !visited.insert(endpoint.clone()) {
                return Err(GhError::Protocol(format!("pagination loop at {endpoint}")));
            }
            // Conditional headers belong only to the stable first-page URL.
            let page = self.request(&endpoint, &[])?;
            ensure_success(&endpoint, &page)?;
            let mut values: Vec<Notification> = decode_body(&endpoint, &page.body)?;
            let crossed_overlap = page_crosses_overlap(&values, high_water);
            notifications.append(&mut values);
            merge_rate_metadata(&mut metadata, &page.metadata);
            next = if crossed_overlap {
                None
            } else {
                page.metadata.next
            };
        }

        Ok(NotificationPoll {
            not_modified: false,
            notifications,
            metadata,
        })
    }

    fn get_json<T: DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        let response = self.request(endpoint, &[])?;
        ensure_success(endpoint, &response)?;
        decode_body(endpoint, &response.body)
    }

    fn get_all_pages<T: DeserializeOwned>(
        &self,
        first_endpoint: &str,
        headers: &[String],
    ) -> Result<Vec<T>> {
        let mut endpoint = first_endpoint.to_owned();
        let mut first = true;
        let mut values = Vec::new();
        let mut visited = HashSet::new();

        loop {
            if !visited.insert(endpoint.clone()) {
                return Err(GhError::Protocol(format!("pagination loop at {endpoint}")));
            }
            let response = self.request(&endpoint, if first { headers } else { &[] })?;
            ensure_success(&endpoint, &response)?;
            let mut page: Vec<T> = decode_body(&endpoint, &response.body)?;
            values.append(&mut page);
            let Some(link) = response.metadata.next else {
                break;
            };
            endpoint = endpoint_from_link(&link)?;
            first = false;
        }
        Ok(values)
    }

    fn request(&self, endpoint: &str, extra_headers: &[String]) -> Result<ApiResponse> {
        let mut command = Command::new(&self.executable);
        command
            .arg("api")
            .arg("--hostname")
            .arg(API_HOST)
            .arg("--include")
            .arg("--method")
            .arg("GET")
            .arg("-H")
            .arg(ACCEPT_HEADER)
            .arg("-H")
            .arg(API_VERSION_HEADER);
        for header in extra_headers {
            command.arg("-H").arg(header);
        }
        command.arg(endpoint);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|source| GhError::Spawn {
            executable: PathBuf::from(&self.executable),
            source,
        })?;
        let mut stdout = child.stdout.take().expect("stdout was piped");
        let mut stderr = child.stderr.take().expect("stderr was piped");
        let stdout_reader = thread::spawn(move || {
            let mut bytes = Vec::new();
            stdout.read_to_end(&mut bytes).map(|_| bytes)
        });
        let stderr_reader = thread::spawn(move || {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).map(|_| bytes)
        });
        let deadline = Instant::now() + GH_COMMAND_TIMEOUT;
        let status = loop {
            if self
                .cancelled
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::SeqCst))
            {
                let _ = child.kill();
                let _ = child.wait();
                drop(stdout_reader);
                drop(stderr_reader);
                return Err(GhError::Cancelled {
                    endpoint: endpoint.to_owned(),
                });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let _ = child.kill();
                let _ = child.wait();
                drop(stdout_reader);
                drop(stderr_reader);
                return Err(GhError::Timeout {
                    endpoint: endpoint.to_owned(),
                });
            }
            if let Some(status) = child
                .wait_timeout(remaining.min(Duration::from_millis(50)))
                .map_err(|source| GhError::Spawn {
                    executable: PathBuf::from(&self.executable),
                    source,
                })?
            {
                break status;
            }
        };
        let stdout = stdout_reader
            .join()
            .map_err(|_| GhError::Protocol("gh stdout reader panicked".into()))?
            .map_err(|error| GhError::Protocol(format!("could not read gh stdout: {error}")))?;
        let stderr = stderr_reader
            .join()
            .map_err(|_| GhError::Protocol("gh stderr reader panicked".into()))?
            .map_err(|error| GhError::Protocol(format!("could not read gh stderr: {error}")))?;
        let output = Output {
            status,
            stdout,
            stderr,
        };
        parse_output(endpoint, output)
    }
}

#[derive(Debug)]
struct ApiResponse {
    metadata: ResponseMetadata,
    body: String,
    stderr: String,
    command_success: bool,
}

fn parse_output(endpoint: &str, output: Output) -> Result<ApiResponse> {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let (status, headers, body) = parse_http_response(&stdout).map_err(|message| {
        GhError::Protocol(format!("{endpoint}: {message}; stderr: {}", stderr.trim()))
    })?;
    Ok(ApiResponse {
        metadata: metadata_from_headers(status, &headers),
        body: body.to_owned(),
        stderr,
        command_success: output.status.success(),
    })
}

fn ensure_success(endpoint: &str, response: &ApiResponse) -> Result<()> {
    if (200..300).contains(&response.metadata.status) && response.command_success {
        return Ok(());
    }
    Err(GhError::Status {
        endpoint: endpoint.to_owned(),
        metadata: Box::new(response.metadata.clone()),
        body: response.body.clone(),
        stderr: response.stderr.clone(),
    })
}

fn decode_body<T: DeserializeOwned>(endpoint: &str, body: &str) -> Result<T> {
    serde_json::from_str(body).map_err(|source| GhError::Decode {
        endpoint: endpoint.to_owned(),
        source,
        body: body.to_owned(),
    })
}

/// Parse the output of `gh api --include`. Header names are normalized to
/// lowercase and the returned body is left byte-for-byte intact apart from
/// UTF-8 lossy conversion performed at the process boundary.
fn parse_http_response(
    input: &str,
) -> std::result::Result<(u16, HashMap<String, String>, &str), String> {
    let mut offset = 0;

    // Be tolerant of blank lines before an HTTP status line.
    while input[offset..].starts_with('\n') || input[offset..].starts_with("\r\n") {
        offset += if input[offset..].starts_with("\r\n") {
            2
        } else {
            1
        };
    }

    loop {
        let header_end = input[offset..]
            .find("\r\n\r\n")
            .map(|position| (position, 4))
            .or_else(|| input[offset..].find("\n\n").map(|position| (position, 2)))
            .ok_or_else(|| "missing blank line after response headers".to_owned())?;
        let header_block = &input[offset..offset + header_end.0];
        let mut lines = header_block.lines();
        let status_line = lines
            .next()
            .ok_or_else(|| "missing HTTP status line".to_owned())?
            .trim_end_matches('\r');
        let mut status_parts = status_line.split_whitespace();
        let protocol = status_parts.next().unwrap_or_default();
        if !protocol.starts_with("HTTP/") {
            return Err(format!("expected HTTP status line, got {status_line:?}"));
        }
        let status = status_parts
            .next()
            .ok_or_else(|| format!("status code missing from {status_line:?}"))?
            .parse::<u16>()
            .map_err(|_| format!("invalid status line {status_line:?}"))?;

        let mut headers = HashMap::new();
        for line in lines {
            let line = line.trim_end_matches('\r');
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| format!("malformed response header {line:?}"))?;
            headers
                .entry(name.trim().to_ascii_lowercase())
                .and_modify(|existing: &mut String| {
                    existing.push_str(", ");
                    existing.push_str(value.trim());
                })
                .or_insert_with(|| value.trim().to_owned());
        }

        let body_offset = offset + header_end.0 + header_end.1;
        // Handle informational/proxy header blocks if gh ever exposes them.
        if input[body_offset..].starts_with("HTTP/") {
            offset = body_offset;
            continue;
        }
        return Ok((status, headers, &input[body_offset..]));
    }
}

fn metadata_from_headers(status: u16, headers: &HashMap<String, String>) -> ResponseMetadata {
    ResponseMetadata {
        status,
        last_modified: headers.get("last-modified").cloned(),
        poll_interval: duration_header(headers, "x-poll-interval"),
        retry_after: duration_header(headers, "retry-after"),
        rate_limit_remaining: integer_header(headers, "x-ratelimit-remaining"),
        rate_limit_reset: integer_header(headers, "x-ratelimit-reset"),
        next: headers.get("link").and_then(|value| next_link(value)),
    }
}

fn duration_header(headers: &HashMap<String, String>, name: &str) -> Option<Duration> {
    integer_header(headers, name).map(Duration::from_secs)
}

fn integer_header(headers: &HashMap<String, String>, name: &str) -> Option<u64> {
    headers.get(name)?.trim().parse().ok()
}

fn next_link(header: &str) -> Option<String> {
    header.split(',').find_map(|part| {
        let mut pieces = part.trim().split(';');
        let target = pieces.next()?.trim();
        let is_next = pieces.any(|parameter| {
            let parameter = parameter.trim();
            parameter == "rel=\"next\"" || parameter == "rel=next"
        });
        if is_next {
            target
                .strip_prefix('<')
                .and_then(|value| value.strip_suffix('>'))
                .map(str::to_owned)
        } else {
            None
        }
    })
}

fn endpoint_from_link(link: &str) -> Result<String> {
    if let Some(endpoint) = link.strip_prefix(API_URL_PREFIX)
        && endpoint.starts_with('/')
    {
        return Ok(endpoint.to_owned());
    }
    if link.starts_with('/') {
        return Ok(link.to_owned());
    }
    Err(GhError::Protocol(format!(
        "refusing pagination URL outside api.github.com: {link}"
    )))
}

fn merge_rate_metadata(target: &mut ResponseMetadata, page: &ResponseMetadata) {
    if let Some(remaining) = page.rate_limit_remaining {
        target.rate_limit_remaining = Some(
            target
                .rate_limit_remaining
                .map_or(remaining, |current| current.min(remaining)),
        );
    }
    if page.rate_limit_reset.is_some() {
        target.rate_limit_reset = page.rate_limit_reset;
    }
    if page.retry_after.is_some() {
        target.retry_after = page.retry_after;
    }
}

/// Notification pages are newest-first. Retain a one-minute overlap before
/// the durable high-water mark so equal timestamps and close races are seen
/// again, then stop before requesting another page.
fn page_crosses_overlap(page: &[Notification], high_water: Option<DateTime<Utc>>) -> bool {
    let Some(high_water) = high_water else {
        return false;
    };
    let boundary = high_water
        .checked_sub_signed(TimeDelta::seconds(60))
        .unwrap_or(DateTime::<Utc>::MIN_UTC);
    page.iter()
        .any(|notification| notification.updated_at <= boundary)
}

/// Extract `owner/repository` and PR number from a REST API pull URL.
pub fn parse_pull_request_api_url(url: &str) -> Result<(String, u64)> {
    let path = url.strip_prefix(API_URL_PREFIX).unwrap_or(url);
    let path = path.split_once('?').map_or(path, |(path, _query)| path);
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    if let ["repos", owner, repository, "pulls", number] = parts.as_slice() {
        let number = number
            .parse::<u64>()
            .map_err(|_| GhError::Protocol(format!("invalid pull request number in URL: {url}")))?;
        return Ok((format!("{owner}/{repository}"), number));
    }
    Err(GhError::Protocol(format!(
        "not a GitHub pull request API URL: {url}"
    )))
}

pub fn parse_pull_request_number(url: &str) -> Result<u64> {
    parse_pull_request_api_url(url).map(|(_, number)| number)
}

fn validate_full_name(full_name: &str) -> Result<()> {
    let mut components = full_name.split('/');
    match (components.next(), components.next(), components.next()) {
        (Some(owner), Some(repository), None) if !owner.is_empty() && !repository.is_empty() => {
            Ok(())
        }
        _ => Err(GhError::Protocol(format!(
            "invalid GitHub repository name: {full_name}"
        ))),
    }
}

#[derive(Deserialize)]
struct PullResponse {
    id: i64,
    number: u64,
    html_url: String,
    title: String,
    user: Viewer,
    state: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    merged: bool,
    head: PullRevision,
    base: PullRevision,
}

#[derive(Deserialize)]
struct PullRevision {
    sha: String,
    #[serde(rename = "ref")]
    reference: String,
}

#[derive(Deserialize)]
struct RequestedReviewers {
    #[serde(default)]
    users: Vec<Viewer>,
    #[serde(default)]
    teams: Vec<TeamId>,
}

#[derive(Deserialize)]
struct TeamId {
    id: i64,
}

#[derive(Clone, Deserialize)]
struct ReviewResponse {
    id: i64,
    user: Viewer,
    state: String,
    commit_id: Option<String>,
    submitted_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
struct TimelineEvent {
    id: i64,
    node_id: Option<String>,
    event: String,
    created_at: DateTime<Utc>,
    requested_reviewer: Option<Viewer>,
    requested_team: Option<TeamId>,
}

impl TimelineEvent {
    fn into_review_request(
        self,
        viewer: &Viewer,
        viewer_team_ids: &[i64],
        trust_team_notification: bool,
        currently_requested_team_ids: &HashSet<i64>,
    ) -> Option<ReviewRequestEvent> {
        if self.event != "review_requested" {
            return None;
        }
        let (kind, requested_id) = if self
            .requested_reviewer
            .as_ref()
            .is_some_and(|requested| requested.id == viewer.id)
        {
            (ReviewRequestKind::User, viewer.id)
        } else {
            let team_id = self.requested_team?.id;
            let belongs_to_viewer = viewer_team_ids.contains(&team_id);
            let trusted_active_team =
                trust_team_notification && currently_requested_team_ids.contains(&team_id);
            if !belongs_to_viewer && !trusted_active_team {
                return None;
            }
            (ReviewRequestKind::Team, team_id)
        };
        Some(ReviewRequestEvent {
            event_id: self.node_id.unwrap_or_else(|| self.id.to_string()),
            created_at: self.created_at,
            kind,
            requested_id,
        })
    }
}

impl ReviewResponse {
    fn into_opinionated(self) -> Option<OpinionatedReview> {
        let state = match self.state.as_str() {
            "APPROVED" => ReviewState::Approved,
            "CHANGES_REQUESTED" => ReviewState::ChangesRequested,
            _ => return None,
        };
        Some(OpinionatedReview {
            id: self.id,
            state,
            commit_sha: self.commit_id,
            submitted_at: self.submitted_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_crlf_headers_and_metadata() {
        let input = concat!(
            "HTTP/2.0 200 OK\r\n",
            "Last-Modified: Tue, 01 Jan 2030 00:00:00 GMT\r\n",
            "X-Poll-Interval: 60\r\n",
            "X-RateLimit-Remaining: 4999\r\n",
            "Link: <https://api.github.com/notifications?page=2>; rel=\"next\", ",
            "<https://api.github.com/notifications?page=5>; rel=\"last\"\r\n",
            "\r\n",
            "[{\"id\":1}]"
        );
        let (status, headers, body) = parse_http_response(input).unwrap();
        let metadata = metadata_from_headers(status, &headers);
        assert_eq!(metadata.status, 200);
        assert_eq!(metadata.poll_interval, Some(Duration::from_secs(60)));
        assert_eq!(metadata.rate_limit_remaining, Some(4999));
        assert_eq!(
            metadata.next.as_deref(),
            Some("https://api.github.com/notifications?page=2")
        );
        assert_eq!(body, "[{\"id\":1}]");
    }

    #[test]
    fn parses_304_without_a_body() {
        let input = "HTTP/2 304 Not Modified\nX-Poll-Interval: 90\n\n";
        let (status, headers, body) = parse_http_response(input).unwrap();
        assert_eq!(status, 304);
        assert_eq!(body, "");
        assert_eq!(
            metadata_from_headers(status, &headers).poll_interval,
            Some(Duration::from_secs(90))
        );
    }

    #[test]
    fn uses_last_header_block() {
        let input = "HTTP/1.1 100 Continue\r\n\r\nHTTP/2 200 OK\r\nFoo: bar\r\n\r\n{}";
        let (status, headers, body) = parse_http_response(input).unwrap();
        assert_eq!(status, 200);
        assert_eq!(headers.get("foo").map(String::as_str), Some("bar"));
        assert_eq!(body, "{}");
    }

    #[test]
    fn extracts_pull_request_identity() {
        assert_eq!(
            parse_pull_request_api_url("https://api.github.com/repos/acme/widgets/pulls/42")
                .unwrap(),
            ("acme/widgets".to_owned(), 42)
        );
        assert_eq!(
            parse_pull_request_api_url("/repos/acme/widgets/pulls/42?ignored=yes").unwrap(),
            ("acme/widgets".to_owned(), 42)
        );
    }

    #[test]
    fn rejects_non_github_pagination_hosts() {
        assert!(endpoint_from_link("https://evil.example/steal").is_err());
    }

    #[test]
    fn ignores_non_opinionated_reviews() {
        let review = ReviewResponse {
            id: 1,
            user: Viewer {
                id: 2,
                login: "reviewer".into(),
            },
            state: "COMMENTED".into(),
            commit_id: Some("abc".into()),
            submitted_at: None,
        };
        assert!(review.into_opinionated().is_none());
    }

    #[test]
    fn notification_page_crossing_overlap_stops_pagination() {
        let page: Vec<Notification> = serde_json::from_value(serde_json::json!([{
            "id": "thread",
            "reason": "review_requested",
            "unread": true,
            "updated_at": "2030-01-01T11:59:00Z",
            "repository": {"id": 7, "full_name": "acme/widgets"},
            "subject": {"title": "Review", "type": "PullRequest", "url": null}
        }]))
        .unwrap();
        assert!(page_crosses_overlap(
            &page,
            Some("2030-01-01T12:00:00Z".parse().unwrap())
        ));
        assert!(!page_crosses_overlap(
            &page,
            Some("2030-01-01T11:59:30Z".parse().unwrap())
        ));
        assert!(!page_crosses_overlap(&page, None));
    }

    #[test]
    fn trusted_notification_accepts_an_active_team_request_event() {
        let event = || TimelineEvent {
            id: 21,
            node_id: Some("RRE_team".into()),
            event: "review_requested".into(),
            created_at: "2030-01-03T00:00:00Z".parse().unwrap(),
            requested_reviewer: None,
            requested_team: Some(TeamId { id: 77 }),
        };
        let viewer = Viewer {
            id: 1,
            login: "me".into(),
        };
        let active_teams = HashSet::from([77]);
        assert!(
            event()
                .into_review_request(&viewer, &[], false, &active_teams)
                .is_none()
        );
        let request = event()
            .into_review_request(&viewer, &[], true, &active_teams)
            .unwrap();
        assert_eq!(request.kind, ReviewRequestKind::Team);
        assert_eq!(request.requested_id, 77);
    }

    #[cfg(unix)]
    #[test]
    fn injected_executable_receives_conditional_header_and_handles_304() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("fake-gh");
        let args = temp.path().join("args");
        fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf 'HTTP/2 304 Not Modified\\nX-Poll-Interval: 75\\n\\n'\nexit 1\n",
                args.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

        let poll = GhClient::with_executable(&script)
            .poll_global_notifications(Some("Tue, 01 Jan 2030 00:00:00 GMT"), None)
            .unwrap();
        assert!(poll.not_modified);
        assert_eq!(poll.metadata.poll_interval, Some(Duration::from_secs(75)));
        let recorded = fs::read_to_string(args).unwrap();
        assert!(recorded.contains("If-Modified-Since: Tue, 01 Jan 2030 00:00:00 GMT"));
        assert!(recorded.contains("/notifications?all=true&per_page=50"));
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_interrupts_an_inflight_gh_request() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let fixture = tempfile::tempdir().unwrap();
        let script = fixture.path().join("gh");
        fs::write(&script, "#!/bin/sh\nexec sleep 30\n").unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&cancelled);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            signal.store(true, Ordering::SeqCst);
        });
        let started = Instant::now();
        let error = GhClient::with_executable(script)
            .with_cancellation(cancelled)
            .viewer()
            .unwrap_err();
        assert!(matches!(error, GhError::Cancelled { .. }));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[test]
    fn notification_poll_follows_link_pages_without_forwarding_validator() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("fake-gh");
        let calls = temp.path().join("calls");
        let notification = |id: &str| {
            format!(
                r#"[{{"id":"{id}","reason":"review_requested","unread":true,"updated_at":"2030-01-01T00:00:00Z","repository":{{"id":7,"full_name":"acme/widgets"}},"subject":{{"title":"Review","type":"PullRequest","url":"https://api.github.com/repos/acme/widgets/pulls/42"}}}}]"#
            )
        };
        fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$*\" in\n  *page=2*) printf 'HTTP/2 200 OK\\n\\n%s' '{}' ;;\n  *) printf 'HTTP/2 200 OK\\nLast-Modified: later\\nLink: <https://api.github.com/notifications?all=true&per_page=100&page=2>; rel=\"next\"\\n\\n%s' '{}' ;;\nesac\n",
                calls.display(),
                notification("second"),
                notification("first")
            ),
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

        let poll = GhClient::with_executable(&script)
            .poll_global_notifications(Some("older"), None)
            .unwrap();
        assert_eq!(poll.notifications.len(), 2);
        assert_eq!(poll.metadata.last_modified.as_deref(), Some("later"));
        let recorded = fs::read_to_string(&calls).unwrap();
        let recorded_calls: Vec<_> = recorded.lines().collect();
        assert_eq!(recorded_calls.len(), 2);
        assert!(recorded_calls[0].contains("If-Modified-Since: older"));
        assert!(!recorded_calls[1].contains("If-Modified-Since"));

        fs::write(&calls, "").unwrap();
        let bounded_poll = GhClient::with_executable(&script)
            .poll_global_notifications(Some("older"), Some("2030-01-01T00:01:00Z".parse().unwrap()))
            .unwrap();
        assert_eq!(bounded_poll.notifications.len(), 1);
        assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn enriches_pull_request_and_selects_latest_opinionated_viewer_review() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("fake-gh");
        fs::write(
            &script,
            r#"#!/bin/sh
case "$*" in
  *requested_reviewers*) body='{"users":[{"id":1,"login":"me"}],"teams":[{"id":77}]}' ; link='' ;;
  *reviews*page=2*) body='[{"id":12,"user":{"id":1,"login":"me"},"state":"CHANGES_REQUESTED","commit_id":"old-head","submitted_at":"2030-01-02T00:00:00Z"}]' ; link='' ;;
  *reviews*) body='[{"id":11,"user":{"id":1,"login":"me"},"state":"APPROVED","commit_id":"older-head","submitted_at":"2030-01-01T00:00:00Z"},{"id":13,"user":{"id":2,"login":"other"},"state":"APPROVED","commit_id":"head","submitted_at":"2030-01-03T00:00:00Z"}]' ; link='Link: <https://api.github.com/repos/acme/widgets/pulls/42/reviews?per_page=100&page=2>; rel="next"' ;;
  *timeline*) body='[{"id":20,"node_id":"RRE_user","event":"review_requested","created_at":"2030-01-01T00:00:00Z","requested_reviewer":{"id":1,"login":"me"}},{"id":21,"node_id":"RRE_team","event":"review_requested","created_at":"2030-01-03T00:00:00Z","requested_team":{"id":77}}]' ; link='' ;;
  *pulls/42*) body='{"id":99,"number":42,"html_url":"https://github.com/acme/widgets/pull/42","title":"Change","user":{"id":3,"login":"author"},"state":"open","draft":false,"merged":false,"head":{"sha":"head","ref":"feature"},"base":{"sha":"base","ref":"main"}}' ; link='' ;;
  *) exit 2 ;;
esac
printf 'HTTP/2 200 OK\n%s\n\n%s' "$link" "$body"
"#,
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

        let pull = GhClient::with_executable(&script)
            .pull_request_state(
                "acme/widgets",
                42,
                &Viewer {
                    id: 1,
                    login: "me".into(),
                },
                &[77],
                false,
            )
            .unwrap();
        assert!(pull.directly_requested);
        assert_eq!(pull.requested_team_ids, vec![77]);
        assert_eq!(pull.head_sha, "head");
        let review = pull.latest_review.unwrap();
        assert_eq!(review.id, 12);
        assert_eq!(review.state, ReviewState::ChangesRequested);
        assert_eq!(review.commit_sha.as_deref(), Some("old-head"));
        let request = pull.latest_request.unwrap();
        assert_eq!(request.event_id, "RRE_team");
        assert_eq!(request.kind, ReviewRequestKind::Team);
        assert_eq!(request.requested_id, 77);
    }

    #[cfg(unix)]
    #[test]
    fn core_refresh_uses_one_request_and_preserves_authoritative_review_state() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("fake-gh");
        let calls = temp.path().join("calls");
        fs::write(
            &script,
            format!(
                r#"#!/bin/sh
printf '%s\n' "$*" >> '{}'
printf 'HTTP/2 200 OK\n\n%s' '{{"id":99,"number":42,"html_url":"https://github.com/acme/widgets/pull/42","title":"Renamed","user":{{"id":4,"login":"new-author"}},"state":"closed","draft":false,"merged":true,"head":{{"sha":"new-head","ref":"feature-v2"}},"base":{{"sha":"new-base","ref":"trunk"}}}}'
"#,
                calls.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let review = OpinionatedReview {
            id: 12,
            state: ReviewState::ChangesRequested,
            commit_sha: Some("old-head".into()),
            submitted_at: Some("2030-01-02T00:00:00Z".parse().unwrap()),
        };
        let request = ReviewRequestEvent {
            event_id: "RRE_team".into(),
            created_at: "2030-01-03T00:00:00Z".parse().unwrap(),
            kind: ReviewRequestKind::Team,
            requested_id: 77,
        };
        let cached = PullRequestState {
            id: 99,
            number: 42,
            url: "old-url".into(),
            title: "Old title".into(),
            author: Viewer {
                id: 3,
                login: "old-author".into(),
            },
            state: "open".into(),
            draft: true,
            merged: false,
            head_sha: "old-head".into(),
            head_ref: "feature".into(),
            base_sha: "old-base".into(),
            base_ref: "main".into(),
            directly_requested: false,
            requested_team_ids: vec![77],
            latest_review: Some(review.clone()),
            latest_request: Some(request.clone()),
            non_opinionated_review_after_request: false,
        };

        let refreshed = GhClient::with_executable(&script)
            .refresh_pull_request_core("acme/widgets", &cached)
            .unwrap();
        assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 1);
        assert_eq!(refreshed.state, "closed");
        assert!(!refreshed.draft);
        assert!(refreshed.merged);
        assert_eq!(refreshed.author.id, 4);
        assert_eq!(refreshed.head_sha, "new-head");
        assert_eq!(refreshed.head_ref, "feature-v2");
        assert_eq!(refreshed.base_sha, "new-base");
        assert_eq!(refreshed.base_ref, "trunk");
        assert_eq!(refreshed.latest_review.unwrap().id, review.id);
        assert_eq!(refreshed.latest_request.unwrap(), request);
        assert_eq!(refreshed.requested_team_ids, vec![77]);
    }
}
