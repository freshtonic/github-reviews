//! GitHub API access through the authenticated `gh` CLI.
//!
//! Keeping the process boundary in this module is deliberate: `gh` owns
//! authentication, while the rest of the application gets typed values and
//! never needs to handle credentials.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fmt;
use std::io::{self, Read, Write};
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
use crate::runner::{configure_process_group, terminate_and_reap};

const API_HOST: &str = "github.com";
const API_URL_PREFIX: &str = "https://api.github.com";
const ACCEPT_HEADER: &str = "Accept: application/vnd.github+json";
const API_VERSION_HEADER: &str = "X-GitHub-Api-Version: 2022-11-28";
const GH_COMMAND_TIMEOUT: Duration = Duration::from_secs(45);

/// Pull requests read by one GraphQL query. Each reads three connections of
/// up to 100 nodes, so a full batch costs one GraphQL rate-limit point.
pub const PULL_REQUEST_BATCH_SIZE: usize = 25;

/// REST represents the author of a deleted account as this `ghost` user;
/// GraphQL returns a null author instead.
const GHOST_USER_ID: i64 = 10137;

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
    NotFound {
        what: String,
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

    /// True when the repository or pull request no longer exists, or is no
    /// longer visible to the viewer.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
            || self
                .metadata()
                .is_some_and(|metadata| metadata.status == 404)
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
            Self::NotFound { what } => write!(f, "GitHub could not find {what}"),
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
    cancelled: Vec<Arc<AtomicBool>>,
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
            cancelled: Vec::new(),
        }
    }

    /// Return a client whose requests are also cancelled when `cancelled` is
    /// set. Flags from this client remain in effect.
    pub fn with_cancellation(&self, cancelled: Arc<AtomicBool>) -> Self {
        let mut client = self.clone();
        client.cancelled.push(cancelled);
        client
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
        self.pull_request_states(
            &[(full_name, number)],
            viewer,
            viewer_team_ids,
            trust_team_notification,
        )?
        .pop()
        .expect("one result per requested pull request")
    }

    /// Read [`Self::pull_request_state`] for many pull requests with batched
    /// GraphQL queries: one `gh` process per [`PULL_REQUEST_BATCH_SIZE`] pull
    /// requests, rather than at least four REST requests for each.
    ///
    /// The outer error fails every pull request: transport failure, rate
    /// limiting, or cancellation. Otherwise the result for `pulls[i]` is at
    /// index `i`, and a missing pull request or repository is
    /// [`GhError::NotFound`].
    pub fn pull_request_states(
        &self,
        pulls: &[(&str, u64)],
        viewer: &Viewer,
        viewer_team_ids: &[i64],
        trust_team_notification: bool,
    ) -> Result<Vec<Result<PullRequestState>>> {
        let mut states = Vec::with_capacity(pulls.len());
        for batch in pulls.chunks(PULL_REQUEST_BATCH_SIZE) {
            let fetched = self.pull_request_batch(batch, viewer)?;
            for (&(full_name, number), pull) in batch.iter().zip(fetched) {
                states.push(
                    pull.and_then(|pull| {
                        self.read_remaining_pages(full_name, number, viewer, pull)
                    })
                    .and_then(GraphPullRequest::into_rest)
                    .map(|(pull, requests, reviews, timeline)| {
                        assemble_pull_request_state(
                            pull,
                            requests,
                            reviews,
                            timeline,
                            viewer,
                            viewer_team_ids,
                            trust_team_notification,
                        )
                    }),
                );
            }
        }
        Ok(states)
    }

    /// Query one batch. Repository owner and name travel as GraphQL
    /// variables; pull request numbers are integers and are inlined.
    fn pull_request_batch(
        &self,
        batch: &[(&str, u64)],
        viewer: &Viewer,
    ) -> Result<Vec<Result<GraphPullRequest>>> {
        let mut repositories: Vec<&str> = Vec::new();
        let mut aliases = Vec::with_capacity(batch.len());
        for &(full_name, _) in batch {
            validate_full_name(full_name)?;
            let index = repositories
                .iter()
                .position(|repository| *repository == full_name)
                .unwrap_or_else(|| {
                    repositories.push(full_name);
                    repositories.len() - 1
                });
            aliases.push(index);
        }

        let mut variables = serde_json::Map::new();
        variables.insert("viewer".into(), viewer.login.clone().into());
        let mut declarations = String::from("$viewer: String!");
        let mut selections = String::new();
        for (repository_index, full_name) in repositories.iter().enumerate() {
            let (owner, name) = full_name.split_once('/').expect("validated full name");
            variables.insert(format!("o{repository_index}"), owner.into());
            variables.insert(format!("n{repository_index}"), name.into());
            declarations.push_str(&format!(
                ", $o{repository_index}: String!, $n{repository_index}: String!"
            ));
            selections.push_str(&format!(
                " r{repository_index}: repository(owner: $o{repository_index}, name: $n{repository_index}) {{"
            ));
            for (pull_index, (&(_, number), &alias)) in batch.iter().zip(&aliases).enumerate() {
                if alias == repository_index {
                    selections.push_str(&format!(
                        " p{pull_index}: pullRequest(number: {number}) {{ ...PullRequestState }}"
                    ));
                }
            }
            selections.push_str(" }");
        }
        let query = format!(
            "query({declarations}) {{{selections} }} fragment PullRequestState on PullRequest {{ {} }}",
            pull_request_fields()
        );
        let data = self.graphql(&query, variables)?;

        Ok(batch
            .iter()
            .zip(&aliases)
            .enumerate()
            .map(|(pull_index, (&(full_name, number), &repository_index))| {
                let repository_alias = format!("r{repository_index}");
                let pull_alias = format!("p{pull_index}");
                data.result_at(&[&repository_alias, &pull_alias], || {
                    format!("{full_name}#{number}")
                })
            })
            .collect())
    }

    /// Fetch any further pages of the three pull request connections. Busy
    /// pull requests rarely need this, so pages are read one at a time.
    fn read_remaining_pages(
        &self,
        full_name: &str,
        number: u64,
        viewer: &Viewer,
        mut pull: GraphPullRequest,
    ) -> Result<GraphPullRequest> {
        self.extend_connection(
            full_name,
            number,
            viewer,
            PullConnection::ReviewRequests,
            &mut pull.review_requests,
        )?;
        self.extend_connection(
            full_name,
            number,
            viewer,
            PullConnection::Reviews,
            &mut pull.reviews,
        )?;
        self.extend_connection(
            full_name,
            number,
            viewer,
            PullConnection::ReviewRequestedEvents,
            &mut pull.timeline_items,
        )?;
        Ok(pull)
    }

    fn extend_connection<T: DeserializeOwned>(
        &self,
        full_name: &str,
        number: u64,
        viewer: &Viewer,
        kind: PullConnection,
        connection: &mut Connection<T>,
    ) -> Result<()> {
        let (owner, name) = full_name.split_once('/').expect("validated full name");
        let mut visited = HashSet::new();
        while connection.page_info.has_next_page {
            let cursor = connection.page_info.end_cursor.clone().ok_or_else(|| {
                GhError::Protocol(format!(
                    "{full_name}#{number}: {} page has no end cursor",
                    kind.field()
                ))
            })?;
            if !visited.insert(cursor.clone()) {
                return Err(GhError::Protocol(format!(
                    "{full_name}#{number}: pagination loop in {}",
                    kind.field()
                )));
            }
            let mut variables = serde_json::Map::new();
            variables.insert("o".into(), owner.into());
            variables.insert("n".into(), name.into());
            variables.insert("after".into(), cursor.into());
            let mut declarations = String::from("$o: String!, $n: String!, $after: String!");
            if kind == PullConnection::Reviews {
                variables.insert("viewer".into(), viewer.login.clone().into());
                declarations.push_str(", $viewer: String!");
            }
            let query = format!(
                "query({declarations}) {{ r: repository(owner: $o, name: $n) {{ p: pullRequest(number: {number}) {{ {} }} }} }}",
                kind.selection(true)
            );
            let data = self.graphql(&query, variables)?;
            let mut page: GraphConnectionPage<T> =
                data.result_at(&["r", "p"], || format!("{full_name}#{number}"))?;
            let mut next = page.0.remove(kind.field()).ok_or_else(|| {
                GhError::Protocol(format!(
                    "{full_name}#{number}: {} page is missing",
                    kind.field()
                ))
            })?;
            connection.nodes.append(&mut next.nodes);
            connection.page_info = next.page_info;
        }
        Ok(())
    }

    /// Run one GraphQL query. GraphQL reports most failures inside an HTTP
    /// 200 response, and `gh` then exits nonzero, so success is decided from
    /// the HTTP status and the `errors` array rather than the exit status.
    fn graphql(
        &self,
        query: &str,
        variables: serde_json::Map<String, serde_json::Value>,
    ) -> Result<GraphData> {
        const ENDPOINT: &str = "graphql";
        let body = serde_json::to_vec(&serde_json::json!({
            "query": query,
            "variables": variables,
        }))
        .map_err(|error| GhError::Protocol(format!("could not encode GraphQL query: {error}")))?;
        let mut args: Vec<OsString> = vec![
            "api".into(),
            "graphql".into(),
            "--hostname".into(),
            API_HOST.into(),
            "--include".into(),
            "--input".into(),
            "-".into(),
        ];
        args.push("-H".into());
        args.push(API_VERSION_HEADER.into());
        let response = self.run(ENDPOINT, args, Some(body))?;
        if !(200..300).contains(&response.metadata.status) {
            return Err(GhError::Status {
                endpoint: ENDPOINT.to_owned(),
                metadata: Box::new(response.metadata),
                body: response.body,
                stderr: response.stderr,
            });
        }
        let decoded: GraphResponse = decode_body(ENDPOINT, &response.body)?;
        let (located, unlocated): (Vec<_>, Vec<_>) = decoded
            .errors
            .into_iter()
            .partition(|error| !error.path.is_empty());
        if located
            .iter()
            .chain(&unlocated)
            .any(|error| error.kind.as_deref() == Some("RATE_LIMITED"))
            || !unlocated.is_empty()
        {
            // Errors that belong to no pull request fail the whole query.
            return Err(GhError::Status {
                endpoint: ENDPOINT.to_owned(),
                metadata: Box::new(response.metadata),
                body: response.body,
                stderr: response.stderr,
            });
        }
        Ok(GraphData {
            data: decoded.data.unwrap_or(serde_json::Value::Null),
            errors: located,
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
        let mut args: Vec<OsString> = vec![
            "api".into(),
            "--hostname".into(),
            API_HOST.into(),
            "--include".into(),
            "--method".into(),
            "GET".into(),
            "-H".into(),
            ACCEPT_HEADER.into(),
            "-H".into(),
            API_VERSION_HEADER.into(),
        ];
        for header in extra_headers {
            args.push("-H".into());
            args.push(header.into());
        }
        args.push(endpoint.into());
        self.run(endpoint, args, None)
    }

    /// Run `gh` with `args`, writing `input` to its standard input, and parse
    /// the `--include` output. `endpoint` names the request in errors.
    fn run(
        &self,
        endpoint: &str,
        args: Vec<OsString>,
        input: Option<Vec<u8>>,
    ) -> Result<ApiResponse> {
        let mut command = Command::new(&self.executable);
        command
            .args(args)
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_process_group(&mut command);
        let mut child = command.spawn().map_err(|source| GhError::Spawn {
            executable: PathBuf::from(&self.executable),
            source,
        })?;
        if let Some(input) = input {
            let mut stdin = child.stdin.take().expect("stdin was piped");
            // A write error means gh exited early; its output reports why.
            thread::spawn(move || stdin.write_all(&input));
        }
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
                .iter()
                .any(|flag| flag.load(Ordering::SeqCst))
            {
                let _ = terminate_and_reap(&mut child);
                drop(stdout_reader);
                drop(stderr_reader);
                return Err(GhError::Cancelled {
                    endpoint: endpoint.to_owned(),
                });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let _ = terminate_and_reap(&mut child);
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

/// Combine the pull request, its requested reviewers, its reviews, and its
/// review-request timeline events into the state used for actionability.
fn assemble_pull_request_state(
    pull: PullResponse,
    requests: RequestedReviewers,
    reviews: Vec<ReviewResponse>,
    timeline: Vec<TimelineEvent>,
    viewer: &Viewer,
    viewer_team_ids: &[i64],
    trust_team_notification: bool,
) -> PullRequestState {
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

    PullRequestState {
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
    }
}

const ACTOR_FIELDS: &str = "__typename login ... on User { databaseId } ... on Bot { databaseId } ... on Mannequin { databaseId }";
const REQUESTED_REVIEWER_FIELDS: &str =
    "requestedReviewer { __typename ... on User { databaseId login } ... on Team { databaseId } }";

fn pull_request_fields() -> String {
    format!(
        "databaseId number url title state isDraft merged \
         headRefOid headRefName baseRefOid baseRefName author {{ {ACTOR_FIELDS} }} {} {} {}",
        PullConnection::ReviewRequests.selection(false),
        PullConnection::Reviews.selection(false),
        PullConnection::ReviewRequestedEvents.selection(false),
    )
}

/// The paginated pull request connections read for pull request state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PullConnection {
    ReviewRequests,
    /// Only the viewer's reviews: the state never uses anyone else's.
    Reviews,
    /// The issue timeline, filtered to review-request events.
    ReviewRequestedEvents,
}

impl PullConnection {
    fn field(self) -> &'static str {
        match self {
            Self::ReviewRequests => "reviewRequests",
            Self::Reviews => "reviews",
            Self::ReviewRequestedEvents => "timelineItems",
        }
    }

    fn selection(self, after: bool) -> String {
        let after = if after { ", after: $after" } else { "" };
        let (arguments, nodes) = match self {
            Self::ReviewRequests => (String::new(), REQUESTED_REVIEWER_FIELDS.to_owned()),
            Self::Reviews => (
                ", author: $viewer".to_owned(),
                "databaseId state submittedAt commit { oid } author { login ... on User { databaseId } }"
                    .to_owned(),
            ),
            Self::ReviewRequestedEvents => (
                ", itemTypes: [REVIEW_REQUESTED_EVENT]".to_owned(),
                format!("... on ReviewRequestedEvent {{ id createdAt {REQUESTED_REVIEWER_FIELDS} }}"),
            ),
        };
        format!(
            "{}(first: 100{arguments}{after}) {{ pageInfo {{ hasNextPage endCursor }} nodes {{ {nodes} }} }}",
            self.field()
        )
    }
}

#[derive(Deserialize)]
struct GraphResponse {
    data: Option<serde_json::Value>,
    #[serde(default)]
    errors: Vec<GraphErrorEntry>,
}

#[derive(Debug, Deserialize)]
struct GraphErrorEntry {
    #[serde(rename = "type")]
    kind: Option<String>,
    message: String,
    #[serde(default)]
    path: Vec<serde_json::Value>,
}

/// A GraphQL `data` object with the errors that GraphQL attached to paths
/// inside it.
struct GraphData {
    data: serde_json::Value,
    errors: Vec<GraphErrorEntry>,
}

impl GraphData {
    /// Decode the value at `path`, or report the errors that apply to it. An
    /// error applies when its path and `path` agree on their common prefix,
    /// so a missing repository fails each of its pull requests.
    fn result_at<T: DeserializeOwned>(
        &self,
        path: &[&str],
        describe: impl Fn() -> String,
    ) -> Result<T> {
        let errors: Vec<&GraphErrorEntry> = self
            .errors
            .iter()
            .filter(|error| {
                error
                    .path
                    .iter()
                    .zip(path)
                    .all(|(segment, expected)| segment.as_str() == Some(*expected))
            })
            .collect();
        if errors.iter().any(|error| {
            error.kind.as_deref() == Some("NOT_FOUND") && error.path.len() <= path.len()
        }) {
            return Err(GhError::NotFound { what: describe() });
        }
        if !errors.is_empty() {
            let messages: Vec<&str> = errors.iter().map(|error| error.message.as_str()).collect();
            return Err(GhError::Protocol(format!(
                "{}: {}",
                describe(),
                messages.join("; ")
            )));
        }
        let value = path
            .iter()
            .fold(&self.data, |value, segment| &value[*segment]);
        if value.is_null() {
            return Err(GhError::Protocol(format!(
                "{} is missing from the GraphQL response",
                describe()
            )));
        }
        T::deserialize(value).map_err(|source| GhError::Decode {
            endpoint: "graphql".into(),
            source,
            body: value.to_string(),
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Connection<T> {
    page_info: PageInfo,
    nodes: Vec<Option<T>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageInfo {
    has_next_page: bool,
    end_cursor: Option<String>,
}

/// One page of a single connection, keyed by its field name.
#[derive(Deserialize)]
struct GraphConnectionPage<T>(HashMap<String, Connection<T>>);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphPullRequest {
    database_id: i64,
    number: u64,
    url: String,
    title: String,
    state: String,
    is_draft: bool,
    merged: bool,
    head_ref_oid: String,
    head_ref_name: String,
    base_ref_oid: String,
    base_ref_name: String,
    author: Option<GraphActor>,
    review_requests: Connection<GraphReviewRequest>,
    reviews: Connection<GraphReview>,
    timeline_items: Connection<GraphReviewRequestedEvent>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphActor {
    #[serde(rename = "__typename")]
    typename: String,
    login: String,
    database_id: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphReviewRequest {
    requested_reviewer: Option<GraphReviewer>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphReviewer {
    #[serde(rename = "__typename")]
    typename: String,
    database_id: Option<i64>,
    login: Option<String>,
}

impl GraphReviewer {
    fn user(&self) -> Option<Viewer> {
        if self.typename != "User" {
            return None;
        }
        Some(Viewer {
            id: self.database_id?,
            login: self.login.clone()?,
        })
    }

    fn team(&self) -> Option<TeamId> {
        if self.typename != "Team" {
            return None;
        }
        Some(TeamId {
            id: self.database_id?,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphReview {
    database_id: Option<i64>,
    state: String,
    submitted_at: Option<DateTime<Utc>>,
    commit: Option<GraphCommit>,
    author: Option<GraphReviewAuthor>,
}

#[derive(Deserialize)]
struct GraphCommit {
    oid: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphReviewAuthor {
    login: String,
    database_id: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphReviewRequestedEvent {
    id: String,
    created_at: DateTime<Utc>,
    requested_reviewer: Option<GraphReviewer>,
}

impl GraphPullRequest {
    /// Convert to the REST shapes, so that REST-compatible IDs, logins, and
    /// states reach the stored pull request state and the review envelope.
    fn into_rest(
        self,
    ) -> Result<(
        PullResponse,
        RequestedReviewers,
        Vec<ReviewResponse>,
        Vec<TimelineEvent>,
    )> {
        let user = match self.author {
            None => Viewer {
                id: GHOST_USER_ID,
                login: "ghost".into(),
            },
            Some(actor) => Viewer {
                id: actor.database_id.ok_or_else(|| {
                    GhError::Protocol(format!(
                        "{}: author {} ({}) has no database ID",
                        self.url, actor.login, actor.typename
                    ))
                })?,
                // REST names bot accounts with a `[bot]` suffix.
                login: if actor.typename == "Bot" {
                    format!("{}[bot]", actor.login)
                } else {
                    actor.login
                },
            },
        };
        let pull = PullResponse {
            id: self.database_id,
            number: self.number,
            html_url: self.url,
            title: self.title,
            user,
            // REST reports a merged pull request as `closed` with `merged`.
            state: if self.state == "OPEN" {
                "open"
            } else {
                "closed"
            }
            .into(),
            draft: self.is_draft,
            merged: self.merged,
            head: PullRevision {
                sha: self.head_ref_oid,
                reference: self.head_ref_name,
            },
            base: PullRevision {
                sha: self.base_ref_oid,
                reference: self.base_ref_name,
            },
        };
        let reviewers: Vec<GraphReviewer> = self
            .review_requests
            .nodes
            .into_iter()
            .flatten()
            .filter_map(|request| request.requested_reviewer)
            .collect();
        let requests = RequestedReviewers {
            users: reviewers.iter().filter_map(GraphReviewer::user).collect(),
            teams: reviewers.iter().filter_map(GraphReviewer::team).collect(),
        };
        let reviews = self
            .reviews
            .nodes
            .into_iter()
            .flatten()
            .filter_map(|review| {
                let author = review.author?;
                Some(ReviewResponse {
                    id: review.database_id?,
                    user: Viewer {
                        id: author.database_id?,
                        login: author.login,
                    },
                    state: review.state,
                    commit_id: review.commit.map(|commit| commit.oid),
                    submitted_at: review.submitted_at,
                })
            })
            .collect();
        let timeline = self
            .timeline_items
            .nodes
            .into_iter()
            .flatten()
            .map(|event| {
                let reviewer = event.requested_reviewer;
                TimelineEvent::ReviewRequested {
                    // GraphQL has no numeric ID for these events. The node ID
                    // equals the REST `node_id`, which is what is stored.
                    id: 0,
                    node_id: Some(event.id),
                    created_at: event.created_at,
                    requested_reviewer: reviewer.as_ref().and_then(GraphReviewer::user),
                    requested_team: reviewer.as_ref().and_then(GraphReviewer::team),
                }
            })
            .collect();
        Ok((pull, requests, reviews, timeline))
    }
}

struct PullResponse {
    id: i64,
    number: u64,
    html_url: String,
    title: String,
    user: Viewer,
    state: String,
    draft: bool,
    merged: bool,
    head: PullRevision,
    base: PullRevision,
}

struct PullRevision {
    sha: String,
    reference: String,
}

struct RequestedReviewers {
    users: Vec<Viewer>,
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
#[serde(tag = "event")]
enum TimelineEvent {
    #[serde(rename = "review_requested")]
    ReviewRequested {
        id: i64,
        node_id: Option<String>,
        created_at: DateTime<Utc>,
        requested_reviewer: Option<Viewer>,
        requested_team: Option<TeamId>,
    },
    #[serde(other)]
    Other,
}

impl TimelineEvent {
    fn into_review_request(
        self,
        viewer: &Viewer,
        viewer_team_ids: &[i64],
        trust_team_notification: bool,
        currently_requested_team_ids: &HashSet<i64>,
    ) -> Option<ReviewRequestEvent> {
        let Self::ReviewRequested {
            id,
            node_id,
            created_at,
            requested_reviewer,
            requested_team,
        } = self
        else {
            return None;
        };
        let (kind, requested_id) = if requested_reviewer
            .as_ref()
            .is_some_and(|requested| requested.id == viewer.id)
        {
            (ReviewRequestKind::User, viewer.id)
        } else {
            let team_id = requested_team?.id;
            let belongs_to_viewer = viewer_team_ids.contains(&team_id);
            let trusted_active_team =
                trust_team_notification && currently_requested_team_ids.contains(&team_id);
            if !belongs_to_viewer && !trusted_active_team {
                return None;
            }
            (ReviewRequestKind::Team, team_id)
        };
        Some(ReviewRequestEvent {
            event_id: node_id.unwrap_or_else(|| id.to_string()),
            created_at,
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
        let event = || TimelineEvent::ReviewRequested {
            id: 21,
            node_id: Some("RRE_team".into()),
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

    /// Write a fake `gh` that records each GraphQL request body in `calls`
    /// and answers from `script`, a shell `case` body over `$input`.
    #[cfg(unix)]
    fn fake_graphql_gh(directory: &std::path::Path, script: &str) -> PathBuf {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let path = directory.join("fake-gh");
        fs::write(
            &path,
            format!(
                "#!/bin/sh\ninput=$(cat)\nprintf '%s\\n' \"$input\" >> '{}'\n{script}\n",
                directory.join("calls").display()
            ),
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[cfg(unix)]
    #[test]
    fn enriches_pull_request_and_selects_latest_opinionated_viewer_review() {
        let temp = tempfile::tempdir().unwrap();
        let script = fake_graphql_gh(
            temp.path(),
            r#"case "$input" in
  *'after: $after'*) body='{"data":{"r":{"p":{"reviews":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"databaseId":12,"state":"CHANGES_REQUESTED","submittedAt":"2030-01-02T00:00:00Z","commit":{"oid":"old-head"},"author":{"login":"me","databaseId":1}}]}}}}}' ;;
  *) body='{"data":{"r0":{"p0":{"databaseId":99,"number":42,"url":"https://github.com/acme/widgets/pull/42","title":"Change","state":"OPEN","isDraft":false,"merged":false,"headRefOid":"head","headRefName":"feature","baseRefOid":"base","baseRefName":"main","author":{"__typename":"User","login":"author","databaseId":3},"reviewRequests":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"requestedReviewer":{"__typename":"User","databaseId":1,"login":"me"}},{"requestedReviewer":{"__typename":"Team","databaseId":77}}]},"reviews":{"pageInfo":{"hasNextPage":true,"endCursor":"c1"},"nodes":[{"databaseId":11,"state":"APPROVED","submittedAt":"2030-01-01T00:00:00Z","commit":{"oid":"older-head"},"author":{"login":"me","databaseId":1}}]},"timelineItems":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"RRE_user","createdAt":"2030-01-01T00:00:00Z","requestedReviewer":{"__typename":"User","databaseId":1,"login":"me"}},{"id":"RRE_team","createdAt":"2030-01-03T00:00:00Z","requestedReviewer":{"__typename":"Team","databaseId":77}}]}}}}}' ;;
esac
printf 'HTTP/2 200 OK\n\n%s' "$body""#,
        );

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
        assert_eq!(pull.id, 99);
        assert_eq!(pull.state, "open");
        assert_eq!(pull.author.login, "author");
        assert!(pull.directly_requested);
        assert_eq!(pull.requested_team_ids, vec![77]);
        assert_eq!(pull.head_sha, "head");
        assert_eq!(pull.base_ref, "main");
        let review = pull.latest_review.unwrap();
        assert_eq!(review.id, 12);
        assert_eq!(review.state, ReviewState::ChangesRequested);
        assert_eq!(review.commit_sha.as_deref(), Some("old-head"));
        let request = pull.latest_request.unwrap();
        assert_eq!(request.event_id, "RRE_team");
        assert_eq!(request.kind, ReviewRequestKind::Team);
        assert_eq!(request.requested_id, 77);

        let calls = std::fs::read_to_string(temp.path().join("calls")).unwrap();
        let calls: Vec<serde_json::Value> = calls
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(calls.len(), 2, "one batch query and one review page");
        assert_eq!(calls[0]["variables"]["viewer"], "me");
        assert_eq!(calls[1]["variables"]["after"], "c1");
    }

    #[cfg(unix)]
    #[test]
    fn batches_pull_requests_in_one_query_and_reports_missing_ones() {
        let temp = tempfile::tempdir().unwrap();
        let empty = r#"{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}"#;
        let pull = |number: u64, author: &str, state: &str| {
            format!(
                r#"{{"databaseId":{number}00,"number":{number},"url":"https://github.com/acme/widgets/pull/{number}","title":"PR {number}","state":"{state}","isDraft":false,"merged":{},"headRefOid":"h","headRefName":"f","baseRefOid":"b","baseRefName":"main","author":{author},"reviewRequests":{empty},"reviews":{empty},"timelineItems":{empty}}}"#,
                state == "MERGED"
            )
        };
        let body = format!(
            r#"{{"data":{{"r0":{{"p0":{},"p1":null,"p3":{}}},"r1":null}},"errors":[{{"type":"NOT_FOUND","path":["r0","p1"],"message":"Could not resolve to a PullRequest with the number of 2."}},{{"type":"NOT_FOUND","path":["r1"],"message":"Could not resolve to a Repository with the name 'other/gone'."}}]}}"#,
            pull(
                1,
                r#"{"__typename":"Bot","login":"dependabot","databaseId":49699333}"#,
                "MERGED"
            ),
            pull(4, "null", "OPEN"),
        );
        let script = fake_graphql_gh(
            temp.path(),
            &format!("printf 'HTTP/2 200 OK\\n\\n%s' '{body}'\nexit 1"),
        );

        let results = GhClient::with_executable(&script)
            .pull_request_states(
                &[
                    ("acme/widgets", 1),
                    ("acme/widgets", 2),
                    ("other/gone", 3),
                    ("acme/widgets", 4),
                ],
                &Viewer {
                    id: 1,
                    login: "me".into(),
                },
                &[],
                false,
            )
            .unwrap();
        assert_eq!(results.len(), 4);
        let merged = results[0].as_ref().unwrap();
        assert_eq!(merged.author.login, "dependabot[bot]");
        assert_eq!(merged.author.id, 49699333);
        assert_eq!(merged.state, "closed");
        assert!(merged.merged);
        assert!(results[1].as_ref().unwrap_err().is_not_found());
        assert!(results[2].as_ref().unwrap_err().is_not_found());
        let ghost = results[3].as_ref().unwrap();
        assert_eq!(
            (ghost.author.id, ghost.author.login.as_str()),
            (GHOST_USER_ID, "ghost")
        );

        let calls = std::fs::read_to_string(temp.path().join("calls")).unwrap();
        assert_eq!(calls.lines().count(), 1);
        let call: serde_json::Value = serde_json::from_str(calls.lines().next().unwrap()).unwrap();
        assert_eq!(call["variables"]["o0"], "acme");
        assert_eq!(call["variables"]["n1"], "gone");
        let query = call["query"].as_str().unwrap();
        assert!(
            !query.contains("acme"),
            "names must travel as variables: {query}"
        );
        assert!(query.contains("p3: pullRequest(number: 4)"));
    }

    #[cfg(unix)]
    #[test]
    fn graphql_rate_limit_fails_the_whole_batch() {
        let temp = tempfile::tempdir().unwrap();
        let script = fake_graphql_gh(
            temp.path(),
            r#"printf 'HTTP/2 200 OK\nX-RateLimit-Remaining: 0\nX-RateLimit-Reset: 1\n\n%s' '{"data":null,"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}'
exit 1"#,
        );
        let error = GhClient::with_executable(&script)
            .pull_request_states(
                &[("acme/widgets", 1)],
                &Viewer {
                    id: 1,
                    login: "me".into(),
                },
                &[],
                false,
            )
            .unwrap_err();
        assert!(matches!(error, GhError::Status { .. }));
        assert_eq!(error.metadata().unwrap().rate_limit_remaining, Some(0));
    }
}
