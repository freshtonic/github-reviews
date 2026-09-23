use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Viewer {
    pub id: i64,
    pub login: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositoryIdentity {
    pub id: i64,
    pub full_name: String,
    pub html_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisteredRepository {
    pub repository: RepositoryIdentity,
    pub origin_url: String,
    pub local_path: PathBuf,
    pub bootstrap_pending: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Notification {
    pub id: String,
    pub reason: String,
    pub unread: bool,
    pub updated_at: DateTime<Utc>,
    pub repository: NotificationRepository,
    pub subject: NotificationSubject,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NotificationRepository {
    pub id: i64,
    pub full_name: String,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NotificationSubject {
    pub title: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub url: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewState {
    Approved,
    ChangesRequested,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpinionatedReview {
    pub id: i64,
    pub state: ReviewState,
    pub commit_sha: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
}

/// The latest authoritative GitHub timeline event requesting this viewer (or
/// one of their teams) to review a pull request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewRequestEvent {
    /// GitHub's stable timeline-event node ID, falling back to its numeric ID
    /// for older API responses which do not expose a node ID.
    pub event_id: String,
    pub created_at: DateTime<Utc>,
    pub kind: ReviewRequestKind,
    pub requested_id: i64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewRequestKind {
    User,
    Team,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequestState {
    pub id: i64,
    pub number: u64,
    pub url: String,
    pub title: String,
    pub author: Viewer,
    pub state: String,
    pub draft: bool,
    pub merged: bool,
    pub head_sha: String,
    pub head_ref: String,
    pub base_sha: String,
    pub base_ref: String,
    pub directly_requested: bool,
    pub requested_team_ids: Vec<i64>,
    pub latest_review: Option<OpinionatedReview>,
    #[serde(default)]
    pub latest_request: Option<ReviewRequestEvent>,
    /// True when a submitted non-opinionated review after the latest request
    /// explains why GitHub removed the viewer from the active request list.
    #[serde(default)]
    pub non_opinionated_review_after_request: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActionReason {
    ReviewRequested,
    HeadChangedAfterChangesRequested,
    RerequestedAfterApproval,
}

impl ActionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReviewRequested => "review_requested",
            Self::HeadChangedAfterChangesRequested => "head_changed_after_changes_requested",
            Self::RerequestedAfterApproval => "rerequested_after_approval",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReviewEnvelope {
    pub schema_version: u8,
    pub reason: ActionReason,
    pub viewer: Viewer,
    pub repository: EnvelopeRepository,
    pub request: EnvelopeRequest,
    pub pull_request: EnvelopePullRequest,
    pub review: Option<OpinionatedReview>,
    pub notification: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnvelopeRepository {
    pub id: i64,
    pub full_name: String,
    pub local_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnvelopeRequest {
    pub kind: String,
    pub event_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnvelopePullRequest {
    pub id: i64,
    pub number: u64,
    pub url: String,
    pub title: String,
    pub author: Viewer,
    pub head_sha: String,
    pub base_sha: String,
    pub base_ref: String,
    pub head_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Actionability {
    Actionable(ActionReason),
    Dormant(&'static str),
    Inactive(&'static str),
}

pub fn classify_actionability(
    viewer: &Viewer,
    viewer_team_ids: &[i64],
    pull: &PullRequestState,
) -> Actionability {
    if pull.state != "open" || pull.merged || pull.draft {
        return Actionability::Inactive("pull request is not open and ready for review");
    }
    if pull.author.id == viewer.id {
        return Actionability::Inactive("current user authored the pull request");
    }
    let team_requested = pull
        .requested_team_ids
        .iter()
        .any(|team| viewer_team_ids.contains(team));
    let currently_requested = pull.directly_requested || team_requested;

    match &pull.latest_review {
        // Discovery verifies an active direct/team request. Once tracked, a
        // comment-only review can remove that request without satisfying the
        // opinionated-review requirement, so absence remains actionable.
        None if currently_requested || pull.non_opinionated_review_after_request => {
            Actionability::Actionable(ActionReason::ReviewRequested)
        }
        None => Actionability::Dormant("review request was withdrawn"),
        Some(review) if review.state == ReviewState::ChangesRequested => {
            if review.commit_sha.as_deref() != Some(pull.head_sha.as_str()) {
                Actionability::Actionable(ActionReason::HeadChangedAfterChangesRequested)
            } else {
                Actionability::Dormant("head has not changed since changes were requested")
            }
        }
        Some(review) if review.state == ReviewState::Approved && currently_requested => {
            if pull.latest_request.as_ref().is_some_and(|request| {
                review
                    .submitted_at
                    .is_none_or(|submitted| request.created_at > submitted)
            }) {
                Actionability::Actionable(ActionReason::RerequestedAfterApproval)
            } else {
                Actionability::Dormant("approval is newer than the request")
            }
        }
        Some(_) => Actionability::Dormant("pull request is approved"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn viewer(id: i64) -> Viewer {
        Viewer {
            id,
            login: format!("user-{id}"),
        }
    }

    fn pull() -> PullRequestState {
        PullRequestState {
            id: 1,
            number: 7,
            url: "https://github.com/acme/widgets/pull/7".into(),
            title: "Change".into(),
            author: viewer(2),
            state: "open".into(),
            draft: false,
            merged: false,
            head_sha: "head".into(),
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

    #[test]
    fn excludes_self_authored_pull_requests() {
        let mut pull = pull();
        pull.author = viewer(1);
        assert_eq!(
            classify_actionability(&viewer(1), &[], &pull),
            Actionability::Inactive("current user authored the pull request")
        );
    }

    #[test]
    fn changed_head_after_changes_requested_is_actionable() {
        let mut pull = pull();
        pull.directly_requested = false;
        pull.latest_review = Some(OpinionatedReview {
            id: 5,
            state: ReviewState::ChangesRequested,
            commit_sha: Some("old".into()),
            submitted_at: Some(Utc::now()),
        });
        assert_eq!(
            classify_actionability(&viewer(1), &[], &pull),
            Actionability::Actionable(ActionReason::HeadChangedAfterChangesRequested)
        );
    }

    #[test]
    fn comment_only_review_remains_actionable_after_github_removes_request() {
        let mut pull = pull();
        pull.directly_requested = false;
        pull.non_opinionated_review_after_request = true;
        assert_eq!(
            classify_actionability(&viewer(1), &[], &pull),
            Actionability::Actionable(ActionReason::ReviewRequested)
        );
    }

    #[test]
    fn manually_withdrawn_request_is_dormant() {
        let mut pull = pull();
        pull.directly_requested = false;
        assert_eq!(
            classify_actionability(&viewer(1), &[], &pull),
            Actionability::Dormant("review request was withdrawn")
        );
    }

    #[test]
    fn approval_requires_a_newer_active_request() {
        let submitted_at = "2026-01-02T00:00:00Z".parse().unwrap();
        let mut pull = pull();
        pull.latest_review = Some(OpinionatedReview {
            id: 5,
            state: ReviewState::Approved,
            commit_sha: Some("head".into()),
            submitted_at: Some(submitted_at),
        });
        pull.latest_request = Some(ReviewRequestEvent {
            event_id: "old-request".into(),
            created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            kind: ReviewRequestKind::User,
            requested_id: 1,
        });
        assert!(matches!(
            classify_actionability(&viewer(1), &[], &pull),
            Actionability::Dormant(_)
        ));
        // An unrelated notification update after the approval is not an
        // authoritative re-request.
        assert!(matches!(
            classify_actionability(&viewer(1), &[], &pull),
            Actionability::Dormant(_)
        ));
        pull.latest_request = Some(ReviewRequestEvent {
            event_id: "new-request".into(),
            created_at: "2026-01-03T00:00:00Z".parse().unwrap(),
            kind: ReviewRequestKind::User,
            requested_id: 1,
        });
        assert_eq!(
            classify_actionability(&viewer(1), &[], &pull),
            Actionability::Actionable(ActionReason::RerequestedAfterApproval)
        );
    }

    #[test]
    fn team_request_is_actionable_for_team_member() {
        let mut pull = pull();
        pull.directly_requested = false;
        pull.requested_team_ids = vec![99];
        assert_eq!(
            classify_actionability(&viewer(1), &[99], &pull),
            Actionability::Actionable(ActionReason::ReviewRequested)
        );
    }
}
