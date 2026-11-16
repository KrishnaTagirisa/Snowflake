from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

VALID_ENTITIES = {"customers", "orders", "products"}
VALID_OPERATIONS = {"INSERT", "UPDATE", "UPSERT", "DELETE"}

@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    event_id: str | None = None

def validate_event(event: dict[str, Any]) -> list[ValidationIssue]:
    issues = []
    entity = event.get("entity")
    operation = str(event.get("operation", "UPSERT")).upper()
    if entity not in VALID_ENTITIES:
        issues.append(ValidationIssue("UNKNOWN_ENTITY", "entity is not supported", event.get("event_id")))
    if operation not in VALID_OPERATIONS:
        issues.append(ValidationIssue("UNKNOWN_OPERATION", "operation is not supported", event.get("event_id")))
    if not event.get("source_pk"):
        issues.append(ValidationIssue("MISSING_SOURCE_PK", "source_pk is required", event.get("event_id")))
    if not event.get("event_id"):
        issues.append(ValidationIssue("MISSING_EVENT_ID", "event_id is required"))
    if not event.get("source_commit_ts"):
        issues.append(ValidationIssue("MISSING_COMMIT_TS", "source_commit_ts is required", event.get("event_id")))
    if operation != "DELETE" and not isinstance(event.get("payload"), dict):
        issues.append(ValidationIssue("MISSING_PAYLOAD", "payload must be an object for non-delete events", event.get("event_id")))
    return issues

def parse_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(timezone.utc)

def deduplicate_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {}
    for event in events:
        event_id = event.get("event_id")
        if not event_id:
            raise ValueError("event_id is required for deduplication")
        current = by_id.get(event_id)
        if current is None or parse_timestamp(event["source_commit_ts"]) >= parse_timestamp(current["source_commit_ts"]):
            by_id[event_id] = event
    return list(by_id.values())

def freshness_minutes(source_timestamp: str | datetime, now: str | datetime | None = None) -> float:
    now = parse_timestamp(now or datetime.now(timezone.utc))
    return (now - parse_timestamp(source_timestamp)).total_seconds() / 60

def assert_freshness(source_timestamp, max_lag_minutes, now=None):
    lag = freshness_minutes(source_timestamp, now)
    if lag > max_lag_minutes:
        raise RuntimeError(f"Freshness SLA breached: lag_minutes={lag:.2f}, threshold={max_lag_minutes}")
