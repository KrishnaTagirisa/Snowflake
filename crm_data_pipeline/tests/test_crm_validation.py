import pytest
from datetime import datetime, timezone, timedelta
from python.crm_validation import validate_event, parse_timestamp, deduplicate_events, freshness_minutes, assert_freshness

def event(event_id="e1", ts="2026-11-16T12:00:00Z"):
    return {"entity":"customers","operation":"UPSERT","source_pk":"c1",
            "source_commit_ts":ts,"event_id":event_id,"payload":{"customer_id":"c1"}}

def test_valid_event():
    assert validate_event(event()) == []

@pytest.mark.parametrize("patch,code", [
    ({"entity":"leads"},"UNKNOWN_ENTITY"),({"operation":"MERGE"},"UNKNOWN_OPERATION"),
    ({"source_pk":None},"MISSING_SOURCE_PK"),({"event_id":None},"MISSING_EVENT_ID"),
    ({"source_commit_ts":None},"MISSING_COMMIT_TS"),({"payload":None},"MISSING_PAYLOAD")])
def test_invalid_event_contract(patch, code):
    value=event(); value.update(patch)
    assert code in {x.code for x in validate_event(value)}

def test_delete_can_have_no_payload():
    value=event(); value.update(operation="DELETE",payload=None)
    assert validate_event(value) == []

def test_timestamp_timezone_normalization():
    assert parse_timestamp("2026-11-16T13:00:00+01:00") == parse_timestamp("2026-11-16T12:00:00Z")

def test_naive_timestamp_rejected():
    with pytest.raises(ValueError): parse_timestamp("2026-11-16T12:00:00")

def test_deduplicate_keeps_latest_commit():
    rows=[event(ts="2026-11-16T12:00:00Z"),event(ts="2026-11-16T12:01:00Z")]
    assert len(deduplicate_events(rows)) == 1
    assert deduplicate_events(rows)[0]["source_commit_ts"] == "2026-11-16T12:01:00Z"

def test_missing_event_id_rejected():
    with pytest.raises(ValueError): deduplicate_events([{"source_commit_ts":"2026-11-16T12:00:00Z"}])

def test_freshness_sla():
    now=datetime(2026,11,16,12,10,tzinfo=timezone.utc)
    source=now-timedelta(minutes=5)
    assert freshness_minutes(source,now) == 5
    assert_freshness(source,10,now)
    with pytest.raises(RuntimeError): assert_freshness(source,2,now)
