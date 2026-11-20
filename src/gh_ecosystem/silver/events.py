"""Event-specific field extraction for the silver layer."""

import json


def extract_push_event(record):
    """Extract strongly-typed PushEvent fields from a bronze event record.
    
    Returns a dict conforming to silver.push_events schema.
    """
    if record.get("event_type") != "PushEvent":
        return None

    raw_payload = record.get("payload")
    if isinstance(raw_payload, str):
        try:
            payload = json.loads(raw_payload)
        except (ValueError, TypeError):
            payload = {}
    elif isinstance(raw_payload, dict):
        payload = raw_payload
    else:
        payload = {}

    commits = payload.get("commits") or []
    commit_count = len(commits) if isinstance(commits, list) else 0

    return {
        "event_id": str(record["event_id"]),
        "event_type": "PushEvent",
        "actor_id": int(record["actor_id"]),
        "repo_id": int(record["repo_id"]),
        "org_id": int(record["org_id"]) if record.get("org_id") is not None else None,
        "created_at": record["created_at"],
        "push_id": int(payload["push_id"]) if payload.get("push_id") is not None else None,
        "size": int(payload["size"]) if payload.get("size") is not None else 0,
        "distinct_size": int(payload["distinct_size"]) if payload.get("distinct_size") is not None else 0,
        "ref": str(payload["ref"]) if payload.get("ref") is not None else None,
        "head": str(payload["head"]) if payload.get("head") is not None else None,
        "before": str(payload["before"]) if payload.get("before") is not None else None,
        "commit_count": commit_count,
    }
