"""Event-specific field extraction for the silver layer."""

import json


def extract_push_event(record):
    """Extract strongly-typed PushEvent fields from a bronze event record.
    
    Returns a dict conforming to silver.push_events schema.
    Validates field types rather than silently coercing or defaulting.
    """
    if record.get("event_type") != "PushEvent":
        return None

    raw_payload = record.get("payload")
    if isinstance(raw_payload, str):
        payload = json.loads(raw_payload)
    elif isinstance(raw_payload, dict):
        payload = raw_payload
    else:
        raise ValueError(f"PushEvent payload must be a JSON object or string, got {type(raw_payload).__name__}")

    commits = payload.get("commits")
    if commits is not None and not isinstance(commits, list):
        raise ValueError(f"PushEvent commits must be a list or null, got {type(commits).__name__}")
    commit_count = len(commits) if isinstance(commits, list) else None

    size = payload.get("size")
    if size is not None and type(size) is not int:
        raise ValueError(f"PushEvent size must be an integer or null, got {type(size).__name__}")

    distinct_size = payload.get("distinct_size")
    if distinct_size is not None and type(distinct_size) is not int:
        raise ValueError(f"PushEvent distinct_size must be an integer or null, got {type(distinct_size).__name__}")

    push_id = payload.get("push_id")
    if push_id is not None and type(push_id) is not int:
        raise ValueError(f"PushEvent push_id must be an integer or null, got {type(push_id).__name__}")

    return {
        "event_id": str(record["event_id"]),
        "event_type": "PushEvent",
        "actor_id": int(record["actor_id"]),
        "repo_id": int(record["repo_id"]),
        "org_id": int(record["org_id"]) if record.get("org_id") is not None else None,
        "created_at": record["created_at"],
        "push_id": push_id,
        "size": size,
        "distinct_size": distinct_size,
        "ref": str(payload["ref"]) if payload.get("ref") is not None else None,
        "head": str(payload["head"]) if payload.get("head") is not None else None,
        "before": str(payload["before"]) if payload.get("before") is not None else None,
        "commit_count": commit_count,
    }
