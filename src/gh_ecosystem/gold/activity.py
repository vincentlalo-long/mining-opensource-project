"""Gold layer activity metrics computation."""

from collections import Counter
from gh_ecosystem.bronze import extract_event_date


def compute_event_volume_daily(silver_events):
    """Aggregate deduplicated silver events into daily event counts by type.
    
    Returns a list of dicts: {"event_date": str, "event_type": str, "event_count": int}.
    Fails on missing created_at/event_type rather than emitting 'unknown'.
    """
    counts = Counter()
    for event in silver_events:
        created_at = event.get("created_at")
        if not created_at:
            raise ValueError(f"Missing created_at in silver event: {event.get('event_id')}")
        date = extract_event_date(created_at)
        event_type = event.get("event_type")
        if not event_type:
            raise ValueError(f"Missing event_type in silver event: {event.get('event_id')}")
        counts[(date, event_type)] += 1

    results = []
    for (event_date, event_type), count in sorted(counts.items()):
        results.append({
            "event_date": event_date,
            "event_type": event_type,
            "event_count": count,
        })
    return results
