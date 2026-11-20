"""Gold layer activity metrics computation."""

from collections import Counter


def compute_event_volume_daily(silver_events):
    """Aggregate deduplicated silver events into daily event counts by type.
    
    Returns a list of dicts: {"event_date": str, "event_type": str, "event_count": int}.
    """
    counts = Counter()
    for event in silver_events:
        date = event.get("event_date")
        if not date:
            created_at = event.get("created_at", "")
            date = created_at[:10] if len(created_at) >= 10 else "unknown"
        event_type = event.get("event_type", "UnknownEvent")
        counts[(date, event_type)] += 1

    results = []
    for (event_date, event_type), count in sorted(counts.items()):
        results.append({
            "event_date": event_date,
            "event_type": event_type,
            "event_count": count,
        })
    return results
