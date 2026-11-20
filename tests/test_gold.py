"""Unit tests for gold layer metrics."""

import unittest

from gh_ecosystem.gold.activity import compute_event_volume_daily


class GoldTests(unittest.TestCase):
    def test_compute_event_volume_daily(self):
        events = [
            {"event_date": "2024-01-01", "event_type": "PushEvent"},
            {"event_date": "2024-01-01", "event_type": "PushEvent"},
            {"event_date": "2024-01-01", "event_type": "CreateEvent"},
            {"event_date": "2024-01-02", "event_type": "WatchEvent"},
        ]
        metrics = compute_event_volume_daily(events)
        self.assertEqual(len(metrics), 3)

        metric_dict = {(m["event_date"], m["event_type"]): m["event_count"] for m in metrics}
        self.assertEqual(metric_dict[("2024-01-01", "PushEvent")], 2)
        self.assertEqual(metric_dict[("2024-01-01", "CreateEvent")], 1)
        self.assertEqual(metric_dict[("2024-01-02", "WatchEvent")], 1)


if __name__ == "__main__":
    unittest.main()
