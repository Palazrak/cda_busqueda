import sys
import tempfile
import ast
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scheduler"))

try:
    import yaml
except ImportError:
    yaml = types.SimpleNamespace(
        safe_dump=lambda config, fh: fh.write(repr(config)),
        safe_load=lambda fh: ast.literal_eval(fh.read()),
        YAMLError=Exception,
    )
    sys.modules["yaml"] = yaml

from pending_queue import PendingQueue
from scheduler_config import SchedulerConfig


class PendingQueuePriorityTest(unittest.TestCase):
    def test_dequeue_best_prefers_higher_score_without_fifo_blocking(self):
        queue = PendingQueue()
        queue.enqueue("slow_low", enqueued_at=0.0)
        queue.enqueue("fast_high", enqueued_at=10.0)

        self.assertFalse(queue.enqueue("fast_high", enqueued_at=20.0))

        picked = queue.dequeue_best(
            free_slots=1,
            required_slots=lambda name: 1,
            score_func=lambda name, age_sec: {"slow_low": 10, "fast_high": 50}[name],
            now=20.0,
        )

        self.assertEqual(picked, "fast_high")
        self.assertEqual(queue.peek_all(), ["slow_low"])

    def test_dequeue_best_skips_high_score_job_that_does_not_fit(self):
        queue = PendingQueue()
        queue.enqueue("large_high", enqueued_at=0.0)
        queue.enqueue("small_medium", enqueued_at=1.0)

        picked = queue.dequeue_best(
            free_slots=1,
            required_slots=lambda name: {"large_high": 3, "small_medium": 1}[name],
            score_func=lambda name, age_sec: {"large_high": 100, "small_medium": 50}[name],
            now=10.0,
        )

        self.assertEqual(picked, "small_medium")
        self.assertEqual(queue.peek_all(), ["large_high"])

    def test_dequeue_best_keeps_blocked_items_in_queue(self):
        queue = PendingQueue()
        queue.enqueue("running_job", enqueued_at=0.0)
        queue.enqueue("ready_job", enqueued_at=1.0)

        picked = queue.dequeue_best(
            free_slots=1,
            required_slots=lambda name: None if name == "running_job" else 1,
            score_func=lambda name, age_sec: 100,
            now=10.0,
        )

        self.assertEqual(picked, "ready_job")
        self.assertEqual(queue.peek_all(), ["running_job"])


class SchedulerPriorityConfigTest(unittest.TestCase):
    def _write_config(self, config):
        tmpdir = tempfile.TemporaryDirectory()
        config_path = Path(tmpdir.name) / "config.yaml"
        with open(config_path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(config, fh)
        self.addCleanup(tmpdir.cleanup)
        return config_path

    def test_priority_config_maps_legacy_priority_and_estimates(self):
        config_path = self._write_config(
            {
                "scrapers": {
                    "legacy_high": {
                        "enabled": True,
                        "estimated_duration_sec": 240,
                        "base_interval_min": None,
                        "min_interval_min": 1.5,
                        "max_interval_min": 30,
                        "priority": "high",
                        "record_count_estimate": 9847,
                        "script_path": "scripts/serial/serial_demo.py",
                        "shards": 1,
                        "shard_strategy": "generic",
                        "max_shards": 1,
                    }
                },
                "advanced": {
                    "priority_queue": {
                        "starvation_bonus_per_min": 4.0,
                        "max_age_bonus": 80.0,
                    }
                },
            }
        )

        cfg = SchedulerConfig(str(config_path)).get_priority_config("legacy_high")

        self.assertEqual(cfg["tier"], "high")
        self.assertEqual(cfg["weight"], 75)
        self.assertEqual(cfg["record_count_estimate"], 9847)
        self.assertEqual(cfg["starvation_bonus_per_min"], 4.0)
        self.assertEqual(cfg["max_age_bonus"], 80.0)


if __name__ == "__main__":
    unittest.main()
