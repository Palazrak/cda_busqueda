import os
import sys
import tempfile
import ast
import types
import time
import unittest
from pathlib import Path
from unittest.mock import patch


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

from scheduler_config import SchedulerConfig
from scraper_executor import ScraperExecutor


class SchedulerPathResolutionTest(unittest.TestCase):
    def _write_config(self, config):
        tmpdir = tempfile.TemporaryDirectory()
        config_path = Path(tmpdir.name) / "config.yaml"
        with open(config_path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(config, fh)
        self.addCleanup(tmpdir.cleanup)
        return config_path

    def test_legacy_script_filename_resolves_to_paralelizado(self):
        config_path = self._write_config(
            {
                "scrapers": {
                    "legacy": {
                        "enabled": True,
                        "estimated_duration_sec": 30,
                        "base_interval_min": None,
                        "min_interval_min": 5,
                        "max_interval_min": 30,
                        "priority": "medium",
                        "script_filename": "paralelo_demo.py",
                        "shards": 1,
                        "shard_strategy": "generic",
                        "max_shards": 1,
                    }
                }
            }
        )

        config = SchedulerConfig(str(config_path))

        self.assertEqual(
            config.get_scraper_script_path("legacy"),
            "scripts/paralelizado/paralelo_demo.py",
        )

    def test_script_path_can_target_serial_directory(self):
        config_path = self._write_config(
            {
                "scrapers": {
                    "serial": {
                        "enabled": True,
                        "estimated_duration_sec": 30,
                        "base_interval_min": None,
                        "min_interval_min": 5,
                        "max_interval_min": 30,
                        "priority": "medium",
                        "script_path": "scripts/serial/serial_demo.py",
                        "shards": 1,
                        "shard_strategy": "generic",
                        "max_shards": 1,
                    }
                }
            }
        )

        config = SchedulerConfig(str(config_path))

        self.assertEqual(
            config.get_scraper_script_path("serial"),
            "scripts/serial/serial_demo.py",
        )

    def test_scraper_args_default_and_override(self):
        config_path = self._write_config(
            {
                "scrapers": {
                    "with_args": {
                        "enabled": True,
                        "estimated_duration_sec": 30,
                        "base_interval_min": None,
                        "min_interval_min": 5,
                        "max_interval_min": 30,
                        "priority": "medium",
                        "script_path": "scripts/serial/serial_demo.py",
                        "args": ["--max-records", "25"],
                        "shards": 1,
                        "shard_strategy": "generic",
                        "max_shards": 1,
                    },
                    "without_args": {
                        "enabled": True,
                        "estimated_duration_sec": 30,
                        "base_interval_min": None,
                        "min_interval_min": 5,
                        "max_interval_min": 30,
                        "priority": "medium",
                        "script_path": "scripts/serial/serial_other.py",
                        "shards": 1,
                        "shard_strategy": "generic",
                        "max_shards": 1,
                    },
                }
            }
        )

        config = SchedulerConfig(str(config_path))

        self.assertEqual(config.get_scraper_args("with_args"), ["--max-records", "25"])
        self.assertEqual(config.get_scraper_args("without_args"), [])


class ScraperExecutorPathTest(unittest.TestCase):
    def test_executor_runs_from_app_with_scripts_pythonpath(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app_dir = Path(tmpdir)
            script_path = app_dir / "scripts" / "serial" / "serial_demo.py"
            script_path.parent.mkdir(parents=True)
            script_path.write_text("print('ok')\n", encoding="utf-8")

            popen_calls = []

            class DummyProcess:
                pid = 12345

                def poll(self):
                    return None

            def fake_popen(cmd, **kwargs):
                popen_calls.append((cmd, kwargs))
                return DummyProcess()

            executor = ScraperExecutor(app_dir=str(app_dir), timeout_sec=900)
            with patch("subprocess.Popen", side_effect=fake_popen):
                launched = executor.execute_shards(
                    scraper_name="serial_demo",
                    script_path="scripts/serial/serial_demo.py",
                    shard_args_list=[["--max-records", "1"]],
                )

            self.assertTrue(launched)
            cmd, kwargs = popen_calls[0]
            self.assertEqual(Path(cmd[1]), script_path)
            self.assertEqual(cmd[-2:], ["--max-records", "1"])
            self.assertEqual(kwargs["cwd"], str(app_dir))
            self.assertIsNone(kwargs["stdout"])
            self.assertIsNone(kwargs["stderr"])

            pythonpath_parts = kwargs["env"]["PYTHONPATH"].split(os.pathsep)
            self.assertIn(str(app_dir / "scripts"), pythonpath_parts)
            self.assertIn(str(app_dir), pythonpath_parts)

    def test_cleanup_finished_uses_snapshot_when_active_processes_mutate(self):
        executor = ScraperExecutor(app_dir="/tmp", timeout_sec=900)

        class DoneProcess:
            pid = 12345

            def __init__(self, mutate=None):
                self._mutate = mutate
                self._mutated = False

            def poll(self):
                if self._mutate and not self._mutated:
                    self._mutated = True
                    self._mutate()
                return 0

            def communicate(self, timeout=None):
                return "", ""

        def add_active_process():
            executor.active_processes["newly_launched"] = {
                "shards": [{"process": DoneProcess(), "shard_id": 0, "pid": 999}],
                "start_time": time.time(),
                "n_shards": 1,
            }

        executor.active_processes = {
            "first_done": {
                "shards": [{"process": DoneProcess(add_active_process), "shard_id": 0, "pid": 1}],
                "start_time": time.time(),
                "n_shards": 1,
            },
            "second_done": {
                "shards": [{"process": DoneProcess(), "shard_id": 0, "pid": 2}],
                "start_time": time.time(),
                "n_shards": 1,
            },
        }
        executor.execution_stats = {
            "first_done": {"total_runs": 1, "successful_runs": 0, "failed_runs": 0, "total_duration_sec": 0.0},
            "second_done": {"total_runs": 1, "successful_runs": 0, "failed_runs": 0, "total_duration_sec": 0.0},
        }

        completed = executor.cleanup_finished()

        self.assertEqual({name for name, _, _ in completed}, {"first_done", "second_done"})
        self.assertEqual(list(executor.active_processes.keys()), ["newly_launched"])


if __name__ == "__main__":
    unittest.main()
