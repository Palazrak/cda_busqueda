import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scheduler"))

from shard_manager import AMBER_NACIONAL_STATES, ShardManager


class ShardManagerTest(unittest.TestCase):
    def test_states_strategy_splits_nacional_into_three_disjoint_shards(self):
        manager = ShardManager()

        shard_args = manager.build_shard_args("amber_nacional", "states", 3)

        shard_states = [
            [int(value) for value in args[1].split(",")]
            for args in shard_args
        ]
        flattened = [state for chunk in shard_states for state in chunk]

        self.assertEqual(len(shard_args), 3)
        self.assertEqual(sorted(flattened), sorted(AMBER_NACIONAL_STATES))
        self.assertEqual(len(flattened), len(set(flattened)))

    def test_jalisco_states_uses_dynamic_shard_index_args(self):
        manager = ShardManager()

        shard_args = manager.build_shard_args("serial_registro_jalisco", "jalisco_states", 3)

        self.assertEqual(
            shard_args,
            [
                ["--shard-index", "0", "--shard-count", "3"],
                ["--shard-index", "1", "--shard-count", "3"],
                ["--shard-index", "2", "--shard-count", "3"],
            ],
        )

    def test_queretaro_pages_splits_four_source_pages(self):
        manager = ShardManager()

        shard_args = manager.build_shard_args("serial_fiscalia_queretaro", "queretaro_pages", 3)

        self.assertEqual(
            shard_args,
            [
                ["--page-indexes", "0,1"],
                ["--page-indexes", "2,3"],
            ],
        )

    def test_generic_slice_uses_standard_shard_args(self):
        manager = ShardManager()

        shard_args = manager.build_shard_args("paralelo_jpg_edomex", "generic_slice", 2)

        self.assertEqual(
            shard_args,
            [
                ["--shard-index", "0", "--shard-count", "2"],
                ["--shard-index", "1", "--shard-count", "2"],
            ],
        )


if __name__ == "__main__":
    unittest.main()
