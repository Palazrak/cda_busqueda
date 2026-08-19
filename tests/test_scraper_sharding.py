import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "serial"))
sys.path.insert(0, str(ROOT / "scripts" / "paralelizado"))
sys.path.insert(0, str(ROOT / "scripts"))

if "requests" not in sys.modules:
    sys.modules["requests"] = types.SimpleNamespace()
if "bs4" not in sys.modules:
    sys.modules["bs4"] = types.SimpleNamespace(BeautifulSoup=object)
if "urllib3" not in sys.modules:
    sys.modules["urllib3"] = types.SimpleNamespace(
        disable_warnings=lambda *args, **kwargs: None,
        exceptions=types.SimpleNamespace(InsecureRequestWarning=Warning),
    )

from serial_registro_jalisco import select_shard_states
from serial_fiscalia_queretaro import select_pages_for_shard
from paralelo_jpg_edomex import select_cards_for_shard


class ScraperShardingTest(unittest.TestCase):
    def test_jalisco_selects_state_slice_by_shard_index(self):
        estados = [("GUANAJUATO", 11), ("JALISCO", 14), ("MEXICO", 15), ("MICHOACAN", 16)]

        shard_zero = select_shard_states(estados, shard_index=0, shard_count=3)
        shard_one = select_shard_states(estados, shard_index=1, shard_count=3)
        shard_two = select_shard_states(estados, shard_index=2, shard_count=3)

        self.assertEqual(shard_zero, [("GUANAJUATO", 11), ("MICHOACAN", 16)])
        self.assertEqual(shard_one, [("JALISCO", 14)])
        self.assertEqual(shard_two, [("MEXICO", 15)])

    def test_queretaro_selects_only_requested_page_indexes(self):
        pages = [
            ("mme", "Femenino", "Menor"),
            ("mmy", "Femenino", "Mayor"),
            ("hme", "Masculino", "Menor"),
            ("hmy", "Masculino", "Mayor"),
        ]

        selected = select_pages_for_shard(pages, page_indexes="1,3")

        self.assertEqual(
            selected,
            [
                ("mmy", "Femenino", "Mayor"),
                ("hmy", "Masculino", "Mayor"),
            ],
        )

    def test_edomex_selects_card_slice_by_shard_index(self):
        cards = [{"nombre": f"persona-{i}"} for i in range(6)]

        selected = select_cards_for_shard(cards, shard_index=1, shard_count=3)

        self.assertEqual(selected, [{"nombre": "persona-1"}, {"nombre": "persona-4"}])


if __name__ == "__main__":
    unittest.main()
