import datetime
import unittest

from scripts.utils import db_utils


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 1
        self.closed = False

    def execute(self, query, params):
        self.executed.append((query, params))

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class DbUtilsTest(unittest.TestCase):
    def test_make_hashid_normalizes_core_fields(self):
        first = db_utils.make_hashid(
            "0601_",
            {
                "folio": " F-001 ",
                "localizado": False,
                "nombre": "  ANA   PEREZ ",
                "edad": "12",
                "descripcion_hechos": " Calle  Principal ",
                "senas": " Lunar ",
            },
        )
        second = db_utils.make_hashid(
            "0601_",
            {
                "folio": "f-001",
                "localizado": False,
                "nombre": "ana perez",
                "edad": "12",
                "descripcion_hechos": "calle principal",
                "senas": "lunar",
            },
        )
        changed_status = db_utils.make_hashid(
            "0601_",
            {
                "folio": "f-001",
                "localizado": True,
                "nombre": "ana perez",
                "edad": "12",
                "descripcion_hechos": "calle principal",
                "senas": "lunar",
            },
        )

        self.assertEqual(first, second)
        self.assertTrue(first.startswith("0601_"))
        self.assertEqual(len(first), len("0601_") + 10)
        self.assertNotEqual(first, changed_status)

    def test_insert_records_uses_hashid_localizado_conflict_target(self):
        connection = FakeConnection()

        inserted = db_utils.insert_records(
            [
                db_utils.DesaparecidoRecord(
                    fecha_extraccion=datetime.date(2026, 6, 16),
                    url_origen="https://example.test/ficha/1",
                    localizado=False,
                    hashid="0601_1234567890",
                    datos={"nombre": "ANA PEREZ"},
                )
            ],
            connect_func=lambda: connection,
        )

        self.assertEqual(inserted, 1)
        self.assertTrue(connection.committed)
        self.assertTrue(connection.closed)
        self.assertTrue(connection.cursor_obj.closed)

        query, params = connection.cursor_obj.executed[0]
        self.assertIn("ON CONFLICT (hashid, localizado) DO NOTHING", query)
        self.assertIn("fecha_extraccion", query)
        self.assertEqual(params[1], "https://example.test/ficha/1")
        self.assertEqual(params[2], False)
        self.assertEqual(params[3], "0601_1234567890")
        self.assertEqual(params[4], '{"nombre": "ANA PEREZ"}')

    def test_build_record_defaults_localizado_and_hashid(self):
        record = db_utils.build_record(
            "1201_",
            {"nombre": "ANA PEREZ", "folio": "A-1"},
            "https://example.test/ficha/1",
        )

        self.assertFalse(record.localizado)
        self.assertFalse(record.datos["localizado"])
        self.assertTrue(record.hashid.startswith("1201_"))
        self.assertEqual(record.url_origen, "https://example.test/ficha/1")

    def test_build_record_normalizes_common_alias_fields(self):
        record = db_utils.build_record(
            "0202_",
            {
                "nombre": "ANA PEREZ",
                "reporte_num": "REP-1",
                "resumen_hechos": "Fue vista por ultima vez",
                "senas_particulares": "Lunar",
            },
            "https://example.test/ficha/1.pdf",
        )

        self.assertEqual(record.datos["folio"], "REP-1")
        self.assertEqual(record.datos["descripcion_hechos"], "Fue vista por ultima vez")
        self.assertEqual(record.datos["senas"], "Lunar")


if __name__ == "__main__":
    unittest.main()
