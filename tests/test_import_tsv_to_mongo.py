import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "import_tsv_to_mongo.py"


class FakeReplaceOne:
    def __init__(self, filter_document, replacement, upsert=False):
        self.filter_document = filter_document
        self.replacement = replacement
        self.upsert = upsert


class FakeMongoClient:
    def __init__(self, *args, **kwargs):
        self.admin = types.SimpleNamespace(command=lambda _name: {"ok": 1})


class FakeServerApi:
    def __init__(self, version):
        self.version = version


def load_module():
    pymongo_module = types.ModuleType("pymongo")
    pymongo_module.ReplaceOne = FakeReplaceOne

    pymongo_mongo_client_module = types.ModuleType("pymongo.mongo_client")
    pymongo_mongo_client_module.MongoClient = FakeMongoClient

    pymongo_server_api_module = types.ModuleType("pymongo.server_api")
    pymongo_server_api_module.ServerApi = FakeServerApi

    with patch.dict(
        sys.modules,
        {
            "pymongo": pymongo_module,
            "pymongo.mongo_client": pymongo_mongo_client_module,
            "pymongo.server_api": pymongo_server_api_module,
        },
    ):
        spec = importlib.util.spec_from_file_location("import_tsv_to_mongo", SCRIPT_PATH)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module


class FakeCollection:
    def __init__(self):
        self.bulk_writes = []
        self.inserts = []
        self.dropped = False

    def bulk_write(self, operations, ordered=False):
        self.bulk_writes.append((operations, ordered))

    def insert_many(self, documents, ordered=False):
        self.inserts.append((documents, ordered))

    def drop(self):
        self.dropped = True


class FakeDatabase:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, collection_name):
        if collection_name not in self.collections:
            self.collections[collection_name] = FakeCollection()
        return self.collections[collection_name]


class FakeClient:
    def __init__(self):
        self.databases = {}

    def __getitem__(self, database_name):
        if database_name not in self.databases:
            self.databases[database_name] = FakeDatabase()
        return self.databases[database_name]


class ImportTsvToMongoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_module()

    def test_normalize_row_maps_nulls_and_id_field(self):
        row = {
            "tconst": "tt123",
            "primaryTitle": "Example Movie",
            "runtimeMinutes": "\\N",
        }

        document = self.module.normalize_row(row, "\\N", ["tconst"])

        self.assertEqual(document["_id"], "tt123")
        self.assertEqual(document["primaryTitle"], "Example Movie")
        self.assertIsNone(document["runtimeMinutes"])

    def test_normalize_row_builds_composite_id(self):
        row = {
            "titleId": "tt123",
            "ordering": "2",
            "title": "Example Alias",
        }

        document = self.module.normalize_row(row, "\\N", ["titleId", "ordering"])

        self.assertEqual(document["_id"], {"titleId": "tt123", "ordering": "2"})

    def test_main_imports_tsv_using_bulk_upserts(self):
        fake_client = FakeClient()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "titles.tsv"
            tsv_path.write_text(
                "tconst\tprimaryTitle\truntimeMinutes\n"
                "tt001\tMovie One\t90\n"
                "tt002\tMovie Two\t\\N\n",
                encoding="utf-8",
            )

            argv = [
                "import_tsv_to_mongo.py",
                str(tsv_path),
                "titles",
                "--database",
                "imdb_test",
                "--id-field",
                "tconst",
                "--batch-size",
                "1",
            ]

            with patch.object(self.module, "load_dotenv"), patch.object(
                self.module, "build_client", return_value=fake_client
            ), patch.object(sys, "argv", argv):
                self.module.main()

        collection = fake_client["imdb_test"]["titles"]
        self.assertEqual(len(collection.bulk_writes), 2)

        first_batch, first_ordered = collection.bulk_writes[0]
        second_batch, second_ordered = collection.bulk_writes[1]

        self.assertFalse(first_ordered)
        self.assertFalse(second_ordered)
        self.assertEqual(first_batch[0].filter_document, {"_id": "tt001"})
        self.assertEqual(first_batch[0].replacement["primaryTitle"], "Movie One")
        self.assertEqual(second_batch[0].filter_document, {"_id": "tt002"})
        self.assertIsNone(second_batch[0].replacement["runtimeMinutes"])

    def test_main_imports_tsv_using_composite_ids(self):
        fake_client = FakeClient()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "akas.tsv"
            tsv_path.write_text(
                "titleId\tordering\ttitle\n"
                "tt001\t1\tAlias One\n"
                "tt001\t2\tAlias Two\n",
                encoding="utf-8",
            )

            argv = [
                "import_tsv_to_mongo.py",
                str(tsv_path),
                "title_akas",
                "--database",
                "imdb_test",
                "--id-fields",
                "titleId,ordering",
                "--batch-size",
                "2",
            ]

            with patch.object(self.module, "load_dotenv"), patch.object(
                self.module, "build_client", return_value=fake_client
            ), patch.object(sys, "argv", argv):
                self.module.main()

        collection = fake_client["imdb_test"]["title_akas"]
        self.assertEqual(len(collection.bulk_writes), 1)

        batch, ordered = collection.bulk_writes[0]
        self.assertFalse(ordered)
        self.assertEqual(
            batch[0].filter_document,
            {"_id": {"titleId": "tt001", "ordering": "1"}},
        )
        self.assertEqual(
            batch[1].replacement["_id"],
            {"titleId": "tt001", "ordering": "2"},
        )


if __name__ == "__main__":
    unittest.main()