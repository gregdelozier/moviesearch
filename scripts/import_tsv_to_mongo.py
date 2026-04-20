#!/usr/bin/env python3

import argparse
import csv
import gzip
import os
from pathlib import Path

from pymongo import ReplaceOne
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


DEFAULT_URI_TEMPLATE = (
    "mongodb+srv://gregdelozier:<db_password>@class-demo.lp2qy8u.mongodb.net/"
    "?appName=Class-Demo"
)


def load_dotenv():
    candidate_paths = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parents[1] / ".env",
    ]

    for env_path in candidate_paths:
        if not env_path.exists():
            continue

        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue

            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
        return


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import a TSV or TSV.GZ file into a MongoDB collection."
    )
    parser.add_argument("tsv_file", help="Path to the input TSV or TSV.GZ file")
    parser.add_argument("collection", help="Target MongoDB collection name")
    parser.add_argument(
        "--database",
        default=os.environ.get("MONGODB_DATABASE", "imdb"),
        help="Target MongoDB database name (default: %(default)s)",
    )
    parser.add_argument(
        "--uri",
        default=os.environ.get("MONGODB_URI", DEFAULT_URI_TEMPLATE),
        help=(
            "MongoDB connection string. Defaults to MONGODB_URI or the Atlas "
            "template in this script."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of documents to insert per batch (default: %(default)s)",
    )
    parser.add_argument(
        "--id-field",
        help="Column name to promote to MongoDB _id for de-duplication",
    )
    parser.add_argument(
        "--id-fields",
        help="Comma-separated column names to combine into MongoDB _id",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop the target collection before importing",
    )
    parser.add_argument(
        "--null-token",
        default="\\N",
        help="TSV token to convert to null (default: %(default)s)",
    )
    return parser.parse_args()


def resolve_id_fields(args):
    if args.id_field and args.id_fields:
        raise SystemExit("Use either --id-field or --id-fields, not both.")

    if args.id_fields:
        return [field.strip() for field in args.id_fields.split(",") if field.strip()]

    if args.id_field:
        return [args.id_field]

    return []


def build_client(uri):
    resolved_uri = uri
    if "<db_password>" in resolved_uri:
        password = os.environ.get("MONGODB_PASSWORD")
        if not password:
            raise SystemExit(
                "The MongoDB URI still contains <db_password>. Set MONGODB_PASSWORD "
                "or pass --uri with the password filled in."
            )
        resolved_uri = resolved_uri.replace("<db_password>", password)

    client = MongoClient(resolved_uri, server_api=ServerApi("1"))

    try:
        client.admin.command("ping")
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as exc:
        raise SystemExit(f"Could not connect to MongoDB: {exc}") from exc

    return client


def open_tsv(path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def normalize_row(row, null_token, id_fields):
    document = {}
    for key, value in row.items():
        if value == null_token:
            document[key] = None
        else:
            document[key] = value

    if id_fields:
        missing_fields = [field for field in id_fields if field not in document]
        if missing_fields:
            missing = ", ".join(missing_fields)
            raise KeyError(f"Column(s) '{missing}' were not found in the TSV header")

        if len(id_fields) == 1:
            document["_id"] = document[id_fields[0]]
        else:
            document["_id"] = {field: document[field] for field in id_fields}

    return document


def flush_batch(collection, batch, id_fields):
    if not batch:
        return

    if id_fields:
        operations = [
            ReplaceOne({"_id": document["_id"]}, document, upsert=True)
            for document in batch
        ]
        collection.bulk_write(operations, ordered=False)
        return

    collection.insert_many(batch, ordered=False)


def main():
    load_dotenv()
    args = parse_args()
    id_fields = resolve_id_fields(args)
    file_path = Path(args.tsv_file)

    if not file_path.exists():
        raise SystemExit(f"Input file does not exist: {file_path}")

    client = build_client(args.uri)
    collection = client[args.database][args.collection]

    if args.drop:
        collection.drop()

    inserted = 0
    batch = []

    with open_tsv(file_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            batch.append(normalize_row(row, args.null_token, id_fields))
            if len(batch) >= args.batch_size:
                flush_batch(collection, batch, id_fields)
                inserted += len(batch)
                print(f"Inserted {inserted} documents...")
                batch = []

    flush_batch(collection, batch, id_fields)
    inserted += len(batch)
    print(
        f"Import complete. Loaded {inserted} documents into "
        f"{args.database}.{args.collection}."
    )


if __name__ == "__main__":
    main()