import os
import re
from pathlib import Path

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


DEFAULT_URI_TEMPLATE = (
    "mongodb+srv://gregdelozier:<db_password>@class-demo.lp2qy8u.mongodb.net/"
    "?appName=Class-Demo"
)


def load_dotenv():
    candidate_paths = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parent / ".env",
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


def resolve_mongo_uri(uri=None):
    load_dotenv()
    resolved_uri = uri or os.environ.get("MONGODB_URI", DEFAULT_URI_TEMPLATE)

    if "<db_password>" in resolved_uri:
        password = os.environ.get("MONGODB_PASSWORD")
        if not password:
            raise RuntimeError(
                "MongoDB password is missing. Set MONGODB_PASSWORD or provide MONGODB_URI."
            )
        resolved_uri = resolved_uri.replace("<db_password>", password)

    return resolved_uri


def create_mongo_client(uri=None):
    client = MongoClient(resolve_mongo_uri(uri), server_api=ServerApi("1"))
    client.admin.command("ping")
    return client


def get_database(uri=None, database_name=None):
    load_dotenv()
    target_database = database_name or os.environ.get("MONGODB_DATABASE", "imdb")
    return create_mongo_client(uri)[target_database]


class MovieRepository:
    def __init__(self, database):
        self.database = database
        self.titles = database["titles"]
        self.ratings = database["ratings"]

    def search_titles(self, query, title_type="movie", page=1, per_page=20):
        filters = {}
        if title_type and title_type != "all":
            filters["titleType"] = title_type

        cleaned_query = (query or "").strip()
        if cleaned_query:
            pattern = re.escape(cleaned_query)
            filters["$or"] = [
                {"primaryTitle": {"$regex": pattern, "$options": "i"}},
                {"originalTitle": {"$regex": pattern, "$options": "i"}},
            ]

        skip = max(page - 1, 0) * per_page
        cursor = (
            self.titles.find(filters)
            .sort([("startYear", -1), ("primaryTitle", 1)])
            .skip(skip)
            .limit(per_page)
        )
        titles = [self._serialize_title(document) for document in cursor]

        ratings = self._ratings_by_title([title["id"] for title in titles])
        for title in titles:
            title["rating"] = ratings.get(title["id"])

        has_more = len(titles) == per_page
        return titles, has_more

    def get_title(self, title_id):
        document = self.titles.find_one({"_id": title_id})
        if not document:
            return None

        title = self._serialize_title(document)
        title["rating"] = self.get_rating(title_id)
        return title

    def get_rating(self, title_id):
        rating = self.ratings.find_one({"_id": title_id})
        if not rating:
            return None

        return {
            "average": rating.get("averageRating"),
            "votes": rating.get("numVotes"),
        }

    def _ratings_by_title(self, title_ids):
        if not title_ids:
            return {}

        cursor = self.ratings.find({"_id": {"$in": title_ids}})
        ratings = {}
        for rating in cursor:
            ratings[rating["_id"]] = {
                "average": rating.get("averageRating"),
                "votes": rating.get("numVotes"),
            }
        return ratings

    @staticmethod
    def _serialize_title(document):
        genres = document.get("genres")
        if genres and genres != "\\N":
            genre_list = [genre.strip() for genre in genres.split(",") if genre.strip()]
        else:
            genre_list = []

        return {
            "id": document.get("_id"),
            "primary_title": document.get("primaryTitle"),
            "original_title": document.get("originalTitle"),
            "title_type": document.get("titleType"),
            "adult": document.get("isAdult") == "1",
            "start_year": document.get("startYear"),
            "end_year": document.get("endYear"),
            "runtime_minutes": document.get("runtimeMinutes"),
            "genres": genre_list,
        }