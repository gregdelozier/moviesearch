import unittest

from database import MovieRepository


class FakeCursor:
    def __init__(self, documents):
        self.documents = list(documents)

    def sort(self, *args, **kwargs):
        if args and isinstance(args[0], list):
            for key, direction in reversed(args[0]):
                reverse = direction == -1
                self.documents.sort(key=lambda item: item.get(key) or "", reverse=reverse)
        elif args:
            key = args[0]
            direction = args[1] if len(args) > 1 else 1
            reverse = direction == -1
            self.documents.sort(key=lambda item: item.get(key) or "", reverse=reverse)
        return self

    def skip(self, amount):
        self.documents = self.documents[amount:]
        return self

    def limit(self, amount):
        self.documents = self.documents[:amount]
        return self

    def __iter__(self):
        return iter(self.documents)


class FakeCollection:
    def __init__(self, documents):
        self.documents = list(documents)

    def find(self, filters=None):
        filters = filters or {}
        return FakeCursor([doc for doc in self.documents if matches(doc, filters)])

    def find_one(self, filters=None):
        filters = filters or {}
        for document in self.documents:
            if matches(document, filters):
                return document
        return None


class FakeDatabase:
    def __init__(self, collections):
        self.collections = collections

    def __getitem__(self, name):
        return self.collections[name]


def matches(document, filters):
    if not filters:
        return True

    for key, value in filters.items():
        if key == "$or":
            return any(matches(document, branch) for branch in value)

        if isinstance(value, dict) and "$regex" in value:
            pattern = value["$regex"].lower()
            current = (document.get(key) or "").lower()
            if pattern not in current:
                return False
            continue

        if isinstance(value, dict) and "$in" in value:
            if document.get(key) not in value["$in"]:
                return False
            continue

        if document.get(key) != value:
            return False

    return True


class MovieRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.repository = MovieRepository(
            FakeDatabase(
                {
                    "titles": FakeCollection(
                        [
                            {
                                "_id": "tt001",
                                "primaryTitle": "Movie One",
                                "originalTitle": "Movie One",
                                "titleType": "movie",
                                "startYear": "1999",
                                "runtimeMinutes": "110",
                                "genres": "Action,Sci-Fi",
                            },
                            {
                                "_id": "tt002",
                                "primaryTitle": "Movie Two",
                                "originalTitle": "Second Movie",
                                "titleType": "short",
                                "startYear": "2002",
                                "runtimeMinutes": "20",
                                "genres": "Drama",
                            },
                        ]
                    ),
                    "ratings": FakeCollection(
                        [
                            {"_id": "tt001", "averageRating": "7.8", "numVotes": "1200"},
                        ]
                    ),
                }
            )
        )

    def test_search_titles_filters_and_attaches_rating(self):
        results, has_more = self.repository.search_titles("Movie", title_type="movie")

        self.assertEqual(len(results), 1)
        self.assertFalse(has_more)
        self.assertEqual(results[0]["id"], "tt001")
        self.assertEqual(results[0]["rating"]["average"], "7.8")
        self.assertEqual(results[0]["genres"], ["Action", "Sci-Fi"])

    def test_get_title_returns_rating_only_detail(self):
        title = self.repository.get_title("tt001")

        self.assertEqual(title["primary_title"], "Movie One")
        self.assertEqual(title["rating"]["average"], "7.8")
        self.assertEqual(title["runtime_minutes"], "110")


if __name__ == "__main__":
    unittest.main()