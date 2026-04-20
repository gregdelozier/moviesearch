import unittest

from app import create_app


class FakeRepository:
    def search_titles(self, query, title_type="movie", page=1, per_page=20):
        return (
            [
                {
                    "id": "tt001",
                    "primary_title": "Movie One",
                    "original_title": "Movie One",
                    "title_type": title_type,
                    "start_year": "1999",
                    "runtime_minutes": "110",
                    "genres": ["Action"],
                    "rating": {"average": "7.8", "votes": "1200"},
                }
            ],
            False,
        )

    def get_title(self, title_id):
        if title_id != "tt001":
            return None

        return {
            "id": "tt001",
            "primary_title": "Movie One",
            "original_title": "Movie One",
            "title_type": "movie",
            "start_year": "1999",
            "runtime_minutes": "110",
            "genres": ["Action"],
            "rating": {"average": "7.8", "votes": "1200"},
            "adult": False,
        }


class AppTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app(repository=FakeRepository()).test_client()

    def test_index_renders_search_results(self):
        response = self.app.get("/?q=Movie&type=movie")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Movie One", response.data)
        self.assertIn(b"IMDb TSV Explorer", response.data)

    def test_detail_page_renders_title(self):
        response = self.app.get("/titles/tt001")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Title Snapshot", response.data)
        self.assertIn(b"Current Dataset Scope", response.data)

    def test_detail_page_404s_for_unknown_title(self):
        response = self.app.get("/titles/tt404")

        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()