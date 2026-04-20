from flask import Flask, abort, render_template, request

from database import MovieRepository, get_database


def create_app(repository=None):
    app = Flask(__name__)
    app.config["REPOSITORY"] = repository

    def get_repository():
        repo = app.config.get("REPOSITORY")
        if repo is None:
            repo = MovieRepository(get_database())
            app.config["REPOSITORY"] = repo
        return repo

    @app.route("/")
    def index():
        query = request.args.get("q", "")
        title_type = request.args.get("type", "movie")
        page = max(request.args.get("page", default=1, type=int), 1)
        repo = get_repository()
        results, has_more = repo.search_titles(query, title_type=title_type, page=page)
        return render_template(
            "index.html",
            query=query,
            title_type=title_type,
            results=results,
            has_more=has_more,
            page=page,
        )

    @app.route("/titles/<title_id>")
    def title_detail(title_id):
        repo = get_repository()
        title = repo.get_title(title_id)
        if title is None:
            abort(404)
        return render_template("title_detail.html", title=title)

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)