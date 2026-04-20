# moviesearch
Search for movies

Flask movie search app:

```bash
python -m pip install -r requirements.txt
python -m flask --app app run --debug
```

The app uses these MongoDB collections derived from the IMDb TSV schema:

- `titles` from `title.basics.tsv`
- `ratings` from `title.ratings.tsv`

This temporary version is intentionally reduced to title search plus ratings only.

The MongoDB access layer lives in `database.py`, so you can test repository behavior independently from the Flask routes.

Run the app tests with:

```bash
python -m unittest tests/test_app.py tests/test_database.py tests/test_import_tsv_to_mongo.py
```

Mongo TSV import helper:

```bash
pip install pymongo

cat > .env <<'EOF'
MONGODB_PASSWORD=your atlas db password
MONGODB_DATABASE=imdb
EOF

python scripts/import_tsv_to_mongo.py data/imdb/title.basics.tsv titles --id-field tconst
python scripts/import_tsv_to_mongo.py data/imdb/name.basics.tsv people --id-field nconst
python scripts/import_tsv_to_mongo.py data/imdb/title.ratings.tsv ratings --id-field tconst
```

The script automatically loads `.env` from the repo root. You can also override the full connection string with `--uri` or `MONGODB_URI`.

For files without a single natural key, use composite ids:

```bash
python scripts/import_tsv_to_mongo.py data/imdb/title.akas.tsv title_akas --id-fields titleId,ordering
python scripts/import_tsv_to_mongo.py data/imdb/title.principals.tsv title_principals --id-fields tconst,ordering
```

Import the whole IMDb dataset into a set of collections with:

```bash
bash scripts/import_imdb_to_mongo.sh
```

By default that script imports only `titles` and `ratings` from `data/imdb` into the `imdb` database. You can override the source directory by passing it as the first argument.

Run the importer tests with:

```bash
python -m unittest tests/test_import_tsv_to_mongo.py
```
