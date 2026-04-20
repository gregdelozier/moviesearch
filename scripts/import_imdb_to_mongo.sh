#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMPORTER="$ROOT_DIR/scripts/import_tsv_to_mongo.py"
DATA_DIR="${1:-$ROOT_DIR/data/imdb}"
DATABASE_NAME="${MONGODB_DATABASE:-imdb}"

python "$IMPORTER" "$DATA_DIR/title.basics.tsv" titles --database "$DATABASE_NAME" --id-field tconst
python "$IMPORTER" "$DATA_DIR/title.ratings.tsv" ratings --database "$DATABASE_NAME" --id-field tconst