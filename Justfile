# Run tests and linters
@default: test lint

# Install dependencies and test dependencies
@init:
  pipenv run pip install -e '.[test,docs]'

# Run pytest with supplied options
@test *options:
  pipenv run pytest {{options}}

# Run development server against local databases
@dev:
  DATASETTE_SECRET=1 datasette . --reload --root -p 8034

# Run linters
@lint:
  echo "Linters..."
  echo "  Black"
  pipenv run black . --check
  echo "  cog"
  pipenv run cog --check \
    README.md docs/*.md
  echo "  ruff"
  pipenv run ruff .

# Rebuild docs with cog
@cog:
  pipenv run cog -r *.md docs/*.md

# Serve live docs on localhost:8000
@docs: cog
  rm -rf docs/_build
  cd docs && pipenv run make livehtml

# Apply Black
@black:
  pipenv run black .

# Run automatic fixes
@fix: cog
  pipenv run ruff . --fix
  pipenv run black .

# Push commit if tests pass
@push: test lint
  git push
