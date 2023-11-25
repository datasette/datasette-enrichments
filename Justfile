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
  echo "  ruff"
  pipenv run ruff .


# Serve live docs on localhost:8000
@docs:
  rm -rf docs/_build
  cd docs && pipenv run make livehtml

# Apply Black
@black:
  pipenv run black .

# Run automatic fixes
@fix:
  pipenv run ruff . --fix
  pipenv run black .

# Push commit if tests pass
@push: test lint
  git push
