# Employee Assignment Service

Service to ingest employee-project assignment data from CSV into SQLite.

## Setup

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## Run

## Run

python -m assignment_service.cli \
  --csv data/employee_projects.csv \
  --init-schema

Database file:

db/app.db