import sqlite3
from pathlib import Path

from assignment_service.schema import initialize_schema
from assignment_service.service import ingest_assignments


def test_basic_ingestion(tmp_path: Path):
    db_path = tmp_path / "test.db"
    csv_path = tmp_path / "input.csv"

    csv_path.write_text(
        "employee_id,employee_name,project_id,project_name,project_location\n"
        "1,Alice,10,Apollo,Bengaluru\n"
        "2,Bob,10,Apollo,Bengaluru\n",
        encoding="utf-8",
    )

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    initialize_schema(conn)

    report = ingest_assignments(conn, str(csv_path))
    assert report.rows_processed == 2

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM projects")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT COUNT(*) FROM employees")
    assert cur.fetchone()[0] == 2

    cur.execute("SELECT COUNT(*) FROM project_employee")
    assert cur.fetchone()[0] == 2

    conn.close()