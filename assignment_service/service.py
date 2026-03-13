import csv
import logging
import sqlite3
from dataclasses import dataclass

logger = logging.getLogger("assignment_service")


@dataclass
class IngestionReport:
    rows_processed: int = 0
    projects_created: int = 0
    employees_created: int = 0
    assignments_created: int = 0


REQUIRED_COLUMNS = {
    "employee_id",
    "employee_name",
    "project_id",
    "project_name",
    "project_location",
}


def ingest_assignments(conn: sqlite3.Connection, csv_path: str) -> IngestionReport:
    report = IngestionReport()
    cursor = conn.cursor()

    with open(csv_path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError("CSV header is missing.")

        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        for row in reader:
            report.rows_processed += 1

            employee_id = int(row["employee_id"])
            employee_name = row["employee_name"].strip()
            project_id = int(row["project_id"])
            project_name = row["project_name"].strip()
            project_location = row["project_location"].strip()

            # Ensure project exists
            cursor.execute("SELECT 1 FROM projects WHERE id = ?", (project_id,))
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO projects (id, name, location) VALUES (?, ?, ?)",
                    (project_id, project_name, project_location),
                )
                report.projects_created += 1

            # Ensure employee exists
            cursor.execute("SELECT 1 FROM employees WHERE id = ?", (employee_id,))
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO employees (id, name) VALUES (?, ?)",
                    (employee_id, employee_name),
                )
                report.employees_created += 1

            # Enforce single project per employee
            cursor.execute(
                "SELECT project_id FROM project_employee WHERE employee_id = ?",
                (employee_id,),
            )
            existing = cursor.fetchone()

            if existing is None:
                cursor.execute(
                    "INSERT INTO project_employee (project_id, employee_id) VALUES (?, ?)",
                    (project_id, employee_id),
                )
                report.assignments_created += 1
            else:
                existing_project_id = existing[0]
                if existing_project_id != project_id:
                    raise ValueError(
                        f"Employee {employee_id} already assigned to project {existing_project_id}"
                    )

            # Per-row commit (intentionally inefficient baseline)
            conn.commit()

    logger.info(
        "Ingestion completed | rows=%d projects=%d employees=%d assignments=%d",
        report.rows_processed,
        report.projects_created,
        report.employees_created,
        report.assignments_created,
    )

    return report