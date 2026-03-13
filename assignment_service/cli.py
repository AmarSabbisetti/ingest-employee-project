import argparse
import logging

from assignment_service.config import Settings, DB_PATH
from assignment_service.database import get_connection
from assignment_service.logging_config import configure_logging
from assignment_service.schema import initialize_schema
from assignment_service.service import ingest_assignments

logger = logging.getLogger("assignment_service.cli")


def parse_args() -> Settings:
    parser = argparse.ArgumentParser(description="Employee Assignment Ingestion Service")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--init-schema", action="store_true")
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()
    configure_logging(args.log_level)

    return Settings(
        csv_path=args.csv,
        init_schema=args.init_schema,
    )


def main() -> None:
    settings = parse_args()

    with get_connection() as conn:
        if settings.init_schema:
            logger.info("Initializing database schema at %s", DB_PATH)
            initialize_schema(conn)

        logger.info("Starting ingestion using DB: %s", DB_PATH)
        ingest_assignments(conn, settings.csv_path)
        logger.info("Finished ingestion.")


if __name__ == "__main__":
    main()