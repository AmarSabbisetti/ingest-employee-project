from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_DIR = BASE_DIR / "db"
DB_PATH = DB_DIR / "app.db"


@dataclass(frozen=True)
class Settings:
    csv_path: str
    init_schema: bool = False