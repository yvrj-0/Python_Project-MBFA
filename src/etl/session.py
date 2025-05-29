
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

HERE     = Path(__file__).resolve().parent
PROJECT  = HERE.parent
DATA_DIR = PROJECT / "data"
DATA_DIR.mkdir(exist_ok=True)

print(f"[DEBUG] __file__       = {__file__}")
print(f"[DEBUG] HERE           = {HERE}")
print(f"[DEBUG] PROJECT        = {PROJECT}")
print(f"[DEBUG] DATA_DIR       = {DATA_DIR.resolve()}")

DB_PATH = DATA_DIR / "mbfa.db"
ENGINE  = create_engine(f"sqlite:///{DB_PATH}", future=True)
Session = sessionmaker(bind=ENGINE, future=True)
Base    = declarative_base()

def init_db():
    import etl.models
    print("[DEBUG] Création des tables en base si nécessaire…")
    Base.metadata.create_all(ENGINE)
    print("[DEBUG] Tables prêtes.")