from pathlib import Path
import sys

def add_project_root_to_path(current_file: str, levels_up: int = 1):
    """
    Ajoute la racine du projet au PYTHONPATH pour permettre
    les imports absolus depuis 'src.helpers'
    """
    root = Path(current_file).resolve().parents[levels_up]
    sys.path.insert(0, str(root))

def get_project_root() -> Path:
    """
    Renvoie le dossier racine du projet (projet_mbfa),
    en remontant de 2 niveaux depuis ce fichier.
    """
    return Path(__file__).resolve().parents[2]
