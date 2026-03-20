from pathlib import Path

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


def load_prompt(name: str) -> str:
    """Carrega prompt do arquivo prompts/{name}.txt"""
    path = PROMPTS_DIR / f"{name}.txt"
    return path.read_text(encoding="utf-8")
