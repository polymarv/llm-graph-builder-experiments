import os
from pathlib import Path

UPLOADS_DIR = Path(os.getenv("UPLOADS_DIR", "/var/lib/llmgb/uploads"))

def resolve_upload_file(file_id: str) -> Path:
    # erwartet "sha256:<hex>"
    if not file_id.startswith("sha256:"):
        raise ValueError("file_id muss mit 'sha256:' beginnen")
    sha = file_id.split(":",1)[1]
    if len(sha) < 4:
        raise ValueError("sha256 zu kurz")
    p = UPLOADS_DIR / sha[0:2] / sha[2:4] / f"{sha}.txt"
    if not p.exists():
        raise FileNotFoundError(f"Upload nicht gefunden: {p}")
    return p
