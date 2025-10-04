import os, json, time, uuid
from pathlib import Path

RUNS_DIR = Path(os.getenv("RUNS_DIR", "/var/lib/llmgb/runs"))

def new_job(mode: str) -> Path:
    job_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
    jobdir = RUNS_DIR / mode / job_id
    jobdir.mkdir(parents=True, exist_ok=False)
    (jobdir/"status.json").write_text(json.dumps({"state":"running","startedAt":time.time()}), encoding="utf-8")
    return jobdir

def set_status(jobdir: Path, state: str, extra: dict | None = None):
    data = {"state": state, "updatedAt": time.time()}
    if extra: data |= extra
    (jobdir/"status.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def read_status(jobdir: Path) -> dict:
    try:
        return json.loads((jobdir/"status.json").read_text(encoding="utf-8"))
    except Exception:
        return {"state":"unknown"}

def jobdir_of(mode: str, job_id: str) -> Path:
    return RUNS_DIR / mode / job_id
