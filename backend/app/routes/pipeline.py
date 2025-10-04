import asyncio, os
from fastapi import APIRouter, HTTPException
from pathlib import Path
from app.models.pipeline_models import RunRequest, RunResponse, StatusResponse
from app.services.files import resolve_upload_file
from app.services.jobs import new_job, set_status, read_status, jobdir_of
from app.services.llm_factory import get_llm

# LLM-Extraktor (offizieller Builder-Adapter)
from neo4j_graphrag.experimental.components.schema import SchemaFromTextExtractor

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

@router.post("/run", response_model=RunResponse)
def run_pipeline(req: RunRequest):
    mode = req.mode.lower()
    if mode not in {"auto","hybrid"}:
        raise HTTPException(status_code=400, detail="mode muss 'auto' oder 'hybrid' sein")

    # Jobordner anlegen
    jobdir = new_job(mode)
    try:
        # 1) Eingabedatei holen (erste Quelle reicht für strict Schema-only)
        if not req.sources:
            raise ValueError("sources leer")
        src_path = resolve_upload_file(req.sources[0].file_id)

        sample = "Eine Person hat einen Namen und arbeitet für eine Firma."
        try:
            sample = src_path.read_text(encoding="utf-8", errors="ignore")[:4000]
        except Exception:
            pass

        # 2) LLM + Schemaextraktion (JSON erzwingen)
        llm = get_llm()
        # JSON-Mode & knappe Limits direkt am Adapter setzen
        if hasattr(llm, "model_params") and isinstance(getattr(llm, "model_params"), dict):
            llm.model_params.update({"format": "json", "temperature": 0, "max_tokens": 256})

        try:
            extractor = SchemaFromTextExtractor(llm=llm, strict_json=True)
        except TypeError:
            extractor = SchemaFromTextExtractor(llm=llm)

        schema = asyncio.run(extractor.run(text=sample))


        # 3) Artefakte ablegen
        (jobdir/"schema.shex").write_text(schema.model_dump_shex(), encoding="utf-8")
        (jobdir/"graph.ttl").write_text("# (Platzhalter) – Writer folgt", encoding="utf-8")
        (jobdir/"shacl_report.json").write_text('{"note":"kein Resolver/Writer – Strict Schema only"}', encoding="utf-8")

        set_status(jobdir, "done", {"message":"schema generated"})
        return RunResponse(job_id=jobdir.name, state="done")

    except Exception as e:
        set_status(jobdir, "error", {"error": str(e)})
        raise HTTPException(status_code=500, detail=f"pipeline failed: {e}")

@router.get("/status/{mode}/{job_id}", response_model=StatusResponse)
def pipeline_status(mode: str, job_id: str):
    jobdir = jobdir_of(mode, job_id)
    if not jobdir.exists():
        raise HTTPException(status_code=404, detail="job not found")
    st = read_status(jobdir)
    return StatusResponse(job_id=job_id, state=st.get("state","unknown"), details=st)
