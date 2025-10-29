import asyncio, os
from fastapi import APIRouter, HTTPException
from pathlib import Path
from app.models.pipeline_models import RunRequest, RunResponse, StatusResponse
from app.services.files import resolve_upload_file
from app.services.jobs import new_job, set_status, read_status, jobdir_of
from app.services.llm_factory import get_llm
from app.services.logging_llm import LoggingLLM
import json

# Using official SchemaFromTextExtractor
from neo4j_graphrag.experimental.components.schema import SchemaFromTextExtractor

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

@router.post("/run", response_model=RunResponse)
def run_pipeline(req: RunRequest):
    mode = req.mode.lower()
    # get valid mode
    if mode not in {"auto", "hybrid"}:
        raise HTTPException(status_code=400, detail="mode must be 'auto' or 'hybrid'")

    jobdir = new_job(mode)
    try:
        # get input text
        if not req.sources:
            raise ValueError("sources empt")
        src_path = resolve_upload_file(req.sources[0].file_id)

        sample = "Eine Person hat einen Namen und arbeitet für eine Firma."
        try:
            sample = src_path.read_text(encoding="utf-8", errors="ignore")[:4000]
        except Exception:
            pass
        # ---- LLM Init
        llm = get_llm()

        # Enable logging
        if os.getenv("LOG_LLM", "1") != "0":
            llm = LoggingLLM(llm, log_dir=jobdir)

        try:
            extractor = SchemaFromTextExtractor(llm=llm, strict_json=True)
        except TypeError:
            extractor = SchemaFromTextExtractor(llm=llm)

        # ---- Extraction
        schema = asyncio.run(extractor.run(text=sample))

        # ---- STRICT OFFICIAL ONLY -> unfortunately no ShEx export available so let's go with export to json
        type_name = type(schema).__name__
        available = sorted(a for a in dir(schema) if not a.startswith("_"))

        # 1) OFFICIAL JSON-Export (only Pydantic/Lib-APIs)
        if hasattr(schema, "store_as_json") and callable(getattr(schema, "store_as_json")):
            schema.store_as_json(str(jobdir / "schema.json"))
        elif hasattr(schema, "model_dump_json") and callable(getattr(schema, "model_dump_json")):
            (jobdir / "schema.json").write_text(
                schema.model_dump_json(indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        elif hasattr(schema, "model_dump") and callable(getattr(schema, "model_dump")):
            (jobdir / "schema.json").write_text(
                json.dumps(schema.model_dump(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        else:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Official JSON-Export not available. "
                    f"Schema-Type={type_name}. Expected: store_as_json(), model_dump_json() or model_dump(). "
                    f"Attributes: {available}"
                ),
            )

        # 2) ShEx-Export only, if officially available
        if hasattr(schema, "to_shex") and callable(getattr(schema, "to_shex")):
            shex_text = schema.to_shex()
        elif hasattr(schema, "model_dump_shex") and callable(getattr(schema, "model_dump_shex")):
            shex_text = schema.model_dump_shex()
        elif hasattr(schema, "as_shex") and callable(getattr(schema, "as_shex")):
            shex_text = schema.as_shex()
        else:
            shex_text = None  # no official ShEx-API in this build

        # ShEx-File only write, if officially produced
        if shex_text is not None:
            (jobdir / "schema.shex").write_text(shex_text, encoding="utf-8")

        # Other artefacts
        # (jobdir / "graph.ttl").write_text("# (Placeholder) – Writer soon..", encoding="utf-8")
        # (jobdir / "shacl_report.json").write_text(
        #     '{"note":"no Resolver/Writer – Strict Schema only"}',
        #     encoding="utf-8",
        # )

        note = "schema.shex not generated: no official ShEx-Export-API available in this build."
        set_status(jobdir, "done", {"message": "schema generated", "notice": note})

        return RunResponse(job_id=jobdir.name, state="done")



    except ValueError as e:
        # typically from Sanitizer -> 422
        set_status(jobdir, "error", {"error": str(e)})
        raise HTTPException(status_code=422, detail=f"LLM output not pure JSON: {e}")
    except Exception as e:
        set_status(jobdir, "error", {"error": str(e)})
        raise HTTPException(status_code=500, detail=f"pipeline failed: {e}")

@router.get("/status/{mode}/{job_id}", response_model=StatusResponse)
def pipeline_status(mode: str, job_id: str):
    jobdir = jobdir_of(mode, job_id)
    if not jobdir.exists():
        raise HTTPException(status_code=404, detail="job not found")
    st = read_status(jobdir)
    return StatusResponse(job_id=job_id, state=st.get("state", "unknown"), details=st)
