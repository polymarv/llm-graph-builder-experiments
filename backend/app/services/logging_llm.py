from __future__ import annotations
import json, traceback
from datetime import datetime, timezone
from pathlib import Path

class LoggingLLM:
    """Leichter Wrapper um einen offiziellen LLM-Adapter (z.B. OllamaLLM).
       Loggt Prompt & rohe .content-Antwort als JSONL ins jobdir."""
    def __init__(self, base_llm, log_dir: Path):
        self._llm = base_llm
        self._logf = Path(log_dir) / "llm_raw.jsonl"

    def _write(self, obj: dict):
        obj["ts"] = datetime.now(timezone.utc).isoformat()
        with self._logf.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # Async-Aufruf (vom Extractor erwartet)
    async def ainvoke(self, prompt: str, **kwargs):
        self._write({"type": "request", "prompt": prompt, "model_params": getattr(self._llm, "model_params", {})})
        try:
            resp = await self._llm.ainvoke(prompt, **kwargs)
            content = getattr(resp, "content", None)
            self._write({"type": "response", "content": content})
            return resp
        except Exception as e:
            self._write({"type": "error", "error": str(e), "trace": traceback.format_exc()})
            raise

    # In case a sync path is used (normally not necessary, but for safety)
    def invoke(self, prompt: str, **kwargs):
        self._write({"type": "request_sync", "prompt": prompt, "model_params": getattr(self._llm, "model_params", {})})
        try:
            resp = getattr(self._llm, "invoke")(prompt, **kwargs)
            content = getattr(resp, "content", None)
            self._write({"type": "response_sync", "content": content})
            return resp
        except Exception as e:
            self._write({"type": "error_sync", "error": str(e), "trace": traceback.format_exc()})
            raise
