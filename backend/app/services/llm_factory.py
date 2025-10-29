import os
import logging
from typing import Any, Dict
from neo4j_graphrag.llm.openai_llm import OpenAILLM, AzureOpenAILLM
from neo4j_graphrag.llm.ollama_llm import OllamaLLM
from neo4j_graphrag.llm.vertexai_llm import VertexAILLM
from neo4j_graphrag.llm.anthropic_llm import AnthropicLLM
from neo4j_graphrag.llm.cohere_llm import CohereLLM
from neo4j_graphrag.llm.mistralai_llm import MistralAILLM
from app.utils.sanitizing_llm import SanitizingLLM

# optional RateLimiter (version tolerant)
try:
    from neo4j_graphrag.llm.rate_limit import RetryRateLimitHandler
except Exception:
    try:
        from neo4j_graphrag.llm.rate_limit_handler import RetryRateLimitHandler
    except Exception:
        RetryRateLimitHandler = None

# --- Logging-Setup (respektiert LOG_LEVEL) ---
logger = logging.getLogger("llm_factory")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(_h)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

def _mp_generic() -> Dict[str, Any]:
    d: Dict[str, Any] = {}
    if "LLM_TEMPERATURE" in os.environ:
        d["temperature"] = float(os.getenv("LLM_TEMPERATURE", "0"))
    if "LLM_MAX_TOKENS" in os.environ:
        d["max_tokens"] = int(os.getenv("LLM_MAX_TOKENS", "2000"))
    logger.debug(f"_mp_generic -> {d}")
    return d

def _mp_ollama() -> Dict[str, Any]:
    opts: Dict[str, Any] = {}
    if "LLM_TEMPERATURE" in os.environ:
        opts["temperature"] = float(os.getenv("LLM_TEMPERATURE", "0"))
    if "LLM_MAX_TOKENS" in os.environ:
        opts["num_predict"] = int(os.getenv("LLM_MAX_TOKENS", "2000"))

    # wichtig: Template aus, strikter Modus
    if os.getenv("LLM_RAW", "1") != "0":
        opts["raw"] = True

    # alles NACH dem JSON kappen (z. B. “Note that ...”)
    stop = os.getenv("LLM_STOP", "").strip()
    if stop:
        opts["stop"] = [s for s in stop.split("||") if s]

    mp: Dict[str, Any] = {}
    if os.getenv("LLM_FORMAT", "").lower() == "json":
        mp["format"] = "json"

    # >>> NEU: offizielles Feld 'system' von /api/generate
    sysmsg = os.getenv("LLM_SYSTEM", "").strip()
    if sysmsg:
        mp["system"] = sysmsg

    if opts:
        mp["options"] = opts
    return mp



def get_llm():
    prov = os.getenv("LLM_PROVIDER", "").lower().strip()
    model = os.getenv("LLM_MODEL")
    if not prov:
        raise RuntimeError("LLM_PROVIDER nicht gesetzt/unterstützt")
    if not model:
        logger.warning("LLM_MODEL ist leer – der Adapter nutzt ggf. Defaults.")

    rl = (
        RetryRateLimitHandler(
            max_attempts=5, min_wait=0.5, max_wait=30, multiplier=2.0, jitter=True
        )
        if RetryRateLimitHandler
        else None
    )
    if rl:
        logger.debug("RateLimiter aktiv")

    def kw(mp: Dict[str, Any]):
        k = dict(model_name=model, model_params=mp)
        if rl:
            k["rate_limit_handler"] = rl
        return k

    logger.info(f"LLM wählen: provider={prov}, model={model}")

    if prov == "ollama":
        mp = _mp_ollama()
        llm = OllamaLLM(**kw(mp))
        logger.debug(f"OllamaLLM init mit params={mp}")
        return SanitizingLLM(llm)

    if prov == "openai":
        mp = _mp_generic()
        llm = OpenAILLM(**kw(mp))
        logger.debug(f"OpenAILLM init mit params={mp}")
        return SanitizingLLM(llm)

    if prov == "azureopenai":
        mp = _mp_generic()
        llm = AzureOpenAILLM(**kw(mp))
        logger.debug(f"AzureOpenAILLM init mit params={mp}")
        return SanitizingLLM(llm)

    if prov == "vertexai":
        mp = _mp_generic()
        if not model:
            mp.setdefault("model_name", "gemini-1.5-flash-001")
        llm = VertexAILLM(**kw(mp))
        logger.debug(f"VertexAILLM init mit params={mp}")
        return SanitizingLLM(llm)

    if prov == "anthropic":
        mp = _mp_generic()
        llm = AnthropicLLM(**kw(mp))
        logger.debug(f"AnthropicLLM init mit params={mp}")
        return SanitizingLLM(llm)

    if prov == "cohere":
        mp = _mp_generic()
        llm = CohereLLM(**kw(mp))
        logger.debug(f"CohereLLM init mit params={mp}")
        return SanitizingLLM(llm)

    if prov == "mistral":
        mp = _mp_generic()
        llm = MistralAILLM(**kw(mp))
        logger.debug(f"MistralAILLM init mit params={mp}")
        return SanitizingLLM(llm)

    raise RuntimeError("LLM_PROVIDER nicht gesetzt/unterstützt")
