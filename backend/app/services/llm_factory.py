import os
from typing import Any, Dict
from neo4j_graphrag.llm.openai_llm import OpenAILLM, AzureOpenAILLM
from neo4j_graphrag.llm.ollama_llm import OllamaLLM
from neo4j_graphrag.llm.vertexai_llm import VertexAILLM
from neo4j_graphrag.llm.anthropic_llm import AnthropicLLM
from neo4j_graphrag.llm.cohere_llm import CohereLLM
from neo4j_graphrag.llm.mistralai_llm import MistralAILLM

# RateLimiter ist nicht in allen Versionen identisch – fallback-tolerant importieren
try:
    from neo4j_graphrag.llm.rate_limit import RetryRateLimitHandler
except Exception:
    try:
        from neo4j_graphrag.llm.rate_limit_handler import RetryRateLimitHandler
    except Exception:
        RetryRateLimitHandler = None  # notfalls ohne Rate-Limit-Handler fahren

def _mp() -> Dict[str, Any]:
    d: Dict[str, Any] = {}
    if "LLM_TEMPERATURE" in os.environ:
        d["temperature"] = float(os.getenv("LLM_TEMPERATURE", "0"))
    if "LLM_MAX_TOKENS" in os.environ:
        d["max_tokens"] = int(os.getenv("LLM_MAX_TOKENS", "2000"))
    # JSON-Mode für Ollama
    if os.getenv("LLM_FORMAT", "").lower() == "json":
        d["format"] = "json"   # Ollama gibt reines JSON zurück
    return d

def get_llm():
    prov = os.getenv("LLM_PROVIDER", "").lower().strip()
    model = os.getenv("LLM_MODEL")
    mp = _mp()
    rl = RetryRateLimitHandler(max_attempts=5, min_wait=0.5, max_wait=30, multiplier=2.0, jitter=True) if RetryRateLimitHandler else None

    def kw():
        # rate_limit_handler nur übergeben, wenn vorhanden (manche Adapter kennen den Param nicht)
        return dict(model_name=model, model_params=mp, rate_limit_handler=rl) if rl else dict(model_name=model, model_params=mp)

    if prov == "ollama":      return OllamaLLM(**kw())
    if prov == "openai":      return OpenAILLM(**kw())
    if prov == "azureopenai": return AzureOpenAILLM(**kw())
    if prov == "vertexai":
        if not model:
            mp.setdefault("model_name", "gemini-1.5-flash-001")
        return VertexAILLM(**kw())
    if prov == "anthropic":   return AnthropicLLM(**kw())
    if prov == "cohere":      return CohereLLM(**kw())
    if prov == "mistral":     return MistralAILLM(**kw())

    raise RuntimeError("LLM_PROVIDER nicht gesetzt/unterstützt")
