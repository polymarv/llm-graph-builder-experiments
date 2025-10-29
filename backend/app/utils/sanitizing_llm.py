from __future__ import annotations
import os, logging, inspect
from functools import wraps
from typing import Any, Iterable, Mapping
from .json_sanitizer import sanitize_to_json

class _LLMText:
    """Small response object, compatible with call sites that expect .content/.text attributes."""
    def __init__(self, s: str):
        self.content = s
        self.text = s
    def __str__(self) -> str:
        return self.text


logger = logging.getLogger("sanitizing_llm")
logger.propagate = True
DEBUG = os.getenv("SANITIZER_DEBUG", "0") == "1"
if DEBUG:
    logger.setLevel(logging.DEBUG)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        h = logging.StreamHandler()
        h.setLevel(logging.DEBUG)
        h.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
        logger.addHandler(h)

def _flatten_content(x: Any) -> str | None:
    """
    Attempts to extract text from common response formats:
    - str
    - obj.text
    - obj.output_text
    - dict["text"] / ["content"] / OpenAI-style: ["choices"][0]["message"]["content"]
    - list/tuple of textual pieces to concatenate
    """
    # 1) direct string
    if isinstance(x, str):
        return x

    # 2) common attributes
    for attr in ("text", "output_text", "content"):
        v = getattr(x, attr, None)
        if isinstance(v, str):
            return v

    # 3) Mapping/Dict
    if isinstance(x, Mapping):
        # direct fields
        for k in ("text", "output_text", "content"):
            v = x.get(k)
            if isinstance(v, str):
                return v
        # OpenAI-/Chat-style
        try:
            choices = x.get("choices")
            if isinstance(choices, Iterable):
                first = next(iter(choices), None)
                if isinstance(first, Mapping):
                    msg = first.get("message") or first.get("delta")
                    if isinstance(msg, Mapping):
                        cont = msg.get("content")
                        if isinstance(cont, str):
                            return cont
        except Exception:
            pass

    # 4) Iterable of segments
    if isinstance(x, Iterable) and not isinstance(x, (bytes, bytearray)):
        parts = []
        for seg in x:
            seg_txt = _flatten_content(seg)
            if isinstance(seg_txt, str):
                parts.append(seg_txt)
        if parts:
            return "".join(parts)

    # 5) nothing suitable found
    return None

def _sanitize_maybe(result: Any) -> Any:
    txt = _flatten_content(result)
    if txt is None:
        if DEBUG:
            logger.debug("no textual content to sanitize (type=%s)", type(result).__name__)
        return result
    try:
        obj, raw = sanitize_to_json(txt)
        if DEBUG:
            logger.debug("sanitized: in_len=%d out_len=%d changed=%s", len(txt), len(raw), txt != raw)
            logger.debug("out_preview=%r", raw[:200])
        # IMPORTANT: Return object with .content/.text attributes, NOT str
        return _LLMText(raw)
    except Exception as e:
        preview = txt[:300].replace("\n", "\\n")
        logger.debug("sanitize_to_json FAILED: %s; type=%s; preview=%r", e, type(result).__name__, preview)
        raise


class SanitizingLLM:
    """
    Catch-all Wrapper: every method of the base LLM is intercepted.
    Text-like responses are sanitized to pure JSON, everything else passes through.
    Useful for various LLM clients that have different response formats.
    """
    def __init__(self, base_llm: Any):
        self._llm = base_llm
        if DEBUG:
            logger.debug("SanitizingLLM ENABLED for %s", type(base_llm).__name__)

    def __call__(self, *a, **k):  # most common shortcut
        return _sanitize_maybe(self._llm(*a, **k))

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._llm, name)
        if not callable(attr):
            return attr

        # sync
        if not inspect.iscoroutinefunction(attr):
            @wraps(attr)
            def wrapper(*args, **kwargs):
                if DEBUG:
                    logger.debug("calling base LLM method: %s", name)
                res = attr(*args, **kwargs)
                return _sanitize_maybe(res)
            return wrapper

        # async
        @wraps(attr)
        async def awrapper(*args, **kwargs):
            if DEBUG:
                logger.debug("async calling base LLM method: %s", name)
            res = await attr(*args, **kwargs)
            return _sanitize_maybe(res)
        return awrapper
