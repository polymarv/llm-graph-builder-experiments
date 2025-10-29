from __future__ import annotations
import json
import re
from typing import Any, Tuple

FENCE_RE = re.compile(r"```(?:json|JSON)?\s*(.*?)```", re.DOTALL)

def _extract_fenced_json(text: str) -> str | None:
    m = FENCE_RE.search(text)
    return m.group(1).strip() if m else None

def _strip_json_comments(s: str) -> str:
    """Entfernt // und /* */ Kommentare außerhalb von Strings."""
    out = []
    in_str = False
    esc = False
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if in_str:
            out.append(c)
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            i += 1
            continue
        # not in string
        if c == '"':
            in_str = True
            out.append(c)
            i += 1
            continue
        if c == "/" and i + 1 < n:
            nxt = s[i + 1]
            # line comment //
            if nxt == "/":
                i += 2
                while i < n and s[i] not in "\r\n":
                    i += 1
                continue
            # block comment /* ... */
            if nxt == "*":
                i += 2
                while i + 1 < n and not (s[i] == "*" and s[i + 1] == "/"):
                    i += 1
                i += 2 if i < n else 0
                continue
        out.append(c)
        i += 1
    return "".join(out)

def _remove_trailing_commas(s: str) -> str:
    """Entfernt trailing commas vor } oder ] außerhalb von Strings."""
    out = []
    in_str = False
    esc = False
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if in_str:
            out.append(c)
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            i += 1
            continue
        if c == '"':
            in_str = True
            out.append(c)
            i += 1
            continue
        if c == ",":
            j = i + 1
            while j < n and s[j] in " \t\r\n":
                j += 1
            if j < n and s[j] in "}]":
                # Komma droppen
                i += 1
                continue
        out.append(c)
        i += 1
    return "".join(out)

def _clean_for_json(s: str) -> str:
    """BOM/Whitespace trimmen, Kommentare & trailing commas entfernen."""
    # BOM entfernen
    if s and s[0] == "\ufeff":
        s = s.lstrip("\ufeff")
    s = s.strip()
    s = _strip_json_comments(s)
    s = _remove_trailing_commas(s)
    return s.strip()

def _try_parse_lenient(s: str) -> Tuple[Any, str]:
    """
    Versucht, s zu parsen; wenn das fehlschlägt, werden Kommentare/trailing commas entfernt.
    Gibt (obj, cleaned_str) zurück oder wirft die letzte JSON-Exception.
    """
    try:
        return json.loads(s), s.strip()
    except Exception:
        pass
    cleaned = _clean_for_json(s)
    return json.loads(cleaned), cleaned

def _extract_first_top_level(text: str) -> str | None:
    """
    Nimmt den ersten balancierten {...} oder [...]-Block außerhalb von Strings.
    Prüft NICHT sofort auf Validität; der Aufrufer parst lenient.
    """
    starts = [i for i, ch in enumerate(text) if ch in "{["]
    if not starts:
        return None
    for start in starts:
        stack = []
        in_str = False
        esc = False
        for i in range(start, len(text)):
            c = text[i]
            if in_str:
                if esc:
                    esc = False
                elif c == "\\":
                    esc = True
                elif c == '"':
                    in_str = False
            else:
                if c == '"':
                    in_str = True
                elif c in "{[":
                    stack.append(c)
                elif c in "}]":
                    if not stack:
                        return None
                    top = stack.pop()
                    if (top == "{" and c != "}") or (top == "[" and c != "]"):
                        return None
                    if not stack:
                        candidate = text[start:i + 1]
                        # no validity check here; lenient parse later
                        return candidate.strip()
    return None

def sanitize_to_json(text: str) -> Tuple[Any, str]:
    """
    Liefert (parsed_obj, cleaned_raw_json_str).
    Versuche in Reihenfolge:
      1) ```json ... ```-Fence extrahieren und lenient parsen
      2) Ersten balancierten JSON-Block extrahieren und lenient parsen
      3) Ab erster { oder [ bis vor evtl. ``` schneiden und lenient parsen
    """
    # 1) Code-Fence
    fenced = _extract_fenced_json(text)
    if fenced is not None:
        try:
            obj, clean = _try_parse_lenient(fenced)
            return obj, clean
        except Exception:
            # continue to the next heuristics
            pass

    # 2) First top-level block
    block = _extract_first_top_level(text)
    if block is not None:
        obj, clean = _try_parse_lenient(block)
        return obj, clean

    # 3) From first { or [
    idx = next((i for i, ch in enumerate(text) if ch in "{["), None)
    if idx is not None:
        candidate = text[idx:]
        candidate = candidate.split("```", 1)[0].strip()
        obj, clean = _try_parse_lenient(candidate)
        return obj, clean

    raise ValueError("Sanitizer: kein gültiger JSON-Block in der LLM-Ausgabe gefunden.")
