#!/usr/bin/env bash
set -euo pipefail

#############################################
# run_schema_snapshot.sh
#  - Zieht WDQS (Wikidata) für Filme 2010–2014,
#  - friert Ergebnisse als Snapshot ein (Queries, RAW, kombinierte Textdatei, artifacts.csv),
#  - lädt kombinierte Textdatei inhaltsadressiert nach /var/lib/llmgb/uploads/,
#  - triggert einen einzigen Schema-Run gegen die API für ein umfassenderes Schema.
#
# Flags:
#   --use <DATE>    : vorhandenen Snapshot (DATE=YYYY-MM-DD) wiederverwenden,
#                     Fetch wird übersprungen; nur Upload+Run
#   --fetch-only    : nur Snapshot holen/einfrieren (kein Run)
#   --run-only      : nur Run ausführen (heutigen oder via --use gewählten Snapshot)
#
# Env (optional):
#   API_BASE         (default: http://localhost:8000)
#   NEO4J_AUTO_URI   (default: bolt://localhost:7687)
#   NEO4J_USER       (default: neo4j)
#   NEO4J_PASSWORD   (default: passw0rd)
#   WDQS_ENDPOINT    (default: https://query.wikidata.org/sparql)
#   MAX_PER_YEAR     (default: 1000)
#   DEFAULT_NAME     (default: films-2010-2014)
#   FORCE=1          : Fetch erzwingen, auch wenn Snapshot existiert
#############################################

# === defaults ===
API_BASE="${API_BASE:-http://localhost:8000}"
NEO4J_URI="${NEO4J_AUTO_URI:-bolt://localhost:7687}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-passw0rd}"
WDQS_ENDPOINT="${WDQS_ENDPOINT:-https://query.wikidata.org/sparql}"
WDQS_FALLBACK_ENDPOINT="${WDQS_FALLBACK_ENDPOINT:-https://query.wikidata.org/bigdata/namespace/wdq/sparql}"
MAX_PER_YEAR="${MAX_PER_YEAR:-20}"
DEFAULT_NAME="${DEFAULT_NAME:-films-2010-2014}"
YEARS=("2010" "2011" "2012" "2013" "2014")

# === usage & arg parsing ===
print_usage() {
  cat <<USAGE
Usage:
  $(basename "$0")                         # heute ziehen + einfrieren + kombinierter Run
  $(basename "$0") <DATE> [RUN_NAME]       # mit Stichtag + optionalem Namen
  $(basename "$0") --use <DATE>            # vorhandenen Snapshot wiederverwenden (nur Upload+Run)
  $(basename "$0") --fetch-only            # nur Snapshot holen/einfrieren
  $(basename "$0") --run-only              # nur kombinierter Run (heute oder mit --use <DATE>)

Flags/Env:
  FORCE=1                 Fetch erzwingen, auch wenn Snapshot existiert
  DEFAULT_NAME=...        Standard-Name für Runs (default: ${DEFAULT_NAME})
  WDQS_ENDPOINT=...       Primary SPARQL endpoint (default: ${WDQS_ENDPOINT})
  WDQS_FALLBACK_ENDPOINT=... Fallback SPARQL endpoint for robustness
  MAX_PER_YEAR=...        Maximum items per year (default: ${MAX_PER_YEAR})

Note: Das Skript erstellt jetzt eine kombinierte Textdatei aller Jahre (2010-2014)
      und führt einen einzigen Pipeline-Run aus für ein umfassenderes Schema.
      Bei Timeouts wird automatisch mit kleineren Limits und alternativen Endpoints versucht.
USAGE
}

ACTION="both"         # both|fetch|run
USE_DATE=""
SNAPSHOT_DATE=""
RUN_NAME="${DEFAULT_NAME}"

# einfache getopt-lose Auswertung
if [[ $# -eq 0 ]]; then
  SNAPSHOT_DATE="$(date +%F)"
elif [[ "$1" == "--use" ]]; then
  [[ $# -ge 2 ]] || { echo "ERROR: --use benötigt <DATE>."; print_usage; exit 2; }
  USE_DATE="$2"
  SNAPSHOT_DATE="$USE_DATE"
  ACTION="run"
  shift 2
elif [[ "$1" == "--fetch-only" ]]; then
  ACTION="fetch"
  SNAPSHOT_DATE="$(date +%F)"
  shift 1
elif [[ "$1" == "--run-only" ]]; then
  ACTION="run"
  SNAPSHOT_DATE="${USE_DATE:-$(date +%F)}"
  shift 1
else
  # Pfad: <DATE> [RUN_NAME]
  SNAPSHOT_DATE="$1"
  RUN_NAME="${2:-${DEFAULT_NAME}}"
  shift $#
fi

# falls --use + eigener Name gewünscht wäre, kann RUN_NAME via Env überschrieben werden
RUN_NAME="${RUN_NAME:-${DEFAULT_NAME}}"

# === derived paths ===
ROOT="$(pwd)"
OUT_DIR="${ROOT}/data/snapshots/${SNAPSHOT_DATE}/${RUN_NAME}"
QDIR="${OUT_DIR}/queries"
RAW_DIR="${OUT_DIR}/raw"
TXT_DIR="${OUT_DIR}/texts"
RUN_DIR="${ROOT}/data/runs/auto/$(date -u +%Y%m%dT%H%M%SZ)-${RUN_NAME}"
ARTIFACTS_CSV="${OUT_DIR}/artifacts.csv"
LOG_DIR="${RUN_DIR}/logs"
RESP_DIR="${RUN_DIR}/responses"
MARKER="${OUT_DIR}/SNAPSHOT.COMPLETE"

mkdir -p "${QDIR}" "${RAW_DIR}" "${TXT_DIR}" "${RUN_DIR}" "${LOG_DIR}" "${RESP_DIR}"

echo "==> SNAPSHOT_DATE=${SNAPSHOT_DATE}"
echo "==> RUN_NAME=${RUN_NAME}"
echo "==> ACTION=${ACTION}  (both|fetch|run)"
[[ -n "${USE_DATE}" ]] && echo "==> USE_DATE=${USE_DATE}"
echo

# artifacts.csv header
if [[ ! -s "${ARTIFACTS_CSV}" ]]; then
  echo "artifact,source,query_file,year,snapshot_date,sha256,size_bytes,created_at_utc" > "${ARTIFACTS_CSV}"
fi

# === helpers ===
make_query_for_year() {
  local Y="$1"
  local LIMIT="${2:-${MAX_PER_YEAR}}"  # Allow custom limit per year
  echo "SELECT DISTINCT ?film ?filmLabel ?year ?directorLabel ?actorLabel ?genreLabel WHERE { ?film wdt:P31 wd:Q11424. ?film wdt:P577 ?date. BIND(YEAR(?date) AS ?year) FILTER(?year = ${Y}) OPTIONAL { ?film wdt:P57 ?director. } OPTIONAL { ?film wdt:P161 ?actor. } OPTIONAL { ?film wdt:P136 ?genre. } SERVICE wikibase:label { bd:serviceParam wikibase:language \"de,en\". } } ORDER BY ?filmLabel LIMIT ${LIMIT}"
}

have_year_raw()   { [[ -s "${RAW_DIR}/films_${1}.json" ]]; }
have_combined_text() { [[ -s "${TXT_DIR}/films_all_years.txt" ]]; }
all_years_present() {
  for y in "${YEARS[@]}"; do
    have_year_raw "$y" || return 1
  done
  have_combined_text && [[ -s "${ARTIFACTS_CSV}" ]]
}

fetch_year() {
  local Y="$1"
  local CUSTOM_LIMIT="${2:-}"  # Optional custom limit for problematic years
  local QFILE="${QDIR}/films_${Y}.rq"
  local RAW_JSON="${RAW_DIR}/films_${Y}.json"

  make_query_for_year "${Y}" "${CUSTOM_LIMIT}" > "${QFILE}"

  echo "==> WDQS fetch for ${Y}${CUSTOM_LIMIT:+ (limit: ${CUSTOM_LIMIT})}"
  
  # Try both primary and fallback endpoints
  local ENDPOINTS=("${WDQS_ENDPOINT}" "${WDQS_FALLBACK_ENDPOINT}")
  
  for ENDPOINT in "${ENDPOINTS[@]}"; do
    echo "    Trying endpoint: ${ENDPOINT}"
    
    # Mehrere Versuche mit exponential backoff bei Timeouts
    local ATTEMPT=1
    local MAX_ATTEMPTS=3  # Reduced per endpoint, but we try multiple endpoints
    local SLEEP_TIME=10
    
    while [[ $ATTEMPT -le $MAX_ATTEMPTS ]]; do
      echo "    Attempt ${ATTEMPT}/${MAX_ATTEMPTS} on $(basename "${ENDPOINT}")..."
      
      # Erweiterte Timeout-Einstellungen und robustere Headers
      if curl -sS --max-time 300 --connect-timeout 60 \
           --retry-delay 30 --retry-max-time 900 \
           -H 'Accept: application/sparql-results+json' \
           -H 'User-Agent: LLM-Graph-Builder/1.0 (Educational Research Project)' \
           -H 'Accept-Encoding: gzip, deflate' \
           -H 'Connection: keep-alive' \
           --compressed \
           --data-urlencode "query=$(cat "${QFILE}")" \
           "${ENDPOINT}" > "${RAW_JSON}" 2>/tmp/curl_error_${Y}_${ATTEMPT}.log; then
        
        # Prüfe ob die Antwort gültiges JSON ist
        if jq -e '.results.bindings | type=="array"' "${RAW_JSON}" >/dev/null 2>&1; then
          # Erfolg!
          local RAW_SHA RAW_SIZE
          RAW_SHA=$(sha256sum "${RAW_JSON}" | awk '{print $1}')
          RAW_SIZE=$(stat -c%s "${RAW_JSON}")
          echo "$(basename "${RAW_JSON}"),WDQS,${QFILE},${Y},${SNAPSHOT_DATE},${RAW_SHA},${RAW_SIZE},$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${ARTIFACTS_CSV}"
          echo "    ✓ Successfully fetched ${RAW_SIZE} bytes from $(basename "${ENDPOINT}")"
          # Cleanup error logs on success
          rm -f /tmp/curl_error_${Y}_*.log
          return 0
        else
          # Zeige was zurückgekommen ist für Debugging
          echo "    ✗ Invalid JSON response. Content:"
          head -n 5 "${RAW_JSON}" | sed 's/^/      /'
          
          # Check if it's a timeout message specifically
          if grep -qi "timeout" "${RAW_JSON}" 2>/dev/null; then
            echo "    → Detected timeout response"
          fi
        fi
      else
        local CURL_EXIT=$?
        echo "    ✗ curl failed with exit code ${CURL_EXIT}"
        
        # Show curl error details if available
        if [[ -f "/tmp/curl_error_${Y}_${ATTEMPT}.log" ]]; then
          echo "    → curl error details:"
          sed 's/^/      /' "/tmp/curl_error_${Y}_${ATTEMPT}.log"
        fi
        
        # Handle specific curl error codes
        case $CURL_EXIT in
          28) echo "    → Operation timeout (consider increasing timeout settings)" ;;
          7)  echo "    → Failed to connect to host" ;;
          6)  echo "    → Couldn't resolve host" ;;
        esac
      fi
      
      if [[ $ATTEMPT -lt $MAX_ATTEMPTS ]]; then
        echo "    Waiting ${SLEEP_TIME}s before retry..."
        sleep $SLEEP_TIME
        SLEEP_TIME=$((SLEEP_TIME + 10))  # Linear increase for longer delays
      fi
      
      ((ATTEMPT++))
    done
    
    echo "    → All attempts failed for endpoint $(basename "${ENDPOINT}")"
  done
  
  echo "ERROR: WDQS fetch for ${Y} failed on all endpoints after multiple attempts." >&2
  echo "Last response content:" >&2
  cat "${RAW_JSON}" >&2
  
  # Cleanup error logs
  rm -f /tmp/curl_error_${Y}_*.log
  
  return 2
}

build_sentences() {
  local Y="$1"
  local RAW_JSON="${RAW_DIR}/films_${Y}.json"
  local TXT_OUT="${TXT_DIR}/films_${Y}.txt"

  echo "==> Build sentences for ${Y}"
  jq -r '
    .results.bindings[]
    | {
        title:   (.filmLabel.value // "Unbekannter Titel"),
        year:    (.year.value      // ""),
        directors: (.directors.value // ""),
        actors:    (.actors.value    // ""),
        genres:    (.genres.value    // "")
      }
    | .title as $t
    | .year  as $y
    | "“\($t)“ ist ein Film (Jahr \($y))."
      + (if .directors != "" then " Regie: " + .directors + "." else "" end)
      + (if .actors    != "" then " Besetzung: " + .actors + "." else "" end)
      + (if .genres    != "" then " Genre: " + .genres + "." else "" end)
  ' "${RAW_JSON}" > "${TXT_OUT}"

  local TXT_SHA TXT_SIZE
  TXT_SHA=$(sha256sum "${TXT_OUT}" | awk '{print $1}')
  TXT_SIZE=$(stat -c%s "${TXT_OUT}")
  echo "$(basename "${TXT_OUT}"),derived:${RAW_JSON},${QDIR}/films_${Y}.rq,${Y},${SNAPSHOT_DATE},${TXT_SHA},${TXT_SIZE},$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${ARTIFACTS_CSV}"
}

build_combined_sentences() {
  local COMBINED_TXT="${TXT_DIR}/films_all_years.txt"
  echo "==> Build combined sentences for all years (2010-2014)"
  
  # Leer die kombinierte Datei
  > "${COMBINED_TXT}"
  
  for Y in "${YEARS[@]}"; do
    local RAW_JSON="${RAW_DIR}/films_${Y}.json"
    if [[ -s "${RAW_JSON}" ]]; then
      echo "==> Processing year ${Y} into combined file"
      
      # Gruppiere die Ergebnisse nach Film und sammle Direktoren, Schauspieler und Genres
      jq -r '
        .results.bindings
        | group_by(.film.value)
        | .[]
        | {
            film: .[0].film.value,
            title: (.[0].filmLabel.value // "Unbekannter Titel"),
            year: (.[0].year.value // ""),
            directors: [.[] | select(.directorLabel) | .directorLabel.value] | unique | join("; "),
            actors: [.[] | select(.actorLabel) | .actorLabel.value] | unique | join("; "),
            genres: [.[] | select(.genreLabel) | .genreLabel.value] | unique | join("; ")
          }
        | .title as $t
        | .year as $y
        | "\($t) ist ein Film (Jahr \($y))."
          + (if .directors != "" then " Regie: " + .directors + "." else "" end)
          + (if .actors != "" then " Besetzung: " + .actors + "." else "" end)
          + (if .genres != "" then " Genre: " + .genres + "." else "" end)
      ' "${RAW_JSON}" >> "${COMBINED_TXT}"
    else
      echo "WARNING: RAW data for year ${Y} not found: ${RAW_JSON}" >&2
    fi
  done

  if [[ -s "${COMBINED_TXT}" ]]; then
    local TXT_SHA TXT_SIZE
    TXT_SHA=$(sha256sum "${COMBINED_TXT}" | awk '{print $1}')
    TXT_SIZE=$(stat -c%s "${COMBINED_TXT}")
    echo "$(basename "${COMBINED_TXT}"),derived:all_years,combined_query,2010-2014,${SNAPSHOT_DATE},${TXT_SHA},${TXT_SIZE},$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${ARTIFACTS_CSV}"
    echo "==> Combined text file created: ${COMBINED_TXT} (${TXT_SIZE} bytes)"
  else
    echo "ERROR: Combined text file is empty!" >&2
    return 1
  fi
}

install_upload() {
  local TXT="$1"
  local HASH P1 P2 DEST_DIR DEST_FILE
  HASH=$(sha256sum "${TXT}" | awk '{print $1}')
  P1=${HASH:0:2}; P2=${HASH:2:2}
  DEST_DIR="/var/lib/llmgb/uploads/${P1}/${P2}"
  DEST_FILE="${DEST_DIR}/${HASH}.txt"
  echo "==> Install upload → ${DEST_FILE}"
  sudo mkdir -p "${DEST_DIR}"
  sudo install -m 0644 "${TXT}" "${DEST_FILE}"
  sudo test -s "${DEST_FILE}"
}

run_combined_pipeline() {
  local COMBINED_TXT="${TXT_DIR}/films_all_years.txt"
  local HASH RESP
  
  if [[ ! -s "${COMBINED_TXT}" ]]; then
    echo "ERROR: Combined text file not found: ${COMBINED_TXT}" >&2
    return 1
  fi
  
  HASH=$(sha256sum "${COMBINED_TXT}" | awk '{print $1}')
  RESP="${RESP_DIR}/schema.json"

  echo "==> Run combined schema pipeline for all years (2010-2014)"
  curl -sS -X POST "${API_BASE}/pipeline/run" \
    -H 'Content-Type: application/json' \
    -d '{
      "mode":"auto",
      "neo4j":{"uri":"'"${NEO4J_URI}"'","user":"'"${NEO4J_USER}"'","password":"'"${NEO4J_PASSWORD}"'"},
      "sources":[{"file_id":"sha256:'"${HASH}"'"}],
      "prune":true,
      "entity_resolution":false
    }' | jq . > "${RESP}"

  if jq -e 'type=="object"' "${RESP}" >/dev/null; then
    echo "==> Combined schema successfully extracted and saved to: ${RESP}"
    echo "==> Source file: ${COMBINED_TXT}"
  else
    echo "ERROR: Combined schema extraction failed" >&2
    return 1
  fi
}

# === SNAPSHOT REUSE LOGIC ===
SNAPSHOT_READY=0
if [[ -f "${MARKER}" ]] && all_years_present; then
  SNAPSHOT_READY=1
fi

# Wenn beide (fetch+run) und Snapshot für heute schon da ist, ohne FORCE nur Run
if [[ "${ACTION}" == "both" && "${SNAPSHOT_READY}" -eq 1 && "${FORCE:-0}" -eq 0 ]]; then
  echo "==> Snapshot bereits vollständig vorhanden. Fetch wird übersprungen."
  ACTION="run"
fi

# === FETCH PHASE ===
if [[ "${ACTION}" == "fetch" || "${ACTION}" == "both" ]]; then
  echo "=== FETCH PHASE ==="
  for y in "${YEARS[@]}"; do
    if [[ "${FORCE:-0}" -eq 1 ]] || ! have_year_raw "$y"; then
      # Try normal fetch first
      if ! fetch_year "$y"; then
        echo "==> Normal fetch failed for ${y}, trying with reduced limit..."
        
        # Try with progressively smaller limits for problematic years
        for reduced_limit in 10 5 3; do
          echo "==> Retry ${y} with limit ${reduced_limit}"
          if fetch_year "$y" "$reduced_limit"; then
            echo "==> Successfully fetched ${y} with reduced limit ${reduced_limit}"
            break
          fi
        done
        
        # If all attempts failed, check if we have any data
        if ! have_year_raw "$y"; then
          echo "ERROR: All attempts failed for year ${y}" >&2
          exit 3
        fi
      fi
    else
      echo "==> RAW für ${y} vorhanden – skip"
    fi
  done

  # Build combined sentences file from all years
  build_combined_sentences

  if all_years_present; then
    touch "${MARKER}"
  else
    echo "ERROR: Snapshot unvollständig – Abbruch." >&2
    exit 3
  fi
fi

# === UPLOAD PHASE (immer vor RUN) ===
if [[ "${ACTION}" == "run" || "${ACTION}" == "both" ]]; then
  echo "=== UPLOAD PHASE ==="
  COMBINED_TXT="${TXT_DIR}/films_all_years.txt"
  [[ -s "${COMBINED_TXT}" ]] || { echo "ERROR: Combined text file missing: ${COMBINED_TXT}"; exit 4; }
  install_upload "${COMBINED_TXT}"

  # === RUN PHASE ===
  echo "=== RUN PHASE ==="
  run_combined_pipeline
fi

echo
echo "✅ Fertig."
echo "  • Snapshot: ${OUT_DIR}"
echo "  • Queries : ${QDIR}"
echo "  • RAW     : ${RAW_DIR}"
echo "  • Texte   : ${TXT_DIR}"
echo "  • Runs    : ${RUN_DIR}"
echo "  • Artifacts CSV: ${ARTIFACTS_CSV}"
[[ -f "${MARKER}" ]] && echo "  • Marker  : ${MARKER}"
