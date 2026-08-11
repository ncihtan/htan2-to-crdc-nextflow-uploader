#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
================================================================================
    PARAMETERS AND INPUTS
================================================================================
*/

include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'

// ---- Resolve samplesheet path (local or GitHub/raw URL) ----
def _raw = params.input ?: 'samplesheet.tsv'
def _isUrl = (_raw ==~ /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//)
def _abs   = file(_raw).isAbsolute()
def repoPath = file("${projectDir}/${_raw}")

def resolved_input = _isUrl ? _raw
                    : (_abs && file(_raw).exists()) ? file(_raw).toString()
                    : (repoPath.exists() ? repoPath.toString() : repoPath.toString())

validateParameters()

// headers must match your TSV - TSV is based on file upload from CRDC
def headers = [
  "type", "study.study_id", "participant.study_participant_id",
  "sample.sample_id", "file_name", "file_type", "file_description",
  "file_size", "md5sum", "experimental_strategy_and_data_subtypes",
  "submission_version", "checksum_value", "checksum_algorithm",
  "file_mapping_level", "release_datetime", "is_supplementary_file", "entityid"
]

ch_input = Channel.fromList(
    samplesheetToList(resolved_input, "assets/schema_input.json")
).map { row ->
    if (row instanceof List) {
        return headers.collectEntries { h -> [h, row[headers.indexOf(h)]] }
    } else {
        return row
    }
}


/*
================================================================================
    SINGLE PROCESS
================================================================================
*/

process synapse_to_crdc {
    // Process 1 file at a time sequentially to avoid shared host memory/network saturation
    maxForks = 1

    // Resource allocation aligned with AWS Batch m5a/m6a/r5a/r6a instance ratios
    cpus   = { 16 * task.attempt }
    memory = { 64.GB * task.attempt }
    disk   = { 300.GB * task.attempt }
    
    errorStrategy = { task.exitStatus in [137, 140, 7] ? 'retry' : 'finish' }
    maxRetries    = 2

    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'

    tag "${meta.file_name}"

    input:
    val(meta)

    secret 'SYNAPSE_AUTH_TOKEN_DYP'
    secret 'CRDC_SUBMISSION_ID'
    secret 'CRDC_API_TOKEN'

    output:
    tuple val(meta),
          path("**/cli-config-*_file.yml"), optional: true,
          path("**/samplesheet_no_entityid-*.tsv")

    script:
    def clean_meta  = meta.findAll { k, v -> k != 'entityid' }
    def json        = groovy.json.JsonOutput.toJson(clean_meta)
    def safe_name   = meta.file_name.replaceAll(/[^a-zA-Z0-9._-]/, "_")
    def dryrun_val  = params.dry_run ? "true" : "false"
    def dryrun_flag = params.dry_run ? "--dry-run" : ""

    """
    set -euo pipefail

    export PYTHONUNBUFFERED=1
    export META_JSON='${json}'
    export ENTITY_ID="${meta.entityid}"
    export SAFE_NAME="${safe_name}"

    echo "=== Step 0: Ensure git dependency ==="
    if ! command -v git >/dev/null 2>&1; then
      if command -v apt-get >/dev/null 2>&1; then
        apt-get update -y && apt-get install -y git
      else
        echo "[ERROR] git is not installed and apt-get is unavailable" >&2
        exit 127
      fi
    fi

    echo "=== Step 1 & 2: Stream Download & Generate TSV Manifest ==="
    python3 - <<'PYPROCESS'
import os
import sys
import csv
import json
import urllib.request
import synapseclient

# Load inputs from environment
meta_json = os.environ.get("META_JSON", "{}")
row = json.loads(meta_json)
entity_id = os.environ.get("ENTITY_ID")
auth_token = os.environ.get("SYNAPSE_AUTH_TOKEN_DYP")
safe_name = os.environ.get("SAFE_NAME")

if not auth_token:
    print("[ERROR] SYNAPSE_AUTH_TOKEN_DYP secret is missing.", file=sys.stderr)
    sys.exit(1)

# Resolve file directory and file name
filename = row.get("file_name") or ""
dirpath = os.path.dirname(filename)
target_basename = os.path.basename(filename)

if dirpath and dirpath != ".":
    os.makedirs(dirpath, exist_ok=True)
    dest_path = os.path.join(dirpath, target_basename)
    tsv_path = os.path.join(dirpath, f"samplesheet_no_entityid-{safe_name}.tsv")
else:
    dirpath = "."
    dest_path = target_basename
    tsv_path = f"samplesheet_no_entityid-{safe_name}.tsv"

# --- 1. Synapse Pre-Signed URL Download ---
syn = synapseclient.Synapse()
syn.login(authToken=auth_token, silent=True)

print(f"[INFO] Resolving metadata for {entity_id}...", file=sys.stderr)
entity = syn.get(entity_id, downloadFile=False)

file_handle_id = entity.dataFileHandleId
url_info = syn.restGET(f"/entity/{entity.id}/filehandle/{file_handle_id}/url?redirect=false")

download_url = url_info if isinstance(url_info, str) else (url_info.get("url") or url_info.get("downloadUrl"))

if not download_url:
    print(f"[ERROR] Could not resolve download URL for {entity_id}", file=sys.stderr)
    sys.exit(1)

print(f"[INFO] Streaming {entity_id} to {dest_path}...", file=sys.stderr)

req = urllib.request.Request(download_url)
with urllib.request.urlopen(req) as response, open(dest_path, 'wb') as out_file:
    chunk_size = 8 * 1024 * 1024  # 8 MB chunks
    downloaded = 0
    while True:
        chunk = response.read(chunk_size)
        if not chunk:
            break
        out_file.write(chunk)
        downloaded += len(chunk)

print(f"[INFO] Downloaded {downloaded} bytes for {entity_id}.", file=sys.stderr)

# --- 2. Write TSV Manifest ---
fieldnames = list(row.keys())
with open(tsv_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
    writer.writeheader()
    writer.writerow(row)

print(f"[INFO] Wrote TSV manifest to {tsv_path}", file=sys.stderr)
PYPROCESS

    echo "=== Step 3: Writing CRDC Config YAML (Absolute Paths) ==="
    FILE_PATH="${meta.file_name}"
    FILE_DIR="\$(dirname "\$FILE_PATH")"
    
    WORK_DIR="\$PWD"
    TSV_NAME="samplesheet_no_entityid-${safe_name}.tsv"
    YML_NAME="cli-config-${safe_name}_file.yml"

    if [[ "\$FILE_DIR" != "." && -n "\$FILE_DIR" ]]; then
      MANIFEST_ABS="\$WORK_DIR/\$FILE_DIR/\$TSV_NAME"
      CONFIG_ABS="\$WORK_DIR/\$FILE_DIR/\$YML_NAME"
    else
      MANIFEST_ABS="\$WORK_DIR/\$TSV_NAME"
      CONFIG_ABS="\$WORK_DIR/\$YML_NAME"
    fi

    cat > "\$CONFIG_ABS" <<YML
Config:
  api-url: https://hub.datacommons.cancer.gov/api/graphql
  dryrun: ${dryrun_val}
  overwrite: false
  retries: 3
  submission: \$CRDC_SUBMISSION_ID
  manifest: \$MANIFEST_ABS
  data: \$WORK_DIR
  token: \$CRDC_API_TOKEN
  type: data file
YML

    echo "[INFO] Wrote CRDC config YAML to \$CONFIG_ABS"

    echo "=== Step 4: Cloning CRDC uploader and executing upload ==="
    if [[ ! -d "crdc-datahub-cli-uploader" ]]; then
      git clone --recurse-submodules --depth 1 https://github.com/CBIIT/crdc-datahub-cli-uploader.git
    fi

    # Install requirements safely
    set +e
    ok=0
    for i in 1 2 3; do
      echo "[INFO] pip install attempt \$i/3"
      python3 -m pip install --quiet --default-timeout=180 --retries 10 -r crdc-datahub-cli-uploader/requirements.txt
      if [[ \$? -eq 0 ]]; then
        ok=1
        break
      fi
      sleep \$((i*20))
    done
    set -e

    if [[ \$ok -ne 1 ]]; then
      echo "[WARN] pip install failed after retries; skipping ${meta.file_name}." >&2
      exit 0
    fi

    # Run uploader from current directory using absolute configs
    python3 crdc-datahub-cli-uploader/src/uploader.py \\
      --config "\$CONFIG_ABS" \\
      --manifest "\$MANIFEST_ABS" \\
      ${dryrun_flag} || true

    echo "=== Step 5: Output Logs ==="
    if ls crdc-datahub-cli-uploader/tmp/Uploader*.log 1> /dev/null 2>&1; then
      cat crdc-datahub-cli-uploader/tmp/Uploader*.log
    else
      echo "[INFO] No uploader log found"
    fi

    echo "=== Completed process for ${meta.file_name} ==="
    """
}


/*
================================================================================
    WORKFLOW
================================================================================
*/

workflow {
    ch_input | synapse_to_crdc
}
