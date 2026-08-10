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
    // Process one 30-100+ GB file at a time to prevent host node OOM saturation
    maxForks = 1

    // Resource allocation for Sequera Tower (starts at 64 GB, scales to 128 GB on retry)
    cpus   = 4
    memory = { 64.GB * task.attempt }
    disk   = { 250.GB * task.attempt }
    
    errorStrategy = { task.exitStatus in [137, 140, 7] ? 'retry' : 'finish' }
    maxRetries    = 2

    // Call container: synapse get + TSV + config + upload
    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'

    tag "${meta.file_name}"

    input:
    val(meta)

    secret 'SYNAPSE_AUTH_TOKEN_DYP'
    secret 'CRDC_SUBMISSION_ID'
    secret 'CRDC_API_TOKEN'

    output:
    // TSV is required; YAML is optional
    tuple val(meta),
          path("**/cli-config-*_file.yml"), optional: true,
          path("**/samplesheet_no_entityid-*.tsv")

    script:
    // remove entityid from TSV metadata
    def clean_meta  = meta.findAll { k, v -> k != 'entityid' }
    def json        = groovy.json.JsonOutput.toJson(clean_meta)
    def safe_name   = meta.file_name.replaceAll(/[^a-zA-Z0-9._-]/, "_")
    def dryrun_val  = params.dry_run ? "true" : "false"
    def dryrun_flag = params.dry_run ? "--dry-run" : ""

    """
    set -euo pipefail

    export PYTHONUNBUFFERED=1

    echo "=== Step 0: Install git (required for cloning uploader repo) ==="
    if command -v apt-get >/dev/null 2>&1; then
      apt-get update -y
      apt-get install -y git
    else
      echo "apt-get not available; cannot install git" >&2
      exit 127
    fi

    echo "=== Step 1: Downloading from Synapse ${meta.entityid} with path-aware file_name via Python API ==="
    FILE_PATH="${meta.file_name}"
    FILE_DIR="\$(dirname "\$FILE_PATH")"

    if [[ "\$FILE_DIR" != "." && -n "\$FILE_DIR" ]]; then
      echo "Detected directory in file_name: \$FILE_DIR"
      mkdir -p "\$FILE_DIR"
    else
      FILE_DIR="."
    fi

    python3 - <<'PYDOWNLOAD'
import os
import sys
import synapseclient

auth_token = os.environ.get("SYNAPSE_AUTH_TOKEN_DYP")
entity_id = "${meta.entityid}"
download_dir = os.environ.get("FILE_DIR", ".")

if not auth_token:
    print("[ERROR] SYNAPSE_AUTH_TOKEN_DYP secret is missing.", file=sys.stderr)
    sys.exit(1)

# Initialize Synapse client
syn = synapseclient.Synapse()
syn.login(authToken=auth_token, silent=True)

print(f"[INFO] Starting download for {entity_id} into {download_dir}...", file=sys.stderr)
syn.get(entity_id, downloadLocation=download_dir, ifoffileexists="overwrite")
print(f"[INFO] Download completed successfully for {entity_id}.", file=sys.stderr)
PYDOWNLOAD

    echo "=== Step 2: Writing per-file TSV for ${meta.file_name} (no md5/file_size verification) ==="
    python3 - <<'PYCODE'
import json, os, sys, csv

row = json.loads('''${json}''')
filename = row.get("file_name") or ""

tsv_basename = "samplesheet_no_entityid-${safe_name}.tsv"
dirpath = os.path.dirname(filename)

if dirpath and dirpath != ".":
    out_path = os.path.join(dirpath, tsv_basename)
else:
    out_path = tsv_basename

# Keep a stable column order (same order as the JSON dict)
fieldnames = list(row.keys())

if os.path.dirname(out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

with open(out_path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
    w.writeheader()
    w.writerow(row)

print(f"[INFO] Wrote per-file TSV to {out_path}", file=sys.stderr)
PYCODE

    echo "=== Step 3: Writing CRDC config YAML ==="

    TSV_BASENAME="samplesheet_no_entityid-${safe_name}.tsv"

    if [[ "\$FILE_DIR" != "." && -n "\$FILE_DIR" ]]; then
      MANIFEST_REL="../\$FILE_DIR/\$TSV_BASENAME"
      CONFIG_FILE_PATH="\$FILE_DIR/cli-config-${safe_name}_file.yml"
      CONFIG_REL="../\$FILE_DIR/cli-config-${safe_name}_file.yml"
    else
      MANIFEST_REL="../\$TSV_BASENAME"
      CONFIG_FILE_PATH="cli-config-${safe_name}_file.yml"
      CONFIG_REL="../cli-config-${safe_name}_file.yml"
    fi

    cat > "\$CONFIG_FILE_PATH" <<YML
Config:
  api-url: https://hub.datacommons.cancer.gov/api/graphql
  dryrun: ${dryrun_val}
  overwrite: false
  retries: 3
  submission: \$CRDC_SUBMISSION_ID
  manifest: \$MANIFEST_REL
  data: ..
  token: \$CRDC_API_TOKEN
  type: data file
YML

    echo "[INFO] Wrote CRDC config YAML to \$CONFIG_FILE_PATH"

    echo "=== Step 4: Cloning CRDC uploader and running upload ==="
    git clone --recurse-submodules --depth 1 https://github.com/CBIIT/crdc-datahub-cli-uploader.git
    cd crdc-datahub-cli-uploader

    echo "=== Step 4b: Installing CRDC uploader requirements (retry; skip file on failure) ==="
    set +e
    ok=0
    for i in 1 2 3; do
      echo "[INFO] pip install attempt \$i/3"
      python3 -m pip install --quiet --default-timeout=180 --retries 10 -r requirements.txt
      rc=\$?
      if [[ \$rc -eq 0 ]]; then
        ok=1
        break
      fi
      echo "[WARN] pip install failed (rc=\$rc); retrying soon..." >&2
      sleep \$((i*20))
    done
    set -e

    if [[ \$ok -ne 1 ]]; then
      echo "[WARN] pip install failed after retries; skipping ${meta.file_name}." >&2
      exit 0
    fi

    python3 src/uploader.py \\
      --config "\$CONFIG_REL" \\
      --manifest "\$MANIFEST_REL" \\
      ${dryrun_flag} || true

    echo "=== Step 5: Uploader log ==="
    if ls tmp/Uploader*.log 1> /dev/null 2>&1; then
      cat tmp/Uploader*.log
    else
      echo "No uploader log found"
    fi

    echo "=== Done for ${meta.file_name} ==="
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
