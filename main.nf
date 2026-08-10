/*
================================================================================
    SINGLE PROCESS
================================================================================
*/

process synapse_to_crdc {
    // Cap concurrent execution to 10 parallel tasks on Sequera Tower
    maxForks = 10

    // Resource allocation for Sequera Tower (requests node RAM & auto-scales on retry)
    cpus   = 4
    memory = { 32.GB * task.attempt }
    disk   = { 200.GB * task.attempt }
    
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

    echo "=== Step 0: Install git (required for cloning uploader repo) ==="
    if command -v apt-get >/dev/null 2>&1; then
      apt-get update -y
      apt-get install -y git
    else
      echo "apt-get not available; cannot install git" >&2
      exit 127
    fi

    echo "=== Step 1: Downloading from Synapse ${meta.entityid} with path-aware file_name ==="
    FILE_PATH="${meta.file_name}"
    FILE_DIR="\$(dirname "\$FILE_PATH")"

    # If file_name includes a directory (e.g. folder/subdir/file.bam),
    # create that directory and download into it.
    if [[ "\$FILE_DIR" != "." && -n "\$FILE_DIR" ]]; then
      echo "Detected directory in file_name: \$FILE_DIR"
      mkdir -p "\$FILE_DIR"
      synapse -p "\$SYNAPSE_AUTH_TOKEN_DYP" get ${meta.entityid} --downloadLocation "\$FILE_DIR" --use-cache False
    else
      echo "No directory component in file_name; using current directory."
      synapse -p "\$SYNAPSE_AUTH_TOKEN_DYP" get ${meta.entityid} --use-cache False
      FILE_DIR="."
    fi

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
