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

// headers must match your TSV
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
    SINGLE ALL-IN-ONE PROCESS
================================================================================
*/

process synapse_to_crdc {
    // One container: synapse get + TSV + config + upload
    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'

    tag "${meta.file_name}"

    input:
    val(meta)

    secret 'SYNAPSE_AUTH_TOKEN_DYP'
    secret 'CRDC_SUBMISSION_ID'
    secret 'CRDC_API_TOKEN'

    output:
    // Emit something minimal if you want downstream inspection
    tuple val(meta), path("cli-config-*_file.yml"), path("samplesheet_no_entityid-*.tsv")

    script:
    // remove entityid from TSV metadata
    def clean_meta  = meta.findAll { k, v -> k != 'entityid' }
    def json        = groovy.json.JsonOutput.toJson(clean_meta)
    def safe_name   = meta.file_name.replaceAll(/[^a-zA-Z0-9._-]/, "_")
    def dryrun_val  = params.dry_run ? "true" : "false"
    def dryrun_flag = params.dry_run ? "--dry-run" : ""

    """
    set -euo pipefail

    echo "=== Step 1: Downloading from Synapse ${meta.entityid} ==="
    synapse -p \$SYNAPSE_AUTH_TOKEN_DYP get ${meta.entityid}

    echo "=== Step 2: Writing per-file TSV for ${meta.file_name} ==="
    pip install --quiet pandas
    python3 - <<'PYCODE'
    import pandas as pd, json
    row = json.loads('''${json}''')
    df = pd.DataFrame([row])
    df.to_csv("samplesheet_no_entityid-${safe_name}.tsv", sep="\\t", index=False)
    PYCODE

    echo "=== Step 3: Writing CRDC config YAML ==="
    cat > cli-config-${safe_name}_file.yml <<YML
    Config:
      api-url: https://hub.datacommons.cancer.gov/api/graphql
      dryrun: ${dryrun_val}
      overwrite: ${params.overwrite}
      retries: 3
      submission: \$CRDC_SUBMISSION_ID
      manifest: ../samplesheet_no_entityid-${safe_name}.tsv
      data: ..
      token: \$CRDC_API_TOKEN
      type: data file
    YML

    echo "=== Step 4: Cloning CRDC uploader and running upload ==="
    git clone --recurse-submodules --depth 1 https://github.com/CBIIT/crdc-datahub-cli-uploader.git
    cd crdc-datahub-cli-uploader
    pip install --quiet -r requirements.txt

    python3 src/uploader.py \\
      --config ../cli-config-${safe_name}_file.yml \\
      --manifest ../samplesheet_no_entityid-${safe_name}.tsv \\
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
