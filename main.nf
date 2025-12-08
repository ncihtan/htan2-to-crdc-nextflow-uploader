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
    PROCESSES
================================================================================
*/

process synapse_get {
    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'

    tag "${meta.entityid}"

    input:
    val(meta)

    secret 'SYNAPSE_AUTH_TOKEN_DYP'

    output:
    tuple val(meta), path('*')

    script:
    def args = task.ext.args ?: ''
    """
    echo "Fetching entity ${meta.entityid} from Synapse into flat directory..."
    synapse -p \$SYNAPSE_AUTH_TOKEN_DYP get $args ${meta.entityid}
    """
}

process write_file_tsv {
    container 'python:3.11'
    tag "${meta.file_name}"

    input:
    tuple val(meta), path(files)

    output:
    tuple val(meta), path(files), path("samplesheet_no_entityid-${meta.file_name}.tsv")

    script:
    def json = groovy.json.JsonOutput.toJson(meta)
    def safe_name = meta.file_name.replaceAll(/[^a-zA-Z0-9._-]/, "_")  // sanitize filename for TSV
    """
    pip install --quiet pandas
    python3 - <<'PYCODE'
    import pandas as pd, json
    row = json.loads('''${json}''')
    df = pd.DataFrame([row])
    df.to_csv("samplesheet_no_entityid-${safe_name}.tsv", sep="\\t", index=False)
    PYCODE
    """
}


process make_config_yml {
    container 'python:3.11'
    tag "${meta.file_name}"

    input:
    tuple val(meta), path(files), path(manifest_tsv)

    secret 'CRDC_SUBMISSION_ID'
    secret 'CRDC_API_TOKEN'

    output:
    tuple val(meta), path(files), path("cli-config-*_file.yml"), path(manifest_tsv)

    script:
    def dryrun_value = params.dry_run ? "true" : "false"
    """
    cat > cli-config-${meta.file_name}_file.yml <<YML
    Config:
      api-url: https://hub.datacommons.cancer.gov/api/graphql
      dryrun: ${dryrun_value}
      overwrite: ${params.overwrite}
      retries: 3
      submission: \$CRDC_SUBMISSION_ID
      manifest: ../${manifest_tsv}
      data: ..
      token: \$CRDC_API_TOKEN
      type: data file
    YML
    """
}


process crdc_upload {
    container 'python:3.11'
    tag "${meta.file_name}"

    input:
    tuple val(meta), path(files), path(config), path(manifest_tsv)

    secret 'CRDC_API_TOKEN'
    secret 'CRDC_SUBMISSION_ID'

    output:
    tuple val(meta), path(config), path(manifest_tsv)

    script:
    def dryrun_flag = params.dry_run ? "--dry-run" : ""
    """
    set -euo pipefail

    echo "============================================"
    echo "Listing files in working directory before upload:"
    ls -lh .
    ls -lh ..
    echo "============================================"

    echo "Fetching CRDC uploader source from GitHub..."
    git clone --recurse-submodules --depth 1 https://github.com/CBIIT/crdc-datahub-cli-uploader.git
    cd crdc-datahub-cli-uploader

    echo "Installing uploader requirements..."
    pip install --quiet -r requirements.txt

    echo "Running CRDC uploader..."
    python3 src/uploader.py \\
      --config ../${config} \\
      --manifest ../${manifest_tsv} \\
      $dryrun_flag || true

    echo "============================================"
    echo "Dumping CRDC uploader log:"
    if ls tmp/Uploader*.log 1> /dev/null 2>&1; then
      cat tmp/Uploader*.log
    else
      echo "No uploader log found"
    fi
    echo "============================================"

    echo "Listing files in working directory after upload:"
    ls -lh ..
    echo "============================================"
    """
}


/*
================================================================================
    WORKFLOW
================================================================================
*/

workflow {
    // Step 1: download files from Synapse
    fetched = ch_input | synapse_get
    
    // Step 2: drop entityid inline
    cleaned = fetched.map { meta, files ->
        def clean_meta = meta.findAll { k, v -> k != 'entityid' }
        tuple(clean_meta, files)
    }
    
    // Step 3: write a per-file TSV
    per_file = cleaned | write_file_tsv
    
    // Step 4: make YAML config for each file
    with_yaml = per_file | make_config_yml
    
    // Step 5: upload
    crdc_upload(with_yaml)
}
