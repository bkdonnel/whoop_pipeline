CREATE TABLE IF NOT EXISTS DATAEXPERT_STUDENT.BRYAN.pipeline_state (
    table_name          VARCHAR,
    last_processed_at   TIMESTAMP_NTZ,
    last_run_status     VARCHAR,
    updated_at          TIMESTAMP_NTZ
);
