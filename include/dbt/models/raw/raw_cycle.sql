CREATE TABLE IF NOT EXISTS DATAEXPERT_STUDENT.BRYAN.raw_cycle (
    id                  VARCHAR,
    user_id             VARCHAR,
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    start_time          TIMESTAMP_NTZ,
    end_time            TIMESTAMP_NTZ,
    timezone_offset     VARCHAR,
    score_state         VARCHAR,
    strain              FLOAT,
    kilojoule           FLOAT,
    average_heart_rate  INTEGER,
    max_heart_rate      INTEGER,
    ingested_at         TIMESTAMP_NTZ
);
