CREATE TABLE IF NOT EXISTS DATAEXPERT_STUDENT.BRYAN.raw_recovery (
    cycle_id            VARCHAR,
    sleep_id            VARCHAR,
    user_id             VARCHAR,
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    score_state         VARCHAR,
    user_calibrating    BOOLEAN,
    recovery_score      FLOAT,
    resting_heart_rate  FLOAT,
    hrv_rmssd_milli     FLOAT,
    spo2_percentage     FLOAT,
    skin_temp_celsius   FLOAT,
    ingested_at         TIMESTAMP_NTZ
);
