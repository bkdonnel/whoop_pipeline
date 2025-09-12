import snowflake.connector
from datetime import datetime
from typing import Optional


class StateManager:
    def __init__(self, snowflake_conn):
        self.conn = snowflake_conn

    def get_last_processed_timestamp(self, table_name: str) -> str:
        """Get the last processed timestamp for a table"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT last_processed_at FROM whoop.metadata.pipeline_state WHERE table_name = %s",
            (table_name,)
        )
        result = cursor.fetchone()
        return result[0].isoformat() + 'Z' if result else '2024-01-01T00:00:00.000Z'
    
    def update_last_processed_timestamp(self, table_name: str, timestamp: str, status: str = 'success'):
        """Update the last processed timestamp for a table"""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            MERGE INTO whoop.metadata.pipeline_state AS target
            USING (SELECT %s AS table_name, %s AS last_processed_at, %s AS status) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET
                last_processed_at = source.last_processed_at,
                last_run_status = source.status,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (table_name, last_processed_at, last_run_status, updated_at)
                VALUES (source.table_name, source.last_processed_at, source.status, CURRENT_TIMESTAMP())
            """,
            (table_name, timestamp, status)
        )
        self.conn.commit()