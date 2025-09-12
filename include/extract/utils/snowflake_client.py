import os
import snowflake.connector
from typing import List, Dict, Any, Optional
import json
from datetime import datetime, timezone


class SnowflakeClient:
    """Snowflake database client for Whoop data pipeline"""
    
    def __init__(self, connection=None):
        """Initialize Snowflake connection using provided connection or environment variables"""
        self.connection = connection
        self.cursor = None
        if self.connection:
            self.cursor = self.connection.cursor()
            print("Using provided Snowflake connection")
        else:
            self._connect()
    
    def _connect(self):
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA'),
                insecure_mode=True  # Disable OCSP checking for certificate validation issues
            )
            self.cursor = self.connection.cursor()
            print("Successfully connected to Snowflake")
        except Exception as e:
            print(f"Failed to connect to Snowflake: {e}")
            raise
    
    def close(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Snowflake connection closed")
    
    def flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested dictionary for database storage"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def prepare_data_for_insert(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare data for insertion by flattening"""
        if not data:
            return []
        
        # Flatten all records
        flattened_data = [self.flatten_dict(record) for record in data]
        
        return flattened_data
    
    def truncate_table(self, table_name: str):
        """Truncate table before inserting new data"""
        try:
            sql = f"TRUNCATE TABLE {table_name}"
            self.cursor.execute(sql)
            print(f"Table {table_name} truncated successfully")
        except Exception as e:
            print(f"Failed to truncate table {table_name}: {e}")
            raise
    
    def insert_data(self, table_name: str, data: List[Dict[str, Any]], truncate_first: bool = True):
        """Insert data into Snowflake table using JSON approach"""
        if not data:
            print(f"No data to insert into {table_name}")
            return
        
        try:
            # Truncate table if requested
            if truncate_first:
                self.truncate_table(table_name)
            
            # Insert data as JSON objects
            self._insert_json_data(table_name, data)
            print(f"Successfully inserted {len(data)} rows into {table_name}")
                
        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")
            raise
    
    
    def _insert_json_data(self, table_name: str, data: List[Dict[str, Any]]):
        """Insert data into the structured tables with proper column mapping"""
        # Determine which table we're inserting into and use appropriate mapper
        table_suffix = table_name.split('.')[-1]  # Extract 'cycle', 'sleep', etc.
        
        if table_suffix == 'cycle':
            self._insert_cycle_data(data)
        elif table_suffix == 'sleep':
            self._insert_sleep_data(data)
        elif table_suffix == 'recovery':
            self._insert_recovery_data(data)
        elif table_suffix == 'workout':
            self._insert_workout_data(data)
        elif table_suffix == 'user':
            self._insert_user_data(data)
        else:
            raise ValueError(f"Unknown table type: {table_suffix}")
    
    def _insert_cycle_data(self, data: List[Dict[str, Any]]):
        """Insert cycle data with proper column mapping"""
        sql = """
            INSERT INTO raw.cycle (
                id, user_id, created_at, updated_at, start_time, end_time,
                timezone_offset, score_state, strain, kilojoule, 
                average_heart_rate, max_heart_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for record in data:
            score = record.get('score') or {}
            row = (
                record.get('id'),
                record.get('user_id'),
                self._parse_timestamp(record.get('created_at')),
                self._parse_timestamp(record.get('updated_at')),
                self._parse_timestamp(record.get('start')),
                self._parse_timestamp(record.get('end')),
                record.get('timezone_offset'),
                score.get('state'),
                score.get('strain'),
                score.get('kilojoule'),
                score.get('average_heart_rate'),
                score.get('max_heart_rate')
            )
            rows.append(row)
        
        self.cursor.executemany(sql, rows)
        self.connection.commit()
    
    def _insert_sleep_data(self, data: List[Dict[str, Any]]):
        """Insert sleep data with proper column mapping"""
        sql = """
            INSERT INTO raw.sleep (
                id, v1_id, user_id, created_at, updated_at, start_time, end_time,
                timezone_offset, nap, score_state, total_in_bed_time_milli,
                total_awake_time_milli, total_no_data_time_milli, total_light_sleep_time_milli,
                total_slow_wave_sleep_time_milli, total_rem_sleep_time_milli,
                sleep_cycle_count, disturbance_count, baseline_milli,
                need_from_sleep_debt_milli, need_from_recent_strain_milli,
                need_from_recent_nap_milli, respiratory_rate, sleep_performance_percentage,
                sleep_consistency_percentage, sleep_efficiency_percentage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for record in data:
            score = record.get('score') or {}
            stage_summary = score.get('stage_summary') or {}
            sleep_needed = score.get('sleep_needed') or {}
            
            row = (
                record.get('id'),
                record.get('v1_id'),
                record.get('user_id'),
                self._parse_timestamp(record.get('created_at')),
                self._parse_timestamp(record.get('updated_at')),
                self._parse_timestamp(record.get('start')),
                self._parse_timestamp(record.get('end')),
                record.get('timezone_offset'),
                record.get('nap'),
                score.get('state'),
                stage_summary.get('total_in_bed_time_milli'),
                stage_summary.get('total_awake_time_milli'),
                stage_summary.get('total_no_data_time_milli'),
                stage_summary.get('total_light_sleep_time_milli'),
                stage_summary.get('total_slow_wave_sleep_time_milli'),
                stage_summary.get('total_rem_sleep_time_milli'),
                stage_summary.get('sleep_cycle_count'),
                stage_summary.get('disturbance_count'),
                sleep_needed.get('baseline_milli'),
                sleep_needed.get('need_from_sleep_debt_milli'),
                sleep_needed.get('need_from_recent_strain_milli'),
                sleep_needed.get('need_from_recent_nap_milli'),
                score.get('respiratory_rate'),
                score.get('sleep_performance_percentage'),
                score.get('sleep_consistency_percentage'),
                score.get('sleep_efficiency_percentage')
            )
            rows.append(row)
        
        self.cursor.executemany(sql, rows)
        self.connection.commit()
    
    def _insert_recovery_data(self, data: List[Dict[str, Any]]):
        """Insert recovery data with proper column mapping"""
        sql = """
            INSERT INTO raw.recovery (
                cycle_id, sleep_id, user_id, created_at, updated_at,
                score_state, user_calibrating, recovery_score, resting_heart_rate,
                hrv_rmssd_milli, spo2_percentage, skin_temp_celsius
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for record in data:
            score = record.get('score') or {}
            row = (
                record.get('cycle_id'),
                record.get('sleep_id'),
                record.get('user_id'),
                self._parse_timestamp(record.get('created_at')),
                self._parse_timestamp(record.get('updated_at')),
                score.get('state'),
                score.get('user_calibrating'),
                score.get('recovery_score'),
                score.get('resting_heart_rate'),
                score.get('hrv_rmssd_milli'),
                score.get('spo2_percentage'),
                score.get('skin_temp_celsius')
            )
            rows.append(row)
        
        self.cursor.executemany(sql, rows)
        self.connection.commit()
    
    def _insert_workout_data(self, data: List[Dict[str, Any]]):
        """Insert workout data with proper column mapping"""
        sql = """
            INSERT INTO raw.workout (
                id, v1_id, user_id, created_at, updated_at, start_time, end_time,
                timezone_offset, sport_name, score_state, sport_id, strain,
                average_heart_rate, max_heart_rate, kilojoule, percent_recorded,
                distance_meter, altitude_gain_meter, altitude_change_meter,
                zone_zero_milli, zone_one_milli, zone_two_milli, zone_three_milli,
                zone_four_milli, zone_five_milli
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for record in data:
            score = record.get('score') or {}
            zone_duration = score.get('zone_duration') or {}
            
            row = (
                record.get('id'),
                record.get('v1_id'),
                record.get('user_id'),
                self._parse_timestamp(record.get('created_at')),
                self._parse_timestamp(record.get('updated_at')),
                self._parse_timestamp(record.get('start')),
                self._parse_timestamp(record.get('end')),
                record.get('timezone_offset'),
                record.get('sport_name'),
                score.get('state'),
                record.get('sport_id'),
                score.get('strain'),
                score.get('average_heart_rate'),
                score.get('max_heart_rate'),
                score.get('kilojoule'),
                score.get('percent_recorded'),
                score.get('distance_meter'),
                score.get('altitude_gain_meter'),
                score.get('altitude_change_meter'),
                zone_duration.get('zone_zero_milli'),
                zone_duration.get('zone_one_milli'),
                zone_duration.get('zone_two_milli'),
                zone_duration.get('zone_three_milli'),
                zone_duration.get('zone_four_milli'),
                zone_duration.get('zone_five_milli')
            )
            rows.append(row)
        
        self.cursor.executemany(sql, rows)
        self.connection.commit()
    
    def _insert_user_data(self, data: List[Dict[str, Any]]):
        """Insert user data with proper column mapping"""
        sql = """
            INSERT INTO raw.user (
                user_id, email, first_name, last_name, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for record in data:
            row = (
                record.get('user_id'),
                record.get('email'),
                record.get('first_name'),
                record.get('last_name'),
                self._parse_timestamp(record.get('created_at')),
                self._parse_timestamp(record.get('updated_at'))
            )
            rows.append(row)
        
        self.cursor.executemany(sql, rows)
        self.connection.commit()
    
    def _parse_timestamp(self, timestamp_str: str) -> str:
        """Parse timestamp string and return in Snowflake format"""
        if not timestamp_str:
            return None
        try:
            # Parse ISO format and return as string for Snowflake TIMESTAMP_NTZ
            from datetime import datetime
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Remove microseconds to milliseconds
        except:
            return timestamp_str
    
    def test_connection(self) -> bool:
        """Test the Snowflake connection"""
        try:
            self.cursor.execute("SELECT CURRENT_VERSION()")
            result = self.cursor.fetchone()
            print(f"Snowflake connection test successful. Version: {result[0]}")
            return True
        except Exception as e:
            print(f"Snowflake connection test failed: {e}")
            return False
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table"""
        try:
            sql = f"SELECT COUNT(*) FROM {table_name}"
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            print(f"Error getting row count for {table_name}: {e}")
            return 0

    def replace_table_data(self, table_name: str, records: List[Dict[str, Any]]):
        """Replace table data - truncate and insert"""
        self.insert_data(table_name, records, truncate_first=True)

    def insert_records(self, table_name: str, records: List[Dict[str, Any]]):
        """Insert records without truncating"""
        self.insert_data(table_name, records, truncate_first=False)