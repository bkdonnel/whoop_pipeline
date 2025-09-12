import os
import requests
import json
from datetime import datetime, timezone, timedelta
import time
from pathlib import Path
import sys
from typing import List, Dict, Any
from datetime import datetime, timezone
from .utils.state_manager import StateManager
from .utils.auth import WhoopAuth
from .utils.snowflake_client import SnowflakeClient


class IncrementalWhoopExtractor:
    def __init__(self, snowflake_conn):
        self.state_manager = StateManager(snowflake_conn)
        self.base_url = "https://api.prod.whoop.com/developer/v1"
        # ... existing auth setup

        # Initialize auth and snowflake client
        self.auth = WhoopAuth()
        self.snowflake_client = SnowflakeClient(connection=snowflake_conn)
        
        # Define all Whoop API endpoints with their configurations
        self.endpoints = {
            'cycle':{
                'url': f"{self.base_url}/cycle",
                'table_name': 'raw.cycle',
                'supports_incremental': True,
                'time_field': 'updated_at'
            },
            'sleep':{
                'url': f"{self.base_url}/activity/sleep",
                'table_name': 'raw.sleep',
                'supports_incremental': True,
                'time_field': 'updated_at'
            },
            'recovery':{
                'url': f"{self.base_url}/recovery",
                'table_name': 'raw.recovery',
                'supports_incremental': True,
                'time_field': 'updated_at'
            },
            'workout':{
                'url': f"{self.base_url}/activity/workout",
                'table_name': 'raw.workout',
                'supports_incremental': True,
                'time_field': 'updated_at'
            },
            'user': {
                'url': f"{self.base_url}/user/profile/basic",
                'table_name': 'raw.user',
                'supports_incremental': False,
                'time_field': None
            }
        }

    def extract_incremental_data(self, endpoint_name):
        """Extract incremental data for any endpoint"""
        config = self.endpoints[endpoint_name]

        # Handle non-incremental endpoints (like user profile)
        if not config['supports_incremental']:
            return self.extract_full_refresh(endpoint_name)
        
        last_processed = self.state_manager.get_last_processed_timestamp(endpoint_name)

        # WHOOP API collection endpoints - avoid large date ranges that cause 400 errors
        # For initial extractions, use no date parameters to get recent data
        last_processed_date = datetime.fromisoformat(last_processed.replace('Z', '+00:00')) if last_processed else None
        
        # If last processed date is very old (> 30 days ago), don't use date parameters
        # This avoids 400 errors from large date ranges
        thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
        
        if not last_processed_date or last_processed_date < thirty_days_ago:
            # No date parameters - get recent data only
            params = {'limit': 25}
        else:
            # Use incremental approach for recent data
            params = {
                'start': last_processed,
                'end': datetime.now(timezone.utc).isoformat(),
                'limit': 25
            }
        
        all_records = []
        url = config['url']

        while url:
            response = self.make_paginated_request(url, params)
            records = response.get('records', [])
            all_records.extend(records)

            # Get next page URL
            next_token = response.get('next_token')
            if next_token:
                params['nextToken'] = next_token
                url = config['url']  # Keep same base URL, add nextToken param
                # Small delay between paginated requests to avoid rate limiting
                time.sleep(1)
            else:
                url = None

        if all_records:
            # Add ingestion timestamp
            current_time = datetime.now(timezone.utc).isoformat()
            for record in all_records:
                record['ingested_at'] = current_time

            # Load to Snowflake
            self.load_to_snowflake(config['table_name'], all_records)

            # Update state with the latest record timestamp
            time_field = config['time_field']
            if time_field and all_records:
                latest_timestamp = max([record[time_field] for record in all_records if time_field in record])
                self.state_manager.update_last_processed_timestamp(endpoint_name, latest_timestamp)

        return len(all_records)
    
    def extract_full_refresh(self, endpoint_name):
        """Extract full data for endpoints that don't support incremental (like user profile)"""
        config = self.endpoints[endpoint_name]
        url = config['url']

        try:
            response_data = self.make_api_request(url)

            # User profile returns single object, not paginated
            if endpoint_name == 'user':
                all_records = [response_data] if response_data else []
            else:
                # Handle other non-incremental endpoints if needed
                all_records = response_data.get('records', [response_data])

            if all_records:
                # Add ingestion timestamp
                current_time = datetime.now(timezone.utc).isoformat()
                for record in all_records:
                    record['ingested_at'] = current_time

                self.load_to_snowflake(config['table_name'], all_records, replace=True)

            return len(all_records)
        
        except Exception as e:
            print(f"Error extracting {endpoint_name}: {e}")
            raise

    def safe_extract_with_retry(self, endpoint_name: str, max_retries: int = 3):
        """Extract with retry logic and state management for any endpoint"""
        for attempt in range(max_retries):
            try:
                # Use the universal extraction method for all endpoints
                count = self.extract_incremental_data(endpoint_name)
                print(f"Successfully extracted {count} {endpoint_name} records")
                return count
            
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {endpoint_name}: {e}")
                if attempt == max_retries - 1:
                    # Mark as failed in state table
                    self.state_manager.update_last_processed_timestamp(
                        endpoint_name, 
                        self.state_manager.get_last_processed_timestamp(endpoint_name),
                        'failed'
                    )
                    raise
                # Longer wait for rate limit errors, shorter for others
                wait_time = 60 if "429" in str(e) else 2 ** attempt
                time.sleep(wait_time)

    def make_api_request(self, url: str, params: Dict = None) -> Dict:
        """Make authenticated API request with rate limiting"""
        headers = {'Authorization': f'Bearer {self.auth.get_valid_access_token()}'}
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 401:
            # Token expired, refresh and retry
            self.auth.refresh_access_token()
            headers = {'Authorization': f'Bearer {self.auth.get_valid_access_token()}'}
            response = requests.get(url, headers=headers, params=params)
        elif response.status_code == 429:
            # Rate limited - wait and retry
            print(f"Rate limited, waiting 60 seconds before retry...")
            time.sleep(60)
            headers = {'Authorization': f'Bearer {self.auth.get_valid_access_token()}'}
            response = requests.get(url, headers=headers, params=params)
        
        response.raise_for_status()
        return response.json()
    
    def make_paginated_request(self, url: str, params: Dict = None) -> Dict:
        """Make paginated API request"""
        return self.make_api_request(url, params)
    
    def load_to_snowflake(self, table_name: str, records: List[Dict], replace: bool = False):
        """Load records to Snowflake table"""
        if replace:
            # For user table - replace all data
            self.snowflake_client.replace_table_data(table_name, records)
        else:
            # For incremental tables - append new data
            self.snowflake_client.insert_records(table_name, records)

if __name__ == "__main__":
    # For testing
    pass