import os
import sys
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from include.extract.whoop_extractor import IncrementalWhoopExtractor

pem_key = os.getenv('NEW_SNOWFLAKE_PRIVATE_KEY').encode()
pk = serialization.load_pem_private_key(pem_key, password=None, backend=default_backend())

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    private_key=pk,
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    role=os.getenv('SNOWFLAKE_ROLE'),
)

extractor = IncrementalWhoopExtractor(conn)
failed = []

for endpoint in ['sleep', 'cycle', 'workout', 'recovery', 'user']:
    try:
        count = extractor.safe_extract_with_retry(endpoint)
        print(f'{endpoint}: {count} records extracted')
    except Exception as e:
        print(f'{endpoint} failed: {e}')
        failed.append(endpoint)

conn.close()

if failed:
    print(f'Failed endpoints: {failed}')
    sys.exit(1)
