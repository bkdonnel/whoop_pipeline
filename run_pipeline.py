import os
import sys
import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from include.extract.whoop_extractor import IncrementalWhoopExtractor

# Debug: confirm env vars are present
for var in ['WHOOP_CLIENT_ID', 'WHOOP_CLIENT_SECRET', 'WHOOP_REFRESH_TOKEN', 'WHOOP_REDIRECT_URI']:
    val = os.getenv(var)
    print(f'{var}: {"SET (len=" + str(len(val)) + ")" if val else "NOT SET"}')

# Refresh Whoop token at the start of every run
print('Refreshing Whoop access token...')
token_response = requests.post(
    'https://api.prod.whoop.com/oauth/oauth2/token',
    data={
        'grant_type': 'refresh_token',
        'client_id': os.getenv('WHOOP_CLIENT_ID'),
        'client_secret': os.getenv('WHOOP_CLIENT_SECRET'),
        'refresh_token': os.getenv('WHOOP_REFRESH_TOKEN'),
        'redirect_uri': os.getenv('WHOOP_REDIRECT_URI', 'http://localhost:8080/callback'),
    }
)
if token_response.status_code != 200:
    print(f'Token refresh failed: {token_response.status_code} - {token_response.text}')
    sys.exit(1)

token_data = token_response.json()
os.environ['WHOOP_ACCESS_TOKEN'] = token_data['access_token']
print('Token refreshed successfully.')

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
