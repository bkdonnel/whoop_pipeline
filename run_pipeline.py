import os
import sys
import base64
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
new_access_token = token_data['access_token']
new_refresh_token = token_data['refresh_token']
os.environ['WHOOP_ACCESS_TOKEN'] = new_access_token
print('Token refreshed successfully.')

# Save new tokens back to GitHub Secrets so the next run has valid tokens
gh_pat = os.getenv('GH_PAT')
gh_repo = os.getenv('GH_REPO')

if gh_pat and gh_repo:
    print('Saving new tokens to GitHub Secrets...')
    try:
        from nacl import encoding, public

        headers = {
            'Authorization': f'Bearer {gh_pat}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
        }

        # Fetch the repo's public key for secret encryption
        key_response = requests.get(
            f'https://api.github.com/repos/{gh_repo}/actions/secrets/public-key',
            headers=headers,
        )
        key_response.raise_for_status()
        key_data = key_response.json()
        repo_public_key = key_data['key']
        key_id = key_data['key_id']

        def encrypt_secret(public_key_b64: str, secret_value: str) -> str:
            pk = public.PublicKey(public_key_b64.encode('utf-8'), encoding.Base64Encoder())
            sealed_box = public.SealedBox(pk)
            encrypted = sealed_box.encrypt(secret_value.encode('utf-8'))
            return base64.b64encode(encrypted).decode('utf-8')

        for secret_name, secret_value in [
            ('WHOOP_ACCESS_TOKEN', new_access_token),
            ('WHOOP_REFRESH_TOKEN', new_refresh_token),
        ]:
            encrypted = encrypt_secret(repo_public_key, secret_value)
            put_response = requests.put(
                f'https://api.github.com/repos/{gh_repo}/actions/secrets/{secret_name}',
                headers=headers,
                json={'encrypted_value': encrypted, 'key_id': key_id},
            )
            put_response.raise_for_status()
            print(f'  {secret_name} updated in GitHub Secrets.')

        print('Tokens saved successfully.')
    except Exception as e:
        print(f'Warning: Failed to save tokens to GitHub Secrets: {e}')
        print('You may need to manually update WHOOP_REFRESH_TOKEN before the next run.')
else:
    print('GH_PAT or GH_REPO not set — skipping GitHub Secrets update.')

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
