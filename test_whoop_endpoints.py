"""
Gets fresh tokens and tests all Whoop API endpoints.
Run this locally: python test_whoop_endpoints.py
"""
import requests
import threading
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlencode, urlparse, parse_qs

CLIENT_ID = 'a2f8bdbb-723e-44b7-8a89-54b5d955f307'
CLIENT_SECRET = 'bc37534c47c1c2b874a3b74dd21364c7c105c76479a030300577e2ceabb56192'
REDIRECT_URI = 'http://localhost:8080/callback'
TOKEN_URL = 'https://api.prod.whoop.com/oauth/oauth2/token'
AUTH_URL = 'https://api.prod.whoop.com/oauth/oauth2/auth'

auth_code = None

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if 'code' in params:
            auth_code = params['code'][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'Authorization successful! You can close this tab.')
    def log_message(self, format, *args):
        pass

params = {
    'response_type': 'code',
    'client_id': CLIENT_ID,
    'redirect_uri': REDIRECT_URI,
    'scope': 'read:cycles read:recovery read:sleep read:workout read:profile offline',
    'state': 'random_state_string'
}
url = AUTH_URL + '?' + urlencode(params)

server = HTTPServer(('localhost', 8080), CallbackHandler)
thread = threading.Thread(target=lambda: server.handle_request())
thread.start()

print('Opening browser for Whoop authorization...')
webbrowser.open(url)
thread.join()

print('Exchanging code for tokens...')
response = requests.post(TOKEN_URL, data={
    'grant_type': 'authorization_code',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'code': auth_code,
    'redirect_uri': REDIRECT_URI
})

tokens = response.json()
access_token = tokens['access_token']
refresh_token = tokens.get('refresh_token')

print(f'\nACCESS TOKEN:  {access_token}')
print(f'REFRESH TOKEN: {refresh_token}')

print('\n--- Testing all endpoints ---')
headers = {'Authorization': f'Bearer {access_token}'}
endpoints = [
    '/cycle',
    '/recovery',
    '/activity/sleep',
    '/activity/workout',
    '/user/profile/basic',
]

for ep in endpoints:
    r = requests.get(f'https://api.prod.whoop.com/developer/v1{ep}?limit=1', headers=headers)
    print(f'{ep}: {r.status_code} - {r.text[:150]}')
