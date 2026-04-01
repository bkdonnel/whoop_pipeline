"""
Run this script to get fresh Whoop OAuth tokens.
It starts a local server to capture the callback automatically.

Usage: python get_whoop_tokens.py
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
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'No code found in callback.')

    def log_message(self, format, *args):
        pass  # Suppress server logs


def run_server(server):
    server.handle_request()


params = {
    'response_type': 'code',
    'client_id': CLIENT_ID,
    'redirect_uri': REDIRECT_URI,
    'scope': 'read:cycles read:recovery read:sleep read:workout read:profile offline',
    'state': 'random_state_string'
}
url = AUTH_URL + '?' + urlencode(params)

server = HTTPServer(('localhost', 8080), CallbackHandler)
thread = threading.Thread(target=run_server, args=(server,))
thread.start()

print('Opening browser for Whoop authorization...')
print(f'If browser does not open, visit:\n{url}\n')
webbrowser.open(url)

thread.join()

if not auth_code:
    print('Failed to capture authorization code.')
    exit(1)

print('Authorization code captured. Exchanging for tokens...')

response = requests.post(TOKEN_URL, data={
    'grant_type': 'authorization_code',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'code': auth_code,
    'redirect_uri': REDIRECT_URI
})

if response.status_code == 200:
    tokens = response.json()
    print('\n--- Update these in your GitHub Repository Secrets ---')
    print(f"WHOOP_ACCESS_TOKEN:  {tokens['access_token']}")
    print(f"WHOOP_REFRESH_TOKEN: {tokens.get('refresh_token', 'N/A')}")
else:
    print(f'Token exchange failed: {response.status_code} - {response.text}')
