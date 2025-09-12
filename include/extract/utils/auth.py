import os
import requests
import json
from urllib.parse import urlencode, parse_qs, urlparse
from dotenv import load_dotenv
import time

try:
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    Variable = None

load_dotenv()

class WhoopAuth:
    def __init__(self):
        # Try Airflow Variables first, fallback to environment variables
        if AIRFLOW_AVAILABLE:
            try:
                self.client_id = Variable.get('WHOOP_CLIENT_ID')
                self.client_secret = Variable.get('WHOOP_CLIENT_SECRET')
                self.redirect_uri = Variable.get('WHOOP_REDIRECT_URI')
            except:
                self.client_id = os.getenv('WHOOP_CLIENT_ID')
                self.client_secret = os.getenv('WHOOP_CLIENT_SECRET')
                self.redirect_uri = os.getenv('WHOOP_REDIRECT_URI')
        else:
            self.client_id = os.getenv('WHOOP_CLIENT_ID')
            self.client_secret = os.getenv('WHOOP_CLIENT_SECRET')
            self.redirect_uri = os.getenv('WHOOP_REDIRECT_URI')
            
        self.auth_url = "https://api.prod.whoop.com/oauth/oauth2/auth"
        self.token_url = "https://api.prod.whoop.com/oauth/oauth2/token"
        
        if not all([self.client_id, self.client_secret, self.redirect_uri]):
            raise ValueError("Missing required environment variables: WHOOP_CLIENT_ID, WHOOP_CLIENT_SECRET, WHOOP_REDIRECT_URI")
    
    def get_authorization_url(self):
        """Generate the authorization URL for OAuth2 flow"""
        params = {
            'response_type': 'code',
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'scope': 'read:cycles read:recovery read:sleep read:workout read:profile offline',
            'state': 'random_state_string'
        }
        return f"{self.auth_url}?{urlencode(params)}"
    
    def exchange_code_for_tokens(self, auth_code):
        """Exchange authorization code for access and refresh tokens"""
        data = {
            'grant_type': 'authorization_code',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'code': auth_code,
            'redirect_uri': self.redirect_uri
        }
        
        response = requests.post(self.token_url, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            self._save_tokens_to_env(token_data)
            return token_data
        else:
            raise Exception(f"Token exchange failed: {response.status_code} - {response.text}")
    
    def refresh_access_token(self, refresh_token=None):
        """Refresh the access token using refresh token"""
        if not refresh_token:
            refresh_token = os.getenv('WHOOP_REFRESH_TOKEN')
        
        if not refresh_token:
            raise ValueError("No refresh token available")
        
        data = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': refresh_token
        }
        
        response = requests.post(self.token_url, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            self._update_env_tokens(token_data)
            return token_data
        else:
            raise Exception(f"Token refresh failed: {response.status_code} - {response.text}")
    
    def get_valid_access_token(self):
        """Get a valid access token, refreshing if necessary"""
        # Try to get tokens from Airflow Variables first, fallback to env vars
        if AIRFLOW_AVAILABLE:
            try:
                access_token = Variable.get('WHOOP_ACCESS_TOKEN', default_var=None)
                refresh_token = Variable.get('WHOOP_REFRESH_TOKEN', default_var=None)
            except:
                access_token = os.getenv('WHOOP_ACCESS_TOKEN')
                refresh_token = os.getenv('WHOOP_REFRESH_TOKEN')
        else:
            access_token = os.getenv('WHOOP_ACCESS_TOKEN')
            refresh_token = os.getenv('WHOOP_REFRESH_TOKEN')
        
        if not access_token:
            if refresh_token:
                token_data = self.refresh_access_token(refresh_token)
                return token_data['access_token']
            else:
                raise ValueError("No access token or refresh token available. Please run initial authentication.")
        
        # Test if current token is valid
        test_response = requests.get(
            'https://api.prod.whoop.com/developer/v1/user/profile/basic',
            headers={'Authorization': f'Bearer {access_token}'}
        )
        
        if test_response.status_code == 401:
            # Token expired, refresh it
            if refresh_token:
                token_data = self.refresh_access_token(refresh_token)
                return token_data['access_token']
            else:
                raise ValueError("Access token expired and no refresh token available")
        
        return access_token
    
    def _save_tokens_to_env(self, token_data):
        """Save tokens to Airflow Variables or .env file"""
        # Try to save to Airflow Variables first
        if AIRFLOW_AVAILABLE:
            try:
                Variable.set('WHOOP_ACCESS_TOKEN', token_data['access_token'])
                Variable.set('WHOOP_REFRESH_TOKEN', token_data.get('refresh_token', ''))
                Variable.set('WHOOP_TOKEN_TYPE', token_data.get('token_type', 'Bearer'))
                Variable.set('WHOOP_EXPIRES_IN', str(token_data.get('expires_in', 3600)))
                return
            except Exception as e:
                print(f"Failed to save to Airflow Variables, falling back to .env: {e}")
        
        # Fallback to .env file
        env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
        
        # Read existing .env content
        env_vars = {}
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                for line in f:
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        env_vars[key] = value
        
        # Update with new tokens
        env_vars['WHOOP_ACCESS_TOKEN'] = token_data['access_token']
        env_vars['WHOOP_REFRESH_TOKEN'] = token_data.get('refresh_token', '')
        env_vars['WHOOP_TOKEN_TYPE'] = token_data.get('token_type', 'Bearer')
        env_vars['WHOOP_EXPIRES_IN'] = str(token_data.get('expires_in', 3600))
        
        # Write back to .env
        with open(env_path, 'w') as f:
            for key, value in env_vars.items():
                f.write(f"{key}={value}\n")
    
    def _update_env_tokens(self, token_data):
        """Update tokens in .env file"""
        self._save_tokens_to_env(token_data)
        
        # Reload environment variables
        load_dotenv(override=True)

def interactive_auth():
    """Interactive authentication flow for initial setup"""
    auth = WhoopAuth()
    
    print("Whoop API Authentication Setup")
    print("=" * 40)
    print(f"1. Visit this URL in your browser:")
    print(f"   {auth.get_authorization_url()}")
    print("\n2. After authorizing, you'll be redirected to your redirect URI with a 'code' parameter")
    print("3. Copy the authorization code from the URL and paste it below")
    
    auth_code = input("\nEnter the authorization code: ").strip()
    
    try:
        token_data = auth.exchange_code_for_tokens(auth_code)
        print("\nAuthentication successful!")
        print(f"Access token expires in: {token_data.get('expires_in', 'unknown')} seconds")
        print("Tokens have been saved to .env file")
        return token_data
    except Exception as e:
        print(f"\nAuthentication failed: {e}")
        return None

if __name__ == "__main__":
    interactive_auth()