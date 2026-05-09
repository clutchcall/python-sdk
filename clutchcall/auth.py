"""
ClutchCall Service Account Authenticator.
"""
import json
import time

try:
    import jwt
except ImportError:
    jwt = None

class ServiceAccountAuthenticator:
    """Automatically generates RS256 signed JWTs."""

    def __init__(self, service_account_path: str):
        if jwt is None:
            raise ImportError("PyJWT is required for Service Accounts.")
            
        with open(service_account_path, 'r') as f:
            self._creds = json.load(f)
            
        self._private_key = self._creds['private_key']
        self._iss = self._creds['client_email']
        self._kid = self._creds.get('private_key_id', 'default-key')

    def generate_token(self) -> str:
        payload = {
            "iss": self._iss,
            "sub": "clutchcall-sdk",
            "aud": "clutchcall-api",
            "iat": int(time.time()),
            "exp": int(time.time()) + 300
        }
        headers = {"kid": self._kid}
        return jwt.encode(payload, self._private_key, algorithm="RS256", headers=headers)
