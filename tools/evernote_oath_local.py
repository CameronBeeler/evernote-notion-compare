"""Flask application for Evernote OAuth authentication flow.

This module provides a local Flask server that handles the OAuth 1.0 flow
for Evernote authentication, obtaining and storing access tokens for API usage.
"""
import os
import json
from pathlib import Path

from flask import Flask, redirect, request
from requests_oauthlib import OAuth1Session

# Evernote OAuth endpoints (production)
REQUEST_TOKEN_URL = "https://www.evernote.com/oauth/request_token"
AUTHORIZE_URL     = "https://www.evernote.com/OAuth.action"
ACCESS_TOKEN_URL  = "https://www.evernote.com/oauth/access_token"

TOKEN_PATH = Path.home() / ".config" / "evernote-notion-compare" / "evernote_token.json"

app = Flask(__name__)

def require_env(name: str) -> str:
    """Require an environment variable to be set.

    Args:
        name: The name of the environment variable to retrieve.

    Returns:
        The value of the environment variable.

    Raises:
        RuntimeError: If the environment variable is not set.
    """
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing env var: {name}")
    return val

def oauth_session(resource_owner_key=None, resource_owner_secret=None) -> OAuth1Session:
    """Create an OAuth1Session for Evernote authentication.

    Args:
        resource_owner_key: Optional OAuth request token key.
        resource_owner_secret: Optional OAuth request token secret.

    Returns:
        An OAuth1Session configured with Evernote credentials from environment variables.

    Raises:
        RuntimeError: If required environment variables are not set.
    """
    return OAuth1Session(
        client_key=require_env("EVERNOTE_CONSUMER_KEY"),
        client_secret=require_env("EVERNOTE_CONSUMER_SECRET"),
        callback_uri=require_env("EVERNOTE_CALLBACK_URL"),
        resource_owner_key=resource_owner_key,
        resource_owner_secret=resource_owner_secret,
    )

@app.get("/")
def start():
    """Handle the initial request to start the OAuth flow.

    This function initiates the OAuth 1.0 flow by obtaining a temporary request token
    from Evernote and redirecting the user to the authorization URL.

    Returns:
        A redirect response to the Evernote authorization URL.
    """
    # 1) Get a temporary request token
    sess = oauth_session()
    fetch = sess.fetch_request_token(REQUEST_TOKEN_URL)

    # Save request token temporarily (in-memory via query params or server state is also OK for local dev)
    oauth_token = fetch["oauth_token"]
    oauth_token_secret = fetch["oauth_token_secret"]

    # 2) Redirect user to Evernote to authorize
    auth_url = sess.authorization_url(AUTHORIZE_URL, oauth_token=oauth_token)

    # Stash request token/secret in a local file for the callback step (simple + fine for local dev)
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(json.dumps({
        "request_oauth_token": oauth_token,
        "request_oauth_token_secret": oauth_token_secret
    }, indent=2))

    return redirect(auth_url)

@app.get("/callback")
def callback():
    """Handle the callback from Evernote after authorization.

    This function exchanges the temporary request token for an access token,
    saving the access token to a local file for future use.

    Returns:
        A success message indicating the OAuth flow is complete.
    """
    # Read saved request token/secret
    data = json.loads(TOKEN_PATH.read_text())
    req_token = data["request_oauth_token"]
    req_secret = data["request_oauth_token_secret"]

    # 3) Evernote returns oauth_verifier
    oauth_verifier = request.args.get("oauth_verifier")
    if not oauth_verifier:
        return "Missing oauth_verifier in callback.", 400

    # 4) Exchange request token for an access token
    sess = oauth_session(resource_owner_key=req_token, resource_owner_secret=req_secret)
    token = sess.fetch_access_token(ACCESS_TOKEN_URL, verifier=oauth_verifier)

    access_token = token.get("oauth_token")
    access_token_secret = token.get("oauth_token_secret")  # sometimes present; keep it

    # Persist access token for later scripts
    TOKEN_PATH.write_text(json.dumps({
        "access_token": access_token,
        "access_token_secret": access_token_secret
    }, indent=2))

    return (
        "Evernote OAuth complete. Token saved to:\n"
        f"{TOKEN_PATH}\n\n"
        "You can close this tab."
    )

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8765, debug=True)