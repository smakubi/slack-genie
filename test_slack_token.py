#!/usr/bin/env python
"""
Test the Slack token to verify it can make API calls.
This will help diagnose the 'not_allowed_token_type' error.
"""
import os
import sys
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Load environment variables from .env file
load_dotenv()

def test_token(token=None):
    """Test if a Slack token is valid and has appropriate permissions"""
    # Use provided token or get from environment
    if not token:
        token = os.environ.get("SLACK_BOT_TOKEN")
        print(token)
    
    if not token:
        print("ERROR: No token provided or found in SLACK_BOT_TOKEN environment variable")
        return False
    
    # Report the token prefix (for debugging only)
    prefix = token.split("-")[0] if "-" in token else "unknown"
    print(f"Token prefix: {prefix}")
    print(f"Token starts with 'xoxb': {token.startswith('xoxb')}")
    
    # Try to use the token
    client = WebClient(token=token)
    try:
        # Try to get bot info
        resp = client.auth_test()
        print("\nSUCCESS! Token is valid.")
        print(f"Connected as: {resp.get('user')} (ID: {resp.get('user_id')})")
        print(f"Team: {resp.get('team')} (ID: {resp.get('team_id')})")
        print(f"Bot ID: {resp.get('bot_id', 'N/A')}")
        
        # Try to list channels (requires different permissions)
        try:
            channels = client.conversations_list(limit=5)
            channel_names = [c["name"] for c in channels.get("channels", [])]
            print(f"\nCan see channels: {', '.join(channel_names[:5])}")
        except SlackApiError as e:
            print(f"\nCannot list channels: {e.response['error']}")
        
        return True
        
    except SlackApiError as e:
        print("\nERROR: Token is invalid or doesn't have required permissions")
        print(f"Error: {e.response['error']}")
        
        if e.response['error'] == 'not_allowed_token_type':
            print("\nCOMMON FIX FOR 'not_allowed_token_type' ERROR:")
            print("1. You must use a Bot User OAuth Token (starts with xoxb-)")
            print("2. Go to your Slack App settings: https://api.slack.com/apps")
            print("3. Click on your app, then select 'OAuth & Permissions'")
            print("4. Find 'Bot User OAuth Token' (not 'User OAuth Token')")
            print("5. Copy this token to your .env file as SLACK_BOT_TOKEN\n")
            
        return False

if __name__ == "__main__":
    # Get token from command line if provided
    token = sys.argv[1] if len(sys.argv) > 1 else None
    test_token(token)