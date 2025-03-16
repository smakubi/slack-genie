"""
Check ngrok status and display the Slack Request URL.

This script:
1. Checks if ngrok is already running
2. Prints the public URL for you to update in the Slack API Dashboard

Usage:
    python start_ngrok.py

Note: You must start ngrok manually with:
    ngrok http <PORT>

Where <PORT> is the port your Flask app is running on (default: 3000)
"""
import os
import sys
import time
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_ngrok_running():
    """Check if ngrok is running"""
    try:
        response = requests.get("http://localhost:4040/api/tunnels", timeout=2)
        if response.status_code == 200:
            return True
        return False
    except requests.exceptions.RequestException:
        return False

def get_ngrok_url():
    """Get the public URL from ngrok"""
    try:
        response = requests.get("http://localhost:4040/api/tunnels")
        tunnels = response.json()["tunnels"]
        
        if not tunnels:
            print("No tunnels found. Please check ngrok is running properly.")
            return None
        
        # Get the HTTPS tunnel URL
        for tunnel in tunnels:
            if tunnel["proto"] == "https":
                public_url = tunnel["public_url"]
                print(f"\n✅ ngrok tunnel is active at: {public_url}")
                return public_url
        
        # If no HTTPS tunnel found, use the first tunnel
        public_url = tunnels[0]["public_url"]
        print(f"\n✅ ngrok tunnel is active at: {public_url}")
        return public_url
        
    except requests.exceptions.RequestException:
        print("Could not connect to ngrok API. Please check it's running correctly.")
        return None

def main():
    # Get the port from environment or use default
    port = int(os.environ.get("PORT", 3000))
    
    # Check if ngrok is running
    if not check_ngrok_running():
        print("❌ ngrok is not running. Please start ngrok first with:")
        print(f"    ngrok http {port}")
        print("\nThen run this script again.")
        sys.exit(1)
    
    # Get the ngrok URL
    public_url = get_ngrok_url()
    
    if not public_url:
        print("❌ Could not get ngrok tunnel URL.")
        sys.exit(1)
    
    # Print instructions for Slack
    print("\n=== Slack Configuration Instructions ===")
    print("1. Go to your Slack App settings page")
    print("2. Navigate to 'Event Subscriptions'")
    print("3. Enable Events and set the Request URL to:")
    print(f"   {public_url}/slack/events")
    print("\n4. Under 'Subscribe to bot events', add these bot events:")
    print("   - message.channels")
    print("   - message.groups")
    print("   - message.im")
    print("\n5. Save your changes and reinstall the app if prompted")
    print(f"\n✅ Your Flask app should be running on port {port}.")
    print("   The ngrok tunnel is active. Leave both processes running.")
    
    # Keep script running until interrupted
    print("\n(Press Ctrl+C to exit this script - ngrok will continue running)")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nExiting script. Remember to manually stop ngrok when done.")
        sys.exit(0)

if __name__ == "__main__":
    main()