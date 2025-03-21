"""
Main application for the Databricks Genie Slack Bot.
"""
import os
import logging
from flask import Flask, request, jsonify
import hashlib
import hmac
import time

from databricks_genie_bot.config import print_config_status, validate_config
from databricks_genie_bot.slack_bot import get_handler
from databricks_genie_bot.databricks_utils import genie_query
from dotenv import load_dotenv

# LOAD ENVIRONMENT VARIABLES FROM .ENV FILE IF IT EXISTS
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=dotenv_path, override=True)

# CONFIGURE LOGGING
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# INITIALIZE FLASK APP
app = Flask(__name__)

# GET THE SLACK REQUEST HANDLER
slack_handler = get_handler()
@app.route("/test", methods=["GET"])
def test_integration():
    """Test the Databricks Genie integration"""
    result = genie_query("test_user", "What's the most expensive SKU on Snowflake?")
    return jsonify(result)

# SIGNATURE VERIFICATION
def verify_slack_signature(request):
    """Verify the request signature from Slack"""
    slack_signing_secret = os.getenv("SLACK_SIGNING_SECRET")
    
    if not slack_signing_secret:
        logger.error("SLACK_SIGNING_SECRET is not set")
        return False
    
    # GET HEADERS
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")
    
    if not timestamp or not signature:
        logger.error(f"Missing headers - Timestamp: {bool(timestamp)}, Signature: {bool(signature)}")
        return False
    
    # Check if timestamp is too old
    if abs(time.time() - int(timestamp)) > 60 * 5:
        logger.error("Request timestamp too old")
        return False
    
    # Create base string (timestamp + : + request body)
    req_data = request.get_data().decode("utf-8")
    base_string = f"v0:{timestamp}:{req_data}"
    
    # Create our own signature
    my_signature = "v0=" + hmac.new(
        slack_signing_secret.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    
    # Compare signatures
    is_valid = hmac.compare_digest(my_signature, signature)
    if not is_valid:
        logger.error("Signature verification failed")
    return is_valid


# SLACK EVENTS 
@app.route("/slack/events", methods=["POST"])
def slack_events():
    """Handle Slack events"""
    logger.info("Received Slack event")
    
    try:
        data = request.json
        logger.info(f"Event type: {data.get('type')}")
        
        # Check if this is a verification challenge
        if "challenge" in data:
            logger.info("Handling verification challenge")
            return jsonify({"challenge": data["challenge"]})
        
        # Verify the request comes from Slack
        if not verify_slack_signature(request):
            logger.error("Failed signature verification")
            return jsonify({"error": "Invalid signature"}), 401
        
        # For regular events, use the slack handler
        logger.info("Processing event with slack handler")
        return slack_handler.handle(request)
        
    except Exception as e:
        logger.error(f"Error processing Slack event: {str(e)}")
        return jsonify({"error": str(e)}), 500

# HEALTH CHECK
@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"})

@app.route("/", methods=["GET"])
def home():
    """Home page with basic info"""
    return """
    <html>
        <head>
            <title>Databricks Genie Slack Bot</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
                h1 { color: #333; }
                .container { max-width: 800px; margin: 0 auto; }
                .status { padding: 20px; background-color: #f5f5f5; border-radius: 5px; }
                .info { margin-top: 20px; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Databricks Genie Slack Bot</h1>
                <p>This is a Slack bot that integrates with Databricks Genie API to provide natural language querying capabilities.</p>
                
                <div class="status">
                    <h2>Status</h2>
                    <p>The bot is running and listening for Slack events at <code>/slack/events</code>.</p>
                </div>
                
                <div class="info">
                    <h2>Usage</h2>
                    <p>In your configured Slack channel, simply type your data-related question, and the bot will process it through Databricks Genie.</p>
                </div>
            </div>
        </body>
    </html>
    """

@app.route("/api/query", methods=["POST"])
def api_query():
    """API endpoint for testing queries directly"""
    try:
        data = request.json
        question = data.get("question")
        user_id = data.get("user_id", "api_user")
        
        if not question:
            return jsonify({"error": "No question provided"}), 400
        
        # Process the query through Databricks Genie
        result = genie_query(user_id, question)
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"API error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
# DEBUG ENDPOINT
@app.route("/debug", methods=["GET"])
def debug():
    """Debugging endpoint"""
    load_dotenv()
    
    config_status = {
        "slack_bot_token": bool(os.getenv("SLACK_BOT_TOKEN")),
        "slack_channel_id": bool(os.getenv("SLACK_CHANNEL_ID")),
        "slack_signing_secret": bool(os.getenv("SLACK_SIGNING_SECRET")),
        "databricks_host": os.getenv("DATABRICKS_HOST"),
        "databricks_token": bool(os.getenv("DATABRICKS_TOKEN")),
        "space_id": bool(os.getenv("SPACE_ID"))
    }
    return jsonify(config_status)

# Add a test endpoint to verify logging
@app.route("/test-logging", methods=["GET"])
def test_logging():
    """Test that logging is working"""
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    return jsonify({"status": "Logging test complete"})

def main():
    """Main function to run the application"""
    try:
        # Print configuration status
        print_config_status()
        
        # Validate configuration
        validate_config()
        
        # Get port from environment or use default
        port = int(os.getenv("PORT", 8000))
        
        # Run the Flask app
        app.run(host="0.0.0.0", port=port)
    
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()