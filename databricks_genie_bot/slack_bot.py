"""
Slack bot implementation for the Databricks Genie integration.
"""
import re
import logging
from typing import Dict, List, Any, Callable

from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler

from databricks_genie_bot.config import SLACK_BOT_TOKEN, SLACK_CHANNEL_ID, SLACK_SIGNING_SECRET, FORMAT_TABLES
from databricks_genie_bot.databricks_utils import genie_query

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the Slack app with signing secret for verification if available
if SLACK_SIGNING_SECRET and SLACK_BOT_TOKEN:
    app = App(
        token=SLACK_BOT_TOKEN,
        signing_secret=SLACK_SIGNING_SECRET
    )
else:
    # Running in insecure mode for testing
    logger.warning("Running without proper Slack credentials (insecure mode)")
    raise ValueError(
        "SLACK_BOT_TOKEN and SLACK_SIGNING_SECRET must be set in environment variables"
    )

def format_dataframe_for_slack(data: Dict[str, Any]) -> List[Dict]:
    """Format a DataFrame as a Slack message with table"""
    blocks = []
    
    # Add query description if available
    if data.get("query_description"):
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Analysis:*\n{data['query_description']}"
            }
        })
    
    # Add SQL query if available (in a code block)
    if data.get("sql_query"):
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*SQL Query:*\n```sql\n{data['sql_query']}\n```"
            }
        })
    
    # Add the result text
    if data.get("text"):
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Results:*\n{data['text']}"
            }
        })
    
    # Add table if there is data
    if data.get("columns") and data.get("rows") and FORMAT_TABLES:
        try:
            # Log table structure
            logger.info(f"Creating table with {len(data['columns'])} columns and {len(data['rows'])} rows")

            # Create header row
            header_row = "| " + " | ".join(data["columns"]) + " |"
            divider_row = "| " + " | ".join(["---"] * len(data["columns"])) + " |"
    
            # Create data rows
            data_rows = []
            for i, row in enumerate(data["rows"]):
                # Ensure row is a list
                if not isinstance(row, (list, tuple)):
                    logger.warning(f"Row {i} is not a list or tuple: {type(row)}")
                    continue
                    
                # Ensure row length matches columns
                if len(row) != len(data["columns"]):
                    logger.warning(f"Row {i} has {len(row)} cells but should have {len(data['columns'])}")
                    row = row[:len(data["columns"])] if len(row) > len(data["columns"]) else row + [""] * (len(data["columns"]) - len(row))
                
                # Format each cell as string, handling potential None values
                formatted_cells = []
                for cell in row:
                    cell_str = str(cell) if cell is not None else ""
                    # Truncate long cell values
                    if len(cell_str) > 50:
                        cell_str = cell_str[:47] + "..."
                    formatted_cells.append(cell_str)
                
                formatted_row = "| " + " | ".join(formatted_cells) + " |"
                data_rows.append(formatted_row)
            
            # Combine into a markdown table
            markdown_table = header_row + "\n" + divider_row + "\n" + "\n".join(data_rows)
            
            # Check if table is too long for Slack (max 3000 chars in a single block)
            if len(markdown_table) > 2900:  # Leave some buffer
                logger.warning(f"Table is too large ({len(markdown_table)} chars), truncating")
                # Truncate rows to fit
                max_rows = min(10, len(data_rows))
                truncated_table = header_row + "\n" + divider_row + "\n" + "\n".join(data_rows[:max_rows])
                if max_rows < len(data_rows):
                    truncated_table += f"\n\n_Showing {max_rows} of {len(data_rows)} rows_"
                markdown_table = truncated_table
            
            # Add to blocks
            logger.info(f"Adding table block with {len(markdown_table)} characters")
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "```" + markdown_table + "```"
                }
            })
        except Exception as e:
            logger.error(f"Error formatting table: {str(e)}")
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Error formatting table: {str(e)}"
                }
            })
    
    # If we have no blocks, add a simple message
    if not blocks:
        logger.info("No content to display, adding default message")
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "No results to display."
            }
        })
    
    return blocks


@app.event("message")
def handle_message_events(body, logger):
    """Handle message events including bot_add"""
    event = body.get("event", {})
    
    # Handle bot being added to a channel
    if event.get("subtype") == "bot_add":
        logger.info(f"Bot was added to channel: {event.get('channel')}")
        return
    
    # If it's a regular message, process it through the message handler
    if event.get("type") == "message" and not event.get("subtype"):
        # Create a simplified message object for the handler
        message = {
            "text": event.get("text", ""),
            "user": event.get("user"),
            "channel": event.get("channel"),
            "ts": event.get("ts"),
            "thread_ts": event.get("thread_ts"),
            "channel_type": event.get("channel_type")
        }
        
        # Create a say function that uses the app client properly
        def say(msg_params):
            return app.client.chat_postMessage(**{
                "channel": message["channel"],
                **msg_params
            })
        
        # Process through the regular message handler
        handle_message(message, say)
    else:
        logger.info(f"Received message event: {body}")

@app.message(re.compile(".*"))
def handle_message(message, say):
    """Handle any message in the channel or DM"""
    # Log the full message for debugging
    logger.info(f"Received Slack message: {message}")
    
    # Ignore messages from bots to prevent loops
    if message.get("bot_id"):
        logger.info("Ignoring message from bot")
        return
    
    # Get the message text and user ID
    text = message.get("text", "").strip()
    user_id = message.get("user", "unknown_user")
    channel_type = message.get("channel_type", "")
    
    # Skip empty messages
    if not text:
        logger.info("Ignoring empty message")
        return
    
    # Allow messages from either:
    # 1. The configured channel
    # 2. Direct messages (im)
    if message.get("channel") != SLACK_CHANNEL_ID and channel_type != "im":
        logger.info(f"Ignoring message from channel {message.get('channel')} (not {SLACK_CHANNEL_ID} or DM)")
        return
    
    logger.info(f"Processing message: '{text}' from user: {user_id}")
    
    # Let the user know we're processing their query
    say({
        "text": f"Processing your query: '{text}'...",
        "thread_ts": message.get("ts")
    })
    
    try:
        logger.info(f"Querying Databricks Genie with question from user {user_id}: '{text}'")
        
        # Query Databricks Genie with the user's question
        result = genie_query(user_id, text)
        
        logger.info(f"Received response from Databricks Genie: conversation_id={result.get('conversation_id')}, message_id={result.get('message_id')}")
        
        # Check if there was an error
        if "error" in result:
            error_message = result.get("error", "Unknown error occurred")
            logger.error(f"Error from Databricks Genie: {error_message}")
            # Send error message
            say({
                "text": f"Sorry, I encountered an error: {error_message}",
                "thread_ts": message.get("ts")
            })
            return
        
        # Format the results for Slack
        formatted_result = result.get("result", {})
        logger.info(f"Raw result: {result}")  # Print the entire result object
        logger.info(f"Formatting result for Slack: text={len(formatted_result.get('text', ''))}, has_data={bool(formatted_result.get('rows'))}")
        
        blocks = format_dataframe_for_slack(formatted_result)
        logger.info(f"Generated {len(blocks)} blocks for Slack message")
        logger.info(f"Blocks content: {blocks}")  # Print the blocks for debugging
        
        # Send the response
        logger.info("Sending results to Slack")
        response_text = formatted_result.get("text", "Here are the results:")
        
        # Make sure we have a meaningful response
        if not response_text or response_text.strip() == "":
            response_text = "Results received but no explanatory text was provided."
        
        response = say({
            "blocks": blocks,
            "text": response_text,
            "thread_ts": message.get("ts")
        })
        logger.info(f"Slack response: {response}")  # Log the Slack API response
        logger.info(f"Sent response to Slack, thread: {message.get('ts')}")
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        # Send error message
        say({
            "text": f"Sorry, I encountered an error: {str(e)}",
            "thread_ts": message.get("ts")
        })

# Handle app_mention events (when someone @mentions the bot)
@app.event("app_mention")
def handle_mentions(body, say):
    """Handle when users mention the bot"""
    event = body.get("event", {})
    channel = event.get("channel")
    user = event.get("user")
    text = event.get("text", "").strip()
    
    # Remove the bot mention from the text
    # This assumes the mention is at the start of the message
    text = re.sub(r"<@[^>]+>\s*", "", text).strip()
    
    if text:
        # Process the message like a regular query
        handle_message(event, say)
    else:
        say({
            "text": "Hi! How can I help you analyze your Databricks usage?",
            "thread_ts": event.get("ts")
        })

def get_handler() -> SlackRequestHandler:
    """Returns the SlackRequestHandler for use with Flask"""
    return SlackRequestHandler(app)