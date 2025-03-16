"""
Utilities for interacting with the Databricks Genie API.
"""
import json
import time
import requests
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple

from databricks_genie_bot.config import (
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    SPACE_ID,
    MAINTAIN_CONTEXT,
    MAX_RETRIES,
    RETRY_INTERVAL,
)

# DICTIONARY TO STORE CONVERSATION CONTEXTS BY USER ID
conversation_contexts = {}

class ConversationContext:
    """Class to maintain conversation context with Databricks Genie"""
    
    def __init__(self):
        """Initialize a new conversation context"""
        self.conversation_id = None
        self.space_id = SPACE_ID
        self.auth_token = DATABRICKS_TOKEN
        self.host = DATABRICKS_HOST
    
    def reset(self):
        """Reset the conversation context"""
        self.conversation_id = None





# SATRT A CONVERSATION AND PRINT DEBUG INFO
def start_conversation(space_id: str, question: str, auth_token: str, host: str) -> Tuple[str, str]:
    """Start a new conversation with Databricks Genie"""
    url = f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation"
    
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    data = {
        "content": question
    }
    # Print debug information
    print(f"Making request to: {url}")
    print(f"Request headers: {headers}")
    print(f"Request data: {data}")
    
    try:
        response = requests.post(url, headers=headers, json=data)
        print(f"Response status: {response.status_code}")
        print(f"Response content: {response.text}")
        response.raise_for_status()
        
        result = response.json()
        conversation_id = result["conversation_id"]
        message_id = result["message_id"]
    
        return conversation_id, message_id
    except requests.exceptions.RequestException as e:
        print(f"Request error: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        raise





# ADD A MESSAGE TO A CONVERSATION (WHICH CONVERSATION??? NEED TO RETURN CONVERSATION ID)
def add_message_to_conversation(space_id: str, conversation_id: str, question: str, auth_token: str, host: str) -> str:
    """Add a message to an existing conversation"""
    url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    data = {
        "content": question
    }
    # Print debug information for add_message
    print(f"Adding message - URL: {url}")
    print(f"Request data: {data}")
    response = requests.post(url, headers=headers, json=data)
    print(f"Response status: {response.status_code}")
    print(f"Response content: {response.text}")
    response.raise_for_status()
    result = response.json()
    message_id = result["message_id"]
    return message_id




# GET QUERY MESSAGE
def get_query_message(space_id: str, conversation_id: str, message_id: str, auth_token: str, host: str) -> Dict:
    """Get details of a message"""
    url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()




# GET QUERY RESULTS
def get_query_results(space_id: str, conversation_id: str, message_id: str, auth_token: str, host: str) -> Dict:
    """Get query results for a message"""
    url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()





##EXPERI02
def process_query_result(result: Dict) -> Optional[Dict[str, Any]]:
    """Process the query result and return formatted data"""
    print(f"Processing query result: {result}")

    formatted_result = {
        "text": "",
        "query_description": "",
        "sql_query": "",
        "columns": [],
        "rows": []
    }

    # Extract the statement_response from the result
    statement_response = result.get("statement_response", {})
    
    # Get the query status
    status = statement_response.get("status", {}).get("state", "")
    
    # Check if query succeeded but returned no data
    if status == "SUCCEEDED" and not (statement_response.get("result", {}).get("data_typed_array", [])):
        formatted_result["text"] = (
            "The query completed successfully but returned no data. This could mean:\n"
            "• The data might not exist for the specified parameters\n"
            "• You might need additional permissions\n\n"
            "Try:\n"
            "• Verifying the parameters in your query\n"
            "• Checking your access permissions"
        )
        return formatted_result
    
    # Check if there are data results
    if "manifest" in statement_response and "result" in statement_response:
        manifest = statement_response["manifest"]
        result_data = statement_response["result"]
        
        # Extract column names from schema
        if manifest and "schema" in manifest and "columns" in manifest["schema"]:
            formatted_result["columns"] = [field.get("name", "") for field in manifest["schema"]["columns"]]
        
        # Extract rows of data
        if result_data and "data_typed_array" in result_data:
            for row in result_data["data_typed_array"]:
                if row and "values" in row:
                    formatted_result["rows"].append([value.get("str", None) for value in row["values"]])
        
        print(f"Extracted {len(formatted_result['rows'])} rows of data with {len(formatted_result['columns'])} columns")

        # Format the results into a readable message
        if formatted_result["rows"] and formatted_result["columns"]:
            # For single value results (like sum or count), make it more readable
            if len(formatted_result["columns"]) == 1 and len(formatted_result["rows"]) == 1:
                col_name = formatted_result["columns"][0]
                value = formatted_result["rows"][0][0]
                formatted_result["text"] += f"Result: {col_name} = {value}\n"
    
    return formatted_result
#EXPERI02







####*******************************QUERY DATA EXPERITEST 00*************************####
def query_data(space_id: str, question: str, auth_token: str, host: str, 
               conversation_id: Optional[str] = None, max_retries: int = MAX_RETRIES, 
               retry_interval: int = RETRY_INTERVAL) -> Dict[str, Any]:
    """Query Databricks with a question, handling conversation context"""
    try:
        print(f"Starting query with max_retries={max_retries}, retry_interval={retry_interval}")
        # Start a new conversation or add to existing one
        if conversation_id is None:
            conversation_id, message_id = start_conversation(space_id, question, auth_token, host)
        else:
            message_id = add_message_to_conversation(space_id, conversation_id, question, auth_token, host)

        # Poll for results with retries
        for attempt in range(max_retries):
            try:
                print(f"Attempt {attempt+1}/{max_retries}: Waiting {retry_interval} seconds before checking status")
                time.sleep(retry_interval)
                
                print(f"Checking message status for conversation_id={conversation_id}, message_id={message_id}")
                message_data = get_query_message(space_id, conversation_id, message_id, auth_token, host)
                print(f"Message data: {message_data}")
                status = message_data.get("status", "")
                print(f"Message status: {status}")

                if status in ["COMPLETE", "COMPLETED"]:
                    print(f"Query completed successfully (status: {status}), retrieving results")
                    
                    # First check for text response in attachments
                    if "attachments" in message_data:
                        for attachment in message_data["attachments"]:
                            # Handle text response
                            if "text" in attachment and "content" in attachment["text"]:
                                return {
                                    "conversation_id": conversation_id,
                                    "message_id": message_id,
                                    "result": {
                                        "text": attachment["text"]["content"],
                                        "query_description": "",
                                        "sql_query": "",
                                        "columns": [],
                                        "rows": []
                                    }
                                }
                            # Handle query response
                            elif "query" in attachment:
                                query_description = attachment["query"].get("description", "")
                                sql_query = attachment["query"].get("query", "")
                                
                                # Check if query contains placeholder values
                                if "<current_workspace_id>" in sql_query:
                                    return {
                                        "conversation_id": conversation_id,
                                        "message_id": message_id,
                                        "result": {
                                            "text": (
                                                "I notice you're querying workspace-specific data. "
                                                "Please specify which workspace you'd like to analyze. For example:\n"
                                                "• 'Show me the top spender in workspace 123456'\n"
                                                "• 'Who used the most compute in workspace my-workspace-name'"
                                            ),
                                            "query_description": query_description,
                                            "sql_query": sql_query,
                                            "columns": [],
                                            "rows": []
                                        }
                                    }
                                
                                # Get and process results for database queries
                                result_data = get_query_results(space_id, conversation_id, message_id, auth_token, host)
                                processed_result = process_query_result(result_data)
                                
                                if processed_result:
                                    processed_result["query_description"] = query_description
                                    processed_result["sql_query"] = sql_query
                                    
                                return {
                                    "conversation_id": conversation_id,
                                    "message_id": message_id,
                                    "result": processed_result
                                }

                # IF QUERY FAILED
                if status == "ERROR":
                    error_message = message_data.get("error_message", "Unknown error occurred")
                    print(f"Query failed with error: {error_message}")
                    raise Exception(f"Query failed: {error_message}")               
                # Check for results even if status is not 'COMPLETED' as the API might behave differently
                if attempt > 3 and status in ["IN_PROGRESS", "PENDING", "RUNNING"]:
                    try:
                        print(f"Attempting to get results even though status is {status}")
                        result_data = get_query_results(space_id, conversation_id, message_id, auth_token, host)  
                        # If we get here without an exception, process the results
                        processed_result = process_query_result(result_data)
                        print(f"Got results despite status being {status}")
                        return {
                            "conversation_id": conversation_id,
                            "message_id": message_id,
                            "result": processed_result,
                            "note": f"Results retrieved while status was '{status}'"
                        }
                    except Exception as result_e:
                        print(f"Could not get results while status is {status}: {str(result_e)}")
                # Continue polling if still in progress
                print(f"Query still in progress (status: {status}), continuing to poll")
            except requests.exceptions.RequestException as e:
                # Handle transient API errors
                print(f"Request exception on attempt {attempt+1}/{max_retries}: {str(e)}")
                if attempt == max_retries - 1:
                    raise e       
        # If we've exhausted all retries
        print(f"Query timed out after {max_retries} attempts with {retry_interval}s intervals")
        raise Exception(f"Query timed out after {max_retries} attempts")       
    except Exception as e:
        # Handle any exceptions
        return {
            "conversation_id": conversation_id,
            "message_id": None,
            "result": {
                "text": f"Error querying data: {str(e)}",
                "query_description": "",
                "sql_query": "",
                "columns": [],
                "rows": []
            },
            "error": str(e)
        }






# GENIE QUERY
def genie_query(user_id: str, question: str) -> Dict[str, Any]:
    """Query Databricks Genie, maintaining conversation context by user"""
    # Get or create conversation context for this user
    if MAINTAIN_CONTEXT and user_id in conversation_contexts:
        context = conversation_contexts[user_id]
    else:
        context = ConversationContext()
        if MAINTAIN_CONTEXT:
            conversation_contexts[user_id] = context
    
    # Query data using the context
    result = query_data(
        space_id=context.space_id,
        question=question,
        auth_token=context.auth_token,
        host=context.host,
        conversation_id=context.conversation_id
    )
    
    # Update the context with the new conversation ID if this is the first query
    if context.conversation_id is None and "conversation_id" in result:
        context.conversation_id = result["conversation_id"]
    
    return result



