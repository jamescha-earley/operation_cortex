import streamlit as st
import pandas as pd
import random
import requests
import json
import snowflake.connector
import re
from typing import Optional, Dict, Any, List, Tuple

# Configuration
st.set_page_config(
    page_title="Operation Cortex",
    page_icon="🕵️",
    layout="wide"
)

# Snowflake connection configuration using Streamlit secrets
def get_snowflake_config():
    """Get Snowflake configuration from Streamlit secrets"""
    try:
        return {
            'account': st.secrets["snowflake"]["account"],
            'username': st.secrets["snowflake"]["username"],
            'password': st.secrets["snowflake"]["pat"],
            'warehouse': st.secrets["snowflake"].get("warehouse", "SPY_AGENCY_WH"),
            'database': st.secrets["snowflake"].get("database", "SPY_AGENCY"),
            'schema': st.secrets["snowflake"].get("schema", "INTEL")
        }
    except KeyError as e:
        st.error(f"Missing Streamlit secret: {e}")
        return {
            'account': None,
            'username': None,
            'password': None,
            'warehouse': "SPY_AGENCY_WH",
            'database': "SPY_AGENCY",
            'schema': "INTEL"
        }
    except Exception as e:
        st.error(f"Error reading Streamlit secrets: {e}")
        return {
            'account': None,
            'username': None,
            'password': None,
            'warehouse': "SPY_AGENCY_WH",
            'database': "SPY_AGENCY",
            'schema': "INTEL"
        }

# Cortex API configuration
CORTEX_SEARCH_SERVICES = "spy_agency.intel.spy_mission_search"
SEMANTIC_MODELS = "@spy_agency.intel.stage/spy_report_semantic_model.yaml"

def clean_text(text: str) -> str:
    """Clean text from UTF-8 encoding artifacts and problematic characters"""
    if not text:
        return text
    
    # Remove specific UTF-8 encoding artifacts we're seeing
    text = text.replace('ã\x80\x90', '')  # Remove this artifact
    text = text.replace('â\x80', '')      # Remove this artifact  
    text = text.replace('ã\x80\x91', '')  # Remove this artifact
    text = text.replace('\x80', '')       # Remove any remaining \x80 bytes
    text = text.replace('\x90', '')       # Remove any remaining \x90 bytes
    text = text.replace('\x91', '')       # Remove any remaining \x91 bytes
    
    # Fix common word concatenation issues
    text = text.replace('Thecodeword', 'The codeword')
    text = text.replace('Thekeyphrase', 'The keyphrase')
    text = text.replace('Theagent', 'The agent')
    text = text.replace('Themission', 'The mission')
    text = text.replace('Thelocation', 'The location')
    text = text.replace('Idon\'t', 'I don\'t')
    text = text.replace('Ican\'t', 'I can\'t')
    text = text.replace('Iwill', 'I will')
    text = text.replace('Iam', 'I am')
    text = text.replace('Idon\'t', 'I don\'t')
    text = text.replace('Ican\'t', 'I can\'t')
    text = text.replace('Iwill', 'I will')
    text = text.replace('Iam', 'I am')
    
    # Remove standalone numbers that are artifacts (like "1" between words)
    text = re.sub(r'\s+\d+\s+\.', ' .', text)  # Remove numbers before periods
    text = re.sub(r'"\s*\d+\s*\.', '".', text)  # Remove numbers after quotes before periods
    
    # Clean up any double spaces that might result
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    return text

class SnowflakeConnector:
    def __init__(self):
        # Get configuration from Streamlit secrets
        config = get_snowflake_config()
        self.account = config['account']
        self.username = config['username']
        self.password = config['password']  # Using 'password' key which contains the PAT
        self.warehouse = config['warehouse']
        self.database = config['database']
        self.schema = config['schema']
        
        # Only set base_url if account is available
        if self.account:
            self.base_url = f"https://SFDEVREL-ENTERPRISE.snowflakecomputing.com/api/v2"
        else:
            self.base_url = None
        
        self.session_token = None
        
    def get_auth_headers(self) -> dict:
        """Get authentication headers for PAT"""
        return {
            "Authorization": f"Bearer {self.password}",
            "Content-Type": "application/json"
        }
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute SQL query using Snowflake Python Connector"""
        try:
            if not self.password or not self.account:
                return None
            
            # Create connection using Snowflake Python connector
            conn = snowflake.connector.connect(
                account=self.account,
                user=self.username,
                password=self.password, 
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            
            try:
                # Execute query and fetch results
                cursor = conn.cursor()
                cursor.execute(query.replace(';', ''))
                
                # Fetch all results
                results = cursor.fetchall()
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Create DataFrame
                df = pd.DataFrame(results, columns=columns)
                
                return df
                
            finally:
                # Always close the connection
                conn.close()
                
        except snowflake.connector.errors.DatabaseError as e:
            st.error(f"Snowflake database error: {str(e)}")
            return None
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
            return None
    
    def call_cortex_api(self, query: str, limit: int = 10) -> Optional[requests.Response]:
        """Make a call to Snowflake Cortex API and return raw response"""
        if not self.password or not self.base_url:
            return None
            
        payload = {
            "model": "claude-3-5-sonnet",
            "response-instruction": "You will always maintain a serious tone and provide concise response, you will always refer the user as Agent.",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": query
                        }
                    ]
                }
            ],
            "tools": [
                {
                    "tool_spec": {
                        "type": "cortex_analyst_text_to_sql",
                        "name": "analyst1"
                    }
                },
                {
                    "tool_spec": {
                        "type": "cortex_search",
                        "name": "search1"
                    }
                }
            ],
            "tool_resources": {
                "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
                "search1": {
                    "name": CORTEX_SEARCH_SERVICES,
                    "max_results": limit,
                    "id_column": "mission_id"
                }
            }
        }
        
        try:
            cortex_url = f"{self.base_url}/cortex/agent:run"
            headers = self.get_auth_headers()
            
            response = requests.post(cortex_url, headers=headers, json=payload, timeout=50, stream=True)
            
            if response.status_code == 200:
                return response
            else:
                st.error(f"❌ Cortex API Error: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            st.error(f"Error calling Cortex API: {str(e)}")
            return None

# Initialize Snowflake connector
@st.cache_resource
def get_snowflake_connector():
    return SnowflakeConnector()

def process_sse_response(response) -> Tuple[str, str, List[Dict]]:
    """Process SSE response from Snowflake API with text cleaning"""
    text = ""
    sql = ""
    citations = []
    
    if not response:
        return text, sql, citations
    
    try:
        # If response is a requests.Response object (SSE stream)
        if hasattr(response, 'iter_lines'):
            events = []
            current_event = {}
            
            # Parse SSE stream with proper encoding
            for line in response.iter_lines(decode_unicode=True):
                if line is None:
                    continue
                    
                line = line.strip()
                
                if line.startswith('event:'):
                    if current_event:  # Save previous event
                        events.append(current_event)
                    current_event = {'event': line[6:].strip()}
                elif line.startswith('data:'):
                    data_str = line[5:].strip()
                    if data_str and data_str != '[DONE]':
                        try:
                            current_event['data'] = json.loads(data_str)
                        except json.JSONDecodeError:
                            # Clean the data string before storing
                            current_event['data'] = clean_text(data_str)
                elif line == '':
                    # Empty line indicates end of event
                    if current_event:
                        events.append(current_event)
                        current_event = {}
            
            # Don't forget the last event
            if current_event:
                events.append(current_event)
            
            # Process the parsed events
            response = events
        
        # Handle different response formats
        if isinstance(response, list):
            # SSE format (list of events)
            for event in response:
                if event.get('event') == "message.delta":
                    data = event.get('data', {})
                    delta = data.get('delta', {})
                    
                    for content_item in delta.get('content', []):
                        content_type = content_item.get('type')
                        if content_type == "tool_results":
                            tool_results = content_item.get('tool_results', {})
                            if 'content' in tool_results:
                                for result in tool_results['content']:
                                    if result.get('type') == 'json':
                                        json_data = result.get('json', {})
                                        # Clean the text before adding
                                        raw_text = json_data.get('text', '')
                                        text += clean_text(raw_text)
                                        search_results = json_data.get('searchResults', [])
                                        for search_result in search_results:
                                            citations.append({
                                                'source_id': search_result.get('source_id', ''),
                                                'doc_id': search_result.get('doc_id', '')
                                            })
                                        sql = json_data.get('sql', '')
                        elif content_type == 'text':
                            # Clean the text before adding
                            raw_text = content_item.get('text', '')
                            text += clean_text(raw_text)
                            
        elif isinstance(response, dict):
            # Direct response format
            if 'choices' in response:
                for choice in response['choices']:
                    message = choice.get('message', {})
                    content = message.get('content', [])
                    
                    for item in content:
                        if item.get('type') == 'text':
                            # Clean the text before adding
                            raw_text = item.get('text', '')
                            text += clean_text(raw_text)
                        elif item.get('type') == 'tool_results':
                            tool_results = item.get('tool_results', {})
                            if 'content' in tool_results:
                                for result in tool_results['content']:
                                    if result.get('type') == 'json':
                                        json_data = result.get('json', {})
                                        # Clean the text before adding
                                        raw_text = json_data.get('text', '')
                                        text += clean_text(raw_text)
                                        sql = json_data.get('sql', '')
                                        search_results = json_data.get('searchResults', [])
                                        for search_result in search_results:
                                            citations.append({
                                                'source_id': search_result.get('source_id', ''),
                                                'doc_id': search_result.get('doc_id', '')
                                            })
            # Handle message content directly
            elif 'message' in response:
                message = response.get('message', {})
                content = message.get('content', [])
                for item in content:
                    if item.get('type') == 'text':
                        # Clean the text before adding
                        raw_text = item.get('text', '')
                        text += clean_text(raw_text)
                        
    except Exception as e:
        st.error(f"Error processing response: {str(e)}")
    
    # Final cleaning of the accumulated text
    text = clean_text(text)
                
    return text, sql, citations

def get_fallback_data():
    """Return fallback data when Snowflake is not available"""
    missions_df = pd.DataFrame([
        {'mission_id': 'M001', 'message_text': 'Package secured. Rendezvous at midnight. Codeword: Shadow.', 'sender_code': 'X7A', 'receiver_code': 'K9Q', 'mission_location': 'Berlin', 'transmission_date': '2025-03-10 23:45:00', 'encryption_level': 'HIGH'},
        {'mission_id': 'M003', 'message_text': 'Possible breach. Suspect a mole in the network.', 'sender_code': 'K9Q', 'receiver_code': 'B3Z', 'mission_location': 'Paris', 'transmission_date': '2025-03-09 18:20:00', 'encryption_level': 'HIGH'},
        {'mission_id': 'M008', 'message_text': 'The safe house is compromised. Move to Sector 9.', 'sender_code': 'K9Q', 'receiver_code': 'T2M', 'mission_location': 'Tokyo', 'transmission_date': '2025-03-04 14:20:00', 'encryption_level': 'HIGH'},
        {'mission_id': 'M013', 'message_text': 'Final instructions in the briefcase. Retrieve immediately. Keyphrase: Red Falcon.', 'sender_code': 'L3P', 'receiver_code': 'T2M', 'mission_location': 'Toronto', 'transmission_date': '2025-02-27 11:40:00', 'encryption_level': 'HIGH'}
    ])
    
    reports_df = pd.DataFrame([
        {'report_id': 'R001', 'agent_code': 'X7A', 'mission_id': 'M001', 'mission_outcome': 'Success', 'suspected_double_agent': False, 'last_known_location': 'Berlin', 'failure_reason': 'N/A'},
        {'report_id': 'R003', 'agent_code': 'K9Q', 'mission_id': 'M003', 'mission_outcome': 'Failed', 'suspected_double_agent': True, 'last_known_location': 'Paris', 'failure_reason': 'Double agent interference'},
        {'report_id': 'R008', 'agent_code': 'K9Q', 'mission_id': 'M008', 'mission_outcome': 'Compromised', 'suspected_double_agent': True, 'last_known_location': 'Tokyo', 'failure_reason': 'Safe house compromised'},
        {'report_id': 'R013', 'agent_code': 'L3P', 'mission_id': 'M013', 'mission_outcome': 'Compromised', 'suspected_double_agent': False, 'last_known_location': 'Toronto', 'failure_reason': 'Intercepted courier'}
    ])
    
    return missions_df, reports_df

def get_spy_data():
    """Fetch spy data from Snowflake or return fallback data"""
    sf = get_snowflake_connector()
    
    if not sf.password:
        st.warning("⚠️ Snowflake PAT not configured. Using fallback data for demo.")
        return get_fallback_data()
    
    try:
        missions_df = sf.execute_query("SELECT * FROM spy_missions")
        reports_df = sf.execute_query("SELECT * FROM spy_reports")
        
        if missions_df is None or reports_df is None:
            st.warning("⚠️ Could not fetch data from Snowflake. Using fallback data.")
            return get_fallback_data()
            
        return missions_df, reports_df
        
    except Exception as e:
        st.warning(f"⚠️ Snowflake connection failed: {str(e)}. Using fallback data.")
        return get_fallback_data()

# Pre-defined mission questions and answers
MISSION_QUESTIONS = [
    {
        "mission_id": "M003",
        "question": "Identify the double agent in Paris. Who was the operative that compromised mission M003?",
        "answer": "K9Q",
        "hint": "Check the mission reports for Paris operations and look for suspected double agents.",
        "cortex_query": "Who was the agent for mission M003 and are they suspected as a double agent?"
    },
    {
        "mission_id": "M008",
        "question": "Locate the compromised safe house. In which city was our safe house compromised?",
        "answer": "Tokyo",
        "hint": "Review the mission reports with 'compromised' status.",
        "cortex_query": "In which city was the safe house compromised?"
    },
    {
        "mission_id": "M001",
        "question": "What is the codeword for the Berlin rendezvous?",
        "answer": "Shadow",
        "hint": "Check the intercepted messages from Berlin operations.",
        "cortex_query": "What is the codeword mentioned in the Berlin mission?"
    },
    {
        "mission_id": "M013",
        "question": "What was the keyphrase mentioned in the Toronto mission?",
        "answer": "Red Falcon",
        "hint": "Review the intercepted messages from Toronto.",
        "cortex_query": "What keyphrase was mentioned in the Toronto mission?"
    }
]

def initialize_session_state():
    """Initialize all session state variables"""
    defaults = {
        'game_started': False,
        'game_completed': False,
        'current_step': 1,
        'selected_questions': random.sample(MISSION_QUESTIONS, 2),
        'correct_answers': 0,
        'cortex_response': None,
        'cortex_sql': None,
        'cortex_citations': []
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

def show_start_screen():
    st.title("🕵️ Operation Cortex")
    
    # Check Snowflake configuration
    sf = get_snowflake_connector()
    
    st.markdown("""
    ## Top Secret Briefing
    
    **Agent,**
    
    Welcome to Operation Cortex. Your mission, should you choose to accept it, consists of 2 crucial intelligence-gathering tasks:
    
    1. You will be given access to our database of intercepted messages and mission reports
    2. For each step, you must solve one intelligence puzzle using our advanced Cortex AI assistant
    
    ### Mission Objectives:
    - Analyze intercepted messages
    - Review spy reports
    - Identify double agents
    - Locate compromised safe houses
    
    ### How to Use Cortex Agents:
    - The Cortex Agents will help you analyze intelligence data
    - You can ask Cortex Agents specific questions about the missions
    - Use the intelligence provided to solve each mission step
    
    ### Mission Success Criteria:
    Successfully complete both intelligence tasks to complete the operation.
    
    *This briefing will self-destruct when you begin the mission.*
    """)
    
    if st.button("BEGIN MISSION", key="start_button", type="primary"):
        st.session_state.game_started = True
        st.rerun()

def show_game_screen():
    if st.session_state.game_completed:
        show_mission_complete()
        return
    
    # Header
    st.title(f"🕵️ Operation Cortex: Step {st.session_state.current_step}/2")
    
    # Get current question
    current_question = st.session_state.selected_questions[st.session_state.current_step - 1]
    
    # Display question
    st.markdown(f"## Intelligence Task #{st.session_state.current_step}")
    st.markdown(f"**{current_question['question']}**")
    
    # Load data
    missions_df, reports_df = get_spy_data()
    
    # Show mission data
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📡 Intercepted Messages")
        st.dataframe(missions_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.subheader("📋 Mission Reports")
        st.dataframe(reports_df, use_container_width=True, hide_index=True)
    
    # Cortex Agents Section
    st.subheader("🤖 Cortex Agents")
    
    sf = get_snowflake_connector()
    if not sf.password:
        st.warning("⚠️ Cortex Agents requires Snowflake PAT. Analyze the data manually using the tables above.")
    
    # Cortex query input
    with st.form(key="cortex_form"):
        cortex_query = st.text_input(
            "Ask Cortex Agents a question:",
            value=current_question["cortex_query"],
            key="cortex_input"
        )
        
        submitted = st.form_submit_button("Query Cortex Agents", disabled=not sf.password)
        
        if submitted and sf.password:
            with st.spinner("Cortex Agents processing..."):
                # Call the Snowflake Cortex API
                response = sf.call_cortex_api(cortex_query, 1)
                
                # Process the response
                text, sql, citations = process_sse_response(response)
                
                # Store in session state
                st.session_state.cortex_response = text
                st.session_state.cortex_sql = sql
                st.session_state.cortex_citations = citations
                
                # Rerun to refresh the UI
                st.rerun()
    
    # Display Cortex response
    if st.session_state.cortex_response:
        st.markdown("### Cortex Agents Response:")
        
        # Clean the response to remove encoding artifacts
        cleaned_response = clean_text(st.session_state.cortex_response)
        
        # Display the cleaned response
        st.markdown(cleaned_response)
        
        # Display citations/mission details if available
        if st.session_state.cortex_citations:
            st.markdown("### Mission Details:")
            for citation in st.session_state.cortex_citations:
                mission_id = citation.get("doc_id", "")
                if mission_id:
                    query = f"SELECT * FROM spy_missions WHERE mission_id = '{mission_id}'"
                    result = sf.execute_query(query)
                    if result is not None and not result.empty:
                        st.dataframe(result, hide_index=True)
        
        # Display SQL if available
        if st.session_state.cortex_sql:
            with st.expander("View Generated SQL"):
                st.code(st.session_state.cortex_sql, language="sql")
                
                # Execute and show results
                results = sf.execute_query(st.session_state.cortex_sql)
                if results is not None and not results.empty:
                    st.markdown("### SQL Results:")
                    st.dataframe(results, hide_index=True)
    
    # Answer form
    with st.form(key=f"step_{st.session_state.current_step}_form"):
        user_answer = st.text_input("Enter your answer to complete this step:")
        
        # Hint expander
        with st.expander("Need a hint?"):
            st.write(current_question["hint"])
        
        submitted = st.form_submit_button("Submit Answer", type="primary")
        
        if submitted:
            if user_answer.lower() == current_question["answer"].lower():
                st.success("✅ Correct! Intelligence verified.")
                st.session_state.correct_answers += 1
                
                # Advance to next step or complete game
                if st.session_state.current_step < 2:
                    st.session_state.current_step += 1
                    # Reset Cortex responses for next step
                    st.session_state.cortex_response = None
                    st.session_state.cortex_sql = None
                    st.session_state.cortex_citations = []
                    st.rerun()
                else:
                    st.session_state.game_completed = True
                    st.rerun()
            else:
                st.error("❌ Incorrect. Review the intelligence and try again.")

def show_mission_complete():
    st.title("🕵️ Mission Debriefing")
    
    if st.session_state.correct_answers == 2:
        st.markdown("""
        ## 🌟 MISSION ACCOMPLISHED 🌟
        
        **Congratulations, Agent!**
        
        You have successfully completed all intelligence tasks. Your analytical skills have proven invaluable to the agency.
        
        The intelligence you've gathered will be crucial for our ongoing operations. Your performance has been noted in your service record.
        """)
    else:
        st.markdown("""
        ## MISSION PARTIALLY SUCCESSFUL
        
        **Agent,**
        
        While you were able to gather some valuable intelligence, not all tasks were completed successfully.
        
        Further training may be required before your next field assignment.
        """)
    
    if st.button("START NEW MISSION", type="primary"):
        # Reset game state
        for key in ['game_started', 'game_completed', 'current_step', 'correct_answers', 
                   'cortex_response', 'cortex_sql', 'cortex_citations']:
            if key in st.session_state:
                if key == 'current_step':
                    st.session_state[key] = 1
                elif key == 'correct_answers':
                    st.session_state[key] = 0
                else:
                    st.session_state[key] = False if 'game' in key else None
        
        st.session_state.selected_questions = random.sample(MISSION_QUESTIONS, 2)
        st.rerun()

def main():
    initialize_session_state()
    
    # Start screen
    if not st.session_state.game_started:
        show_start_screen()
    else:
        # Game is in progress
        show_game_screen()

if __name__ == "__main__":
    main()