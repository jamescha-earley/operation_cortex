import streamlit as st
import pandas as pd
import random
import requests
import json
import snowflake.connector
import re
import time
from datetime import datetime, timedelta
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
    text = text.replace('thatquestion', 'that question')
    text = text.replace('knowthe', 'know the')
    
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
    
    def execute_non_query(self, query: str) -> bool:
        """Execute SQL command that doesn't return data (INSERT, UPDATE, DELETE, CREATE)"""
        try:
            if not self.password or not self.account:
                return False
            
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
                # Execute query
                cursor = conn.cursor()
                cursor.execute(query.replace(';', ''))
                return True
                
            finally:
                # Always close the connection
                conn.close()
                
        except snowflake.connector.errors.DatabaseError as e:
            st.error(f"Snowflake database error: {str(e)}")
            return False
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
            return False
    
    def save_leaderboard_entry(self, agent_name: str, completion_time: int, correct_answers: int, 
                              total_questions: int, accuracy: float, score: int) -> bool:
        """Save a leaderboard entry to the database"""
        insert_sql = f"""
        INSERT INTO AGENT_LEADERBOARD 
        (AGENT_NAME, COMPLETION_TIME, CORRECT_ANSWERS, TOTAL_QUESTIONS, ACCURACY, SCORE, MISSION_TIMESTAMP)
        VALUES 
        ('{agent_name}', {completion_time}, {correct_answers}, {total_questions}, {accuracy}, {score}, CURRENT_TIMESTAMP())
        """
        return self.execute_non_query(insert_sql)
    
    def get_leaderboard_from_db(self, limit: int = 10) -> Optional[pd.DataFrame]:
        """Get leaderboard data from database for today only"""
        # Get best score for each agent from today only
        query = f"""
        WITH ranked_scores AS (
            SELECT 
                AGENT_NAME,
                COMPLETION_TIME,
                CORRECT_ANSWERS,
                TOTAL_QUESTIONS,
                ACCURACY,
                SCORE,
                MISSION_TIMESTAMP,
                ROW_NUMBER() OVER (PARTITION BY UPPER(AGENT_NAME) ORDER BY SCORE DESC, COMPLETION_TIME ASC) as rn
            FROM AGENT_LEADERBOARD
            WHERE DATE(MISSION_TIMESTAMP) = CURRENT_DATE()
        )
        SELECT 
            AGENT_NAME,
            COMPLETION_TIME,
            CORRECT_ANSWERS,
            TOTAL_QUESTIONS,
            ACCURACY,
            SCORE,
            MISSION_TIMESTAMP
        FROM ranked_scores 
        WHERE rn = 1
        ORDER BY SCORE DESC, COMPLETION_TIME ASC
        LIMIT {limit}
        """
        return self.execute_query(query)
    
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
        "question": "What was the keyphrase mentioned in mission M013 (Toronto location)?",
        "answer": "Red Falcon",
        "hint": "Look specifically for mission M013 in the Toronto location and find the keyphrase mentioned.",
        "cortex_query": "What is the keyphrase mentioned in mission M013 that took place in Toronto?"
    }
]

# Leaderboard functions
def save_to_leaderboard(agent_name: str, completion_time: int, correct_answers: int, total_questions: int):
    """Save player results to leaderboard (both session state and database)"""
    if 'leaderboard' not in st.session_state:
        st.session_state.leaderboard = []
    
    # Calculate score (prioritize correct answers, then speed)
    accuracy = correct_answers / total_questions
    # Score: accuracy weight (70%) + speed weight (30%), normalized to 1000 points
    speed_score = max(0, 1000 - completion_time) / 1000  # Lower time = higher score
    total_score = int((accuracy * 700) + (speed_score * 300))
    
    new_entry = {
        'agent_name': agent_name,
        'completion_time': completion_time,
        'correct_answers': correct_answers,
        'total_questions': total_questions,
        'accuracy': f"{accuracy*100:.1f}%",
        'score': total_score,
        'timestamp': datetime.now()
    }
    
    # Save to database if Snowflake is available
    sf = get_snowflake_connector()
    db_saved = False
    
    if sf.password:
        try:
            # Save to database
            db_saved = sf.save_leaderboard_entry(
                agent_name=agent_name,
                completion_time=completion_time,
                correct_answers=correct_answers,
                total_questions=total_questions,
                accuracy=accuracy,
                score=total_score
            )
            
            if db_saved:
                st.success("✅ Results saved to database!")
            else:
                st.warning("⚠️ Could not save to database, using session storage.")
                
        except Exception as e:
            st.warning(f"⚠️ Database error: {str(e)}. Using session storage.")
    
    # Handle session state leaderboard (fallback or supplement)
    existing_index = None
    for i, entry in enumerate(st.session_state.leaderboard):
        if entry['agent_name'].lower() == agent_name.lower():
            existing_index = i
            break
    
    # If agent exists, update only if new score is better
    if existing_index is not None:
        existing_entry = st.session_state.leaderboard[existing_index]
        if total_score > existing_entry['score']:
            st.session_state.leaderboard[existing_index] = new_entry
            if not db_saved:  # Only show this if not already shown for DB save
                st.success(f"🎉 New personal best! Previous score: {existing_entry['score']}")
        else:
            if not db_saved:  # Only show this if not already shown for DB save
                st.info(f"Score: {total_score}. Your best remains: {existing_entry['score']}")
            return  # Don't update leaderboard
    else:
        # New agent, add to leaderboard
        st.session_state.leaderboard.append(new_entry)
    
    # Sort by score (highest first), then by time (fastest first)
    st.session_state.leaderboard.sort(key=lambda x: (-x['score'], x['completion_time']))
    
    # Keep only top 10
    st.session_state.leaderboard = st.session_state.leaderboard[:10]

def load_leaderboard_from_db():
    """Load leaderboard from database if available"""
    sf = get_snowflake_connector()
    
    if not sf.password:
        return None
    
    try:
        # Try to get leaderboard from database
        df = sf.get_leaderboard_from_db(10)
        
        if df is not None and not df.empty:
            # Convert to session state format
            leaderboard = []
            for _, row in df.iterrows():
                entry = {
                    'agent_name': row['AGENT_NAME'],
                    'completion_time': int(row['COMPLETION_TIME']),
                    'correct_answers': int(row['CORRECT_ANSWERS']),
                    'total_questions': int(row['TOTAL_QUESTIONS']),
                    'accuracy': f"{float(row['ACCURACY'])*100:.1f}%",
                    'score': int(row['SCORE']),
                    'timestamp': row['MISSION_TIMESTAMP']
                }
                leaderboard.append(entry)
            
            return leaderboard
        
    except Exception as e:
        st.warning(f"Could not load leaderboard from database: {str(e)}")
    
    return None

def show_leaderboard():
    """Display the leaderboard"""
    st.subheader("🏆 Agent Leaderboard")
    
    # Try to load from database first
    db_leaderboard = load_leaderboard_from_db()
    
    if db_leaderboard:
        leaderboard_data = db_leaderboard
    elif 'leaderboard' in st.session_state and st.session_state.leaderboard:
        leaderboard_data = st.session_state.leaderboard
    else:
        st.info("No agents have completed missions today.")
        return
    
    leaderboard_df = pd.DataFrame(leaderboard_data)
    
    # Format the display
    display_df = leaderboard_df[['agent_name', 'score', 'accuracy', 'completion_time', 'correct_answers', 'total_questions']].copy()
    display_df.columns = ['Agent Name', 'Score', 'Accuracy', 'Time (s)', 'Correct', 'Total']
    display_df.index = range(1, len(display_df) + 1)
    
    st.dataframe(display_df, use_container_width=True)

def format_time(seconds):
    """Format time in MM:SS format"""
    minutes = int(seconds // 60)
    seconds = int(seconds % 60)
    return f"{minutes:02d}:{seconds:02d}"

def show_timer_sidebar():
    """Display timer in sidebar"""
    if 'start_time' in st.session_state and st.session_state.start_time:
        elapsed_time = time.time() - st.session_state.start_time
        
        with st.sidebar:
            st.markdown("### ⏱️ Mission Timer")
            st.markdown(f"## {format_time(elapsed_time)}")
            
            # Show progress - handle completion state
            if st.session_state.game_completed:
                progress = 1.0
                progress_text = "Mission Complete!"
            else:
                progress = (st.session_state.current_step - 1) / 2
                progress_text = f"Step {st.session_state.current_step}/2"
            
            st.progress(progress, text=progress_text)
            
            st.markdown("---")
            
            # Show leaderboard table in sidebar
            st.subheader("🏆 Today's Leaderboard")
            
            # Use same data source as main leaderboard
            db_leaderboard = load_leaderboard_from_db()
            
            if db_leaderboard:
                # Show top 5 in table format for sidebar
                top_agents = db_leaderboard[:5]
                sidebar_df = pd.DataFrame(top_agents)
                
                # Create compact display dataframe
                display_df = sidebar_df[['agent_name', 'score', 'accuracy']].copy()
                display_df.columns = ['Agent', 'Score', 'Accuracy']
                display_df.index = range(1, len(display_df) + 1)
                
                st.dataframe(display_df, use_container_width=True, hide_index=False)
            else:
                st.info("No rankings yet today")

def initialize_session_state():
    """Initialize all session state variables"""
    defaults = {
        'game_started': False,
        'game_completed': False,
        'current_step': 1,
        'selected_questions': random.sample(MISSION_QUESTIONS, 2),
        'correct_answers': 0,
        'total_answers': 0,
        'cortex_response': None,
        'cortex_sql': None,
        'cortex_citations': [],
        'start_time': None,
        'completion_time': None,
        'agent_name': '',
        'leaderboard': []
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
    2. For each step, you must solve one intelligence puzzle using our advanced Cortex Agent AI
    
    ### Mission Objectives:
    - Analyze intercepted messages
    - Review spy reports
    - Identify double agents
    - Locate compromised safe houses
    
    ### How to Use Cortex Agents:
    - The Cortex Agents will help you analyze intelligence data
    - You can ask Cortex Agents specific questions about the missions
    - Use the intelligence provided to solve each mission step
                
    **Cortex Agents utilize two specialized tools:**
    - **Cortex Search**: Searches through unstructured mission documents and intelligence reports
    - **Cortex Analyst**: Analyzes structured data from spy databases and generates SQL queries
    
    The agents automatically choose the right tool based on your question to provide comprehensive intelligence analysis.
    
    ### Mission Success Criteria:
    Complete both intelligence tasks as quickly and accurately as possible to earn a high score on the leaderboard!
    
    *This briefing will self-destruct when you begin the mission.*
    """)
    
    # Check for existing names in database
    def check_name_exists(name: str) -> bool:
        """Check if agent name already exists in database"""
        if not sf.password:
            # If no database, check session state
            if 'leaderboard' in st.session_state:
                return any(entry['agent_name'].lower() == name.lower() for entry in st.session_state.leaderboard)
            return False
        
        try:
            query = f"SELECT COUNT(*) as count FROM AGENT_LEADERBOARD WHERE UPPER(AGENT_NAME) = UPPER('{name}')"
            result = sf.execute_query(query)
            if result is not None and not result.empty:
                return result.iloc[0]['COUNT'] > 0
        except Exception:
            pass
        return False
    
    # Agent name input
    agent_name = st.text_input("Enter your agent codename:", placeholder="Your Name")
    
    # Check if name already exists
    name_exists = False
    if agent_name.strip():
        name_exists = check_name_exists(agent_name.strip())
        if name_exists:
            st.error("🚫 This agent codename is already taken. Please choose a different name.")
    
    if st.button("BEGIN MISSION", key="start_button", type="primary", disabled=not agent_name.strip() or name_exists):
        st.session_state.agent_name = agent_name.strip()
        st.session_state.game_started = True
        st.session_state.start_time = time.time()
        st.rerun()

def show_game_screen():
    # Show timer in sidebar
    show_timer_sidebar()
    
    if st.session_state.game_completed:
        show_mission_complete()
        return
    
    # Header
    st.title(f"🕵️ Operation Cortex: Step {st.session_state.current_step}/2")
    st.markdown(f"**Agent: {st.session_state.agent_name}**")
    
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
            value="What is your question?",
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
        
        if submitted and user_answer.strip():
            st.session_state.total_answers += 1
            
            if user_answer.lower() == current_question["answer"].lower():
                st.success("✅ Correct! Intelligence verified.")
                st.session_state.correct_answers += 1
            else:
                st.error(f"❌ Incorrect. The correct answer was: **{current_question['answer']}**")
            
            # Advance to next step or complete game
            if st.session_state.current_step < 2:
                st.session_state.current_step += 1
                # Reset Cortex responses for next step
                st.session_state.cortex_response = None
                st.session_state.cortex_sql = None
                st.session_state.cortex_citations = []
                st.rerun()
            else:
                # Calculate completion time
                st.session_state.completion_time = int(time.time() - st.session_state.start_time)
                st.session_state.game_completed = True
                st.rerun()

def show_mission_complete():
    st.title("🕵️ Mission Debriefing")
    
    # Calculate final score and save to leaderboard
    if st.session_state.completion_time and st.session_state.agent_name:
        save_to_leaderboard(
            st.session_state.agent_name,
            st.session_state.completion_time,
            st.session_state.correct_answers,
            2  # total questions
        )
    
    # Show results
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Mission Results")
        st.metric("Correct Answers", f"{st.session_state.correct_answers}/2")
        st.metric("Completion Time", format_time(st.session_state.completion_time))
        
        accuracy = st.session_state.correct_answers / 2
        st.metric("Accuracy", f"{accuracy*100:.1f}%")
    
    with col2:
        st.markdown("### 🏆 Your Ranking")
        # Find current agent's position in leaderboard
        current_score = 0
        position = "Not ranked"
        
        # Try to get position from database leaderboard first
        db_leaderboard = load_leaderboard_from_db()
        
        if db_leaderboard:
            # Use database leaderboard for position
            for i, entry in enumerate(db_leaderboard):
                if entry['agent_name'].lower() == st.session_state.agent_name.lower():
                    position = f"#{i + 1} of {len(db_leaderboard)}"
                    current_score = entry['score']
                    break
        elif 'leaderboard' in st.session_state and st.session_state.leaderboard:
            # Fallback to session state leaderboard
            for i, entry in enumerate(st.session_state.leaderboard):
                if entry['agent_name'].lower() == st.session_state.agent_name.lower():
                    position = f"#{i + 1} of {len(st.session_state.leaderboard)}"
                    current_score = entry['score']
                    break
        
        st.metric("Final Score", current_score)
        st.metric("Leaderboard Position", position)
    
    # Mission outcome message
    if st.session_state.correct_answers == 2:
        st.markdown("""
        ## 🌟 MISSION ACCOMPLISHED 🌟
        
        **Congratulations, Agent!**
        
        You have successfully completed all intelligence tasks with perfect accuracy. Your analytical skills have proven invaluable to the agency.
        
        The intelligence you've gathered will be crucial for our ongoing operations. Your performance has been noted in your service record.
        """)
    elif st.session_state.correct_answers == 1:
        st.markdown("""
        ## 🔍 MISSION PARTIALLY SUCCESSFUL
        
        **Agent,**
        
        You successfully completed 50% of the intelligence tasks. While not perfect, you've gathered valuable intelligence for the agency.
        
        Consider reviewing the mission data more carefully in future operations.
        """)
    else:
        st.markdown("""
        ## ⚠️ MISSION NEEDS IMPROVEMENT
        
        **Agent,**
        
        While you completed the mission timeline, the intelligence gathered was not accurate. 
        
        Additional training is recommended before your next field assignment.
        """)
    
    # Show leaderboard
    st.markdown("---")
    show_leaderboard()
    
    if st.button("START NEW MISSION", type="primary"):
        # Reset game state but keep leaderboard
        keys_to_reset = ['game_started', 'game_completed', 'current_step', 'correct_answers', 
                        'total_answers', 'cortex_response', 'cortex_sql', 'cortex_citations',
                        'start_time', 'completion_time', 'agent_name']
        
        for key in keys_to_reset:
            if key in st.session_state:
                if key == 'current_step':
                    st.session_state[key] = 1
                elif key in ['correct_answers', 'total_answers']:
                    st.session_state[key] = 0
                elif key in ['game_started', 'game_completed']:
                    st.session_state[key] = False
                else:
                    st.session_state[key] = None if key != 'agent_name' else ''
        
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