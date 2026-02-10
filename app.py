import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
from thefuzz import process

# --- CONFIGURATION ---
st.set_page_config(page_title="Skylark Ops Agent", layout="wide")

# --- AUTHENTICATION & CONNECTION ---
@st.cache_resource
def init_connection():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    except:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    
    client = gspread.authorize(creds)
    return client

def load_data(client):
    try:
        sh = client.open("Drone_Operations_DB")
        pilots_ws = sh.worksheet("Pilots")
        drones_ws = sh.worksheet("Drones")
        missions_ws = sh.worksheet("Missions")

        pilots_df = pd.DataFrame(pilots_ws.get_all_records())
        drones_df = pd.DataFrame(drones_ws.get_all_records())
        missions_df = pd.DataFrame(missions_ws.get_all_records())
        
        return pilots_df, drones_df, missions_df, pilots_ws, drones_ws, missions_ws
    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {e}")
        return None, None, None, None, None, None

# --- CONVERSATION CONTEXT MANAGER ---
class ConversationContext:
    def __init__(self):
        if 'context' not in st.session_state:
            st.session_state.context = {
                'awaiting_response': False,
                'current_action': None,
                'temp_data': {},
                'last_query': None
            }
    
    def set_awaiting(self, action, data=None):
        st.session_state.context['awaiting_response'] = True
        st.session_state.context['current_action'] = action
        if data:
            st.session_state.context['temp_data'] = data
    
    def clear(self):
        st.session_state.context = {
            'awaiting_response': False,
            'current_action': None,
            'temp_data': {},
            'last_query': None
        }
    
    def get_context(self):
        return st.session_state.context
    
    def is_awaiting(self):
        return st.session_state.context.get('awaiting_response', False)

# --- ENHANCED AGENT LOGIC WITH CRUD ---
class OpsAgent:
    def __init__(self, pilots, drones, missions):
        self.pilots = pilots
        self.drones = drones
        self.missions = missions
        self.context = ConversationContext()

    def check_conflicts(self):
        conflicts = []
        
        for idx, pilot in self.pilots.iterrows():
            if pilot['current_assignment'] and pilot['current_assignment'] != "–":
                mission = self.missions[self.missions['project_id'] == pilot['current_assignment']]
                
                if not mission.empty:
                    m_data = mission.iloc[0]
                    m_end = pd.to_datetime(m_data['end_date'])
                    p_avail = pd.to_datetime(pilot['available_from'])
                    if p_avail > m_end:
                         conflicts.append({
                            "type": "Pilot Date Overlap",
                            "entity": pilot['name'],
                            "detail": f"Assigned to {pilot['current_assignment']} but unavailable until {pilot['available_from']}",
                            "severity": "High"
                        })

                    req_skills = [x.strip() for x in m_data['required_skills'].split(',')]
                    pilot_skills = pilot['skills']
                    missing = [s for s in req_skills if s not in pilot_skills]
                    if missing:
                        conflicts.append({
                            "type": "Skill Mismatch",
                            "entity": pilot['name'],
                            "detail": f"Missing {missing} for {pilot['current_assignment']}",
                            "severity": "Medium"
                        })

        for idx, drone in self.drones.iterrows():
            if drone['current_assignment'] and drone['current_assignment'] != "–":
                if drone['status'] == 'Maintenance':
                    conflicts.append({
                        "type": "Drone in Maintenance",
                        "entity": drone['drone_id'],
                        "detail": f"Assigned to {drone['current_assignment']} but status is Maintenance",
                        "severity": "High"
                    })
                
                mission = self.missions[self.missions['project_id'] == drone['current_assignment']]
                if not mission.empty:
                    mission_loc = mission.iloc[0]['location']
                    if drone['location'] != mission_loc:
                        conflicts.append({
                            "type": "Drone Location Mismatch",
                            "entity": drone['drone_id'],
                            "detail": f"Drone is in {drone['location']} but Project is in {mission_loc}",
                            "severity": "Medium"
                        })

        return conflicts
    
    def recommend_replacement(self, project_id):
        """Find best pilot replacements for a project"""
        project_id = str(project_id).strip().upper()
        mission_rows = self.missions[self.missions['project_id'] == project_id]
        
        if mission_rows.empty:
            return pd.DataFrame()
            
        mission = mission_rows.iloc[0]
        req_skills = [x.strip() for x in mission['required_skills'].split(',')]
        location = mission['location']
        
        candidates = self.pilots[
            (self.pilots['status'] == 'Available') & 
            (self.pilots['location'] == location)
        ].copy()
        
        if candidates.empty:
            return pd.DataFrame()
            
        candidates['score'] = candidates['skills'].apply(
            lambda x: sum(1 for s in req_skills if s in x)
        )
        
        best_matches = candidates.sort_values(by='score', ascending=False)
        available_cols = [col for col in ['name', 'pilot_id', 'skills', 'location', 'score'] if col in best_matches.columns]
        return best_matches[available_cols]

    def get_available_resources(self, resource_type='all'):
        """Get available pilots and/or drones"""
        result = {}
        if resource_type in ['pilot', 'all']:
            result['pilots'] = self.pilots[self.pilots['status'] == 'Available']
        if resource_type in ['drone', 'all']:
            result['drones'] = self.drones[self.drones['status'] == 'Available']
        return result
    
    def get_non_working_resources(self, resource_type='all'):
        """Get non-working pilots and/or drones"""
        result = {}
        if resource_type in ['pilot', 'all']:
            result['pilots'] = self.pilots[self.pilots['status'] != 'Available']
        if resource_type in ['drone', 'all']:
            result['drones'] = self.drones[self.drones['status'] != 'Available']
        return result

    def get_pilot_info(self, pilot_id):
        pilot_data = self.pilots[self.pilots['pilot_id'] == pilot_id]
        if not pilot_data.empty:
            return pilot_data.iloc[0].to_dict()
        return None
    
    def get_drone_info(self, drone_id):
        drone_data = self.drones[self.drones['drone_id'] == drone_id]
        if not drone_data.empty:
            return drone_data.iloc[0].to_dict()
        return None
    
    def get_mission_info(self, project_id):
        project_id = str(project_id).strip().upper()
        mission_data = self.missions[self.missions['project_id'] == project_id]
        if not mission_data.empty:
            return mission_data.iloc[0].to_dict()
        return None

    # ========== CREATE OPERATIONS ==========
    
    def add_pilot(self, pilot_data, worksheet):
        """Add a new pilot to the database"""
        try:
            # Get all current data to find next ID
            all_pilots = self.pilots
            if not all_pilots.empty and 'pilot_id' in all_pilots.columns:
                # Extract numeric part and increment
                max_num = max([int(p[1:]) for p in all_pilots['pilot_id'] if p.startswith('P')])
                new_id = f"P{str(max_num + 1).zfill(3)}"
            else:
                new_id = "P001"
            
            pilot_data['pilot_id'] = new_id
            
            # Append to sheet
            row = [pilot_data.get(col, '') for col in worksheet.row_values(1)]
            worksheet.append_row(row)
            
            return new_id
        except Exception as e:
            return str(e)
    
    def add_drone(self, drone_data, worksheet):
        """Add a new drone to the database"""
        try:
            all_drones = self.drones
            if not all_drones.empty and 'drone_id' in all_drones.columns:
                max_num = max([int(d[1:]) for d in all_drones['drone_id'] if d.startswith('D')])
                new_id = f"D{str(max_num + 1).zfill(3)}"
            else:
                new_id = "D001"
            
            drone_data['drone_id'] = new_id
            
            row = [drone_data.get(col, '') for col in worksheet.row_values(1)]
            worksheet.append_row(row)
            
            return new_id
        except Exception as e:
            return str(e)
    
    def add_mission(self, mission_data, worksheet):
        """Add a new mission to the database"""
        try:
            all_missions = self.missions
            if not all_missions.empty and 'project_id' in all_missions.columns:
                max_num = max([int(m[3:]) for m in all_missions['project_id'] if m.startswith('PRJ')])
                new_id = f"PRJ{str(max_num + 1).zfill(3)}"
            else:
                new_id = "PRJ001"
            
            mission_data['project_id'] = new_id
            
            row = [mission_data.get(col, '') for col in worksheet.row_values(1)]
            worksheet.append_row(row)
            
            return new_id
        except Exception as e:
            return str(e)

    # ========== UPDATE OPERATIONS ==========
    
    def assign_pilot_to_mission(self, pilot_id, project_id, pilots_ws, missions_ws):
        """Assign a pilot to a mission"""
        try:
            pilot_cell = pilots_ws.find(pilot_id)
            headers = pilots_ws.row_values(1)
            
            assignment_col = headers.index('current_assignment') + 1
            status_col = headers.index('status') + 1
            
            pilots_ws.update_cell(pilot_cell.row, assignment_col, project_id)
            pilots_ws.update_cell(pilot_cell.row, status_col, 'Assigned')
            
            mission_cell = missions_ws.find(project_id)
            mission_headers = missions_ws.row_values(1)
            pilot_assignment_col = mission_headers.index('assigned_pilot') + 1
            
            missions_ws.update_cell(mission_cell.row, pilot_assignment_col, pilot_id)
            
            return True
        except Exception as e:
            return str(e)
    
    def assign_drone_to_mission(self, drone_id, project_id, drones_ws, missions_ws):
        """Assign a drone to a mission"""
        try:
            drone_cell = drones_ws.find(drone_id)
            headers = drones_ws.row_values(1)
            
            assignment_col = headers.index('current_assignment') + 1
            status_col = headers.index('status') + 1
            
            drones_ws.update_cell(drone_cell.row, assignment_col, project_id)
            drones_ws.update_cell(drone_cell.row, status_col, 'Assigned')
            
            mission_cell = missions_ws.find(project_id)
            mission_headers = missions_ws.row_values(1)
            drone_assignment_col = mission_headers.index('assigned_drone') + 1
            
            missions_ws.update_cell(mission_cell.row, drone_assignment_col, drone_id)
            
            return True
        except Exception as e:
            return str(e)

    def update_pilot_status(self, pilot_id, new_status, worksheet):
        try:
            cell = worksheet.find(pilot_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            return True
        except Exception as e:
            return str(e)
    
    def update_drone_status(self, drone_id, new_status, worksheet):
        try:
            cell = worksheet.find(drone_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            return True
        except Exception as e:
            return str(e)
    
    def update_mission_status(self, project_id, new_status, worksheet):
        try:
            cell = worksheet.find(project_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            return True
        except Exception as e:
            return str(e)
    
    def update_pilot_field(self, pilot_id, field_name, new_value, worksheet):
        """Update any field of a pilot"""
        try:
            cell = worksheet.find(pilot_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index(field_name) + 1
            worksheet.update_cell(cell.row, col_idx, new_value)
            return True
        except Exception as e:
            return str(e)
    
    def update_drone_field(self, drone_id, field_name, new_value, worksheet):
        """Update any field of a drone"""
        try:
            cell = worksheet.find(drone_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index(field_name) + 1
            worksheet.update_cell(cell.row, col_idx, new_value)
            return True
        except Exception as e:
            return str(e)
    
    def update_mission_field(self, project_id, field_name, new_value, worksheet):
        """Update any field of a mission"""
        try:
            cell = worksheet.find(project_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index(field_name) + 1
            worksheet.update_cell(cell.row, col_idx, new_value)
            return True
        except Exception as e:
            return str(e)

    # ========== DELETE OPERATIONS ==========
    
    def delete_pilot(self, pilot_id, worksheet):
        """Delete a pilot from the database"""
        try:
            cell = worksheet.find(pilot_id)
            worksheet.delete_rows(cell.row)
            return True
        except Exception as e:
            return str(e)
    
    def delete_drone(self, drone_id, worksheet):
        """Delete a drone from the database"""
        try:
            cell = worksheet.find(drone_id)
            worksheet.delete_rows(cell.row)
            return True
        except Exception as e:
            return str(e)
    
    def delete_mission(self, project_id, worksheet):
        """Delete a mission from the database"""
        try:
            cell = worksheet.find(project_id)
            worksheet.delete_rows(cell.row)
            return True
        except Exception as e:
            return str(e)


def parse_intent(user_input):
    """Enhanced intent classifier with CRUD support"""
    user_input_lower = user_input.lower()
    
    intents = {
        "create": ["add", "create", "new", "register"],
        "delete": ["delete", "remove", "deactivate"],
        "edit": ["edit", "modify", "change", "update field"],
        "assign": ["assign", "assignment", "allocate", "give"],
        "show_roster": ["show all drones", "list all drones", "show all pilots", "list all pilots",
                        "all drones", "all pilots", "show drones", "list drones", 
                        "show pilots", "list pilots", "roster"],
        "show_missions": ["show missions", "list missions", "projects", "show projects", 
                          "active missions", "all missions"],
        "show_available": ["show available", "list available", "available", "working", "free", "ready"],
        "show_non_working": ["not working", "not available", "unavailable", "in maintenance", 
                             "broken", "down", "out of service", "offline"],
        "recommend": ["recommend", "replacement", "who can", "suggest"],
        "update_status": ["update status", "set status", "mark as", "change status"],
        "check_status": ["status of", "check status", "is available"],
        "find_info": ["find", "show", "get", "details", "info", "where is", "who is"]
    }
    
    detected_intent = None
    
    # Priority matching
    if any(k in user_input_lower for k in ["delete", "remove"]):
        detected_intent = "delete"
    elif any(k in user_input_lower for k in ["edit", "modify", "change field", "update field"]):
        detected_intent = "edit"
    elif any(k in user_input_lower for k in ["add", "create", "new", "register"]):
        detected_intent = "create"
    elif any(k in user_input_lower for k in ["not working", "not available", "broken", "down", "offline", "unavailable"]):
        detected_intent = "show_non_working"
    elif any(k in user_input_lower for k in ["assign", "assignment", "allocate"]):
        detected_intent = "assign"
    elif any(k in user_input_lower for k in ["show available", "list available"]):
        detected_intent = "show_available"
    elif any(k in user_input_lower for k in ["show all drones", "list all drones", "all drones", "show drones", "list drones"]):
        detected_intent = "show_roster"
    elif any(k in user_input_lower for k in ["show all pilots", "list all pilots", "all pilots", "show pilots", "list pilots"]):
        detected_intent = "show_roster"
    elif any(k in user_input_lower for k in ["show missions", "list missions", "active missions", "all missions"]):
        detected_intent = "show_missions"
    else:
        for intent, keywords in intents.items():
            if intent not in ["create", "delete", "edit", "assign", "show_roster", "show_missions", "show_available", "show_non_working"]:
                if any(k in user_input_lower for k in keywords):
                    detected_intent = intent
                    break
    
    # Extract Entity IDs
    words = user_input.replace("?", "").replace(".", "").split()
    entity_id = None
    entity_type = None
    
    for w in words:
        w_upper = w.upper()
        if w_upper.startswith("PRJ") and any(c.isdigit() for c in w_upper):
            entity_id = w_upper
            entity_type = "mission"
            break
        elif w_upper.startswith("P0") and any(c.isdigit() for c in w_upper):
            entity_id = w_upper
            entity_type = "pilot"
            break
        elif w_upper.startswith("D0") and any(c.isdigit() for c in w_upper):
            entity_id = w_upper
            entity_type = "drone"
            break
            
    return detected_intent, entity_id, entity_type, user_input_lower


def get_safe_columns(df, desired_cols):
    """Helper function to get columns that actually exist in the dataframe"""
    return [col for col in desired_cols if col in df.columns]


def main():
    st.title("🚁 Skylark Conversational Ops Agent")
    st.caption("🤖 AI-Powered Operations Assistant with CRUD | Privacy Mode: On")
    
    # Sidebar
    st.sidebar.header("System Status")
    client = init_connection()
    
    if client:
        st.sidebar.success("✅ Database Connected")
        if st.sidebar.button("🔄 Sync Data"):
            st.cache_data.clear()
            st.rerun()
    
    # Load Data
    pilots_df, drones_df, missions_df, pilots_ws, drones_ws, missions_ws = load_data(client)
    
    if pilots_df is not None:
        agent = OpsAgent(pilots_df, drones_df, missions_df)
        
        tab1, tab2, tab3 = st.tabs(["💬 Ops Console", "📊 Roster & Fleet", "⚠️ Conflicts"])
        
        with tab1:
            st.info("💡 **Try CRUD operations!** Examples:\n- 'Add new pilot'\n- 'Delete drone D005'\n- 'Edit pilot P001'\n- 'Assign pilot to mission'\n- 'Show available drones'")
            
            if "messages" not in st.session_state:
                st.session_state.messages = []

            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

            if prompt := st.chat_input("What would you like to do?"):
                st.session_state.messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.markdown(prompt)

                ctx = agent.context.get_context()
                response = ""
                
                # Handle conversation flows
                if agent.context.is_awaiting():
                    current_action = ctx['current_action']
                    temp_data = ctx['temp_data']
                    
                    # ========== CREATE FLOWS ==========
                    if current_action == 'awaiting_create_type':
                        if 'pilot' in prompt.lower():
                            agent.context.set_awaiting('awaiting_pilot_details')
                            response = "📝 **Let's add a new pilot!**\n\nPlease provide:\n"
                            response += "- Name\n- Location\n- Skills (comma-separated)\n- Available From (YYYY-MM-DD)\n\n"
                            response += "Example: 'John Doe, Mumbai, Mapping,Inspection, 2024-01-15'"
                        elif 'drone' in prompt.lower():
                            agent.context.set_awaiting('awaiting_drone_details')
                            response = "📝 **Let's add a new drone!**\n\nPlease provide:\n"
                            response += "- Model\n- Location\n- Flight Hours\n- Last Maintenance (YYYY-MM-DD)\n\n"
                            response += "Example: 'DJI Phantom 5, Delhi, 120, 2024-01-10'"
                        elif 'mission' in prompt.lower():
                            agent.context.set_awaiting('awaiting_mission_details')
                            response = "📝 **Let's create a new mission!**\n\nPlease provide:\n"
                            response += "- Project Name\n- Location\n- Start Date (YYYY-MM-DD)\n- End Date (YYYY-MM-DD)\n- Required Skills (comma-separated)\n\n"
                            response += "Example: 'Pipeline Survey, Bangalore, 2024-02-01, 2024-02-15, Mapping,Thermal'"
                        else:
                            response = "What would you like to add? **Pilot**, **Drone**, or **Mission**?"
                    
                    elif current_action == 'awaiting_pilot_details':
                        parts = [p.strip() for p in prompt.split(',')]
                        if len(parts) >= 4:
                            pilot_data = {
                                'name': parts[0],
                                'location': parts[1],
                                'skills': parts[2],
                                'available_from': parts[3],
                                'status': 'Available',
                                'current_assignment': '–'
                            }
                            result = agent.add_pilot(pilot_data, pilots_ws)
                            if result.startswith('P'):
                                response = f"✅ **Pilot Added Successfully!**\n\nID: {result}\nName: {parts[0]}\nLocation: {parts[1]}"
                                st.cache_data.clear()
                            else:
                                response = f"❌ Failed to add pilot: {result}"
                        else:
                            response = "⚠️ Missing information. Please provide: Name, Location, Skills, Available From"
                        agent.context.clear()
                    
                    elif current_action == 'awaiting_drone_details':
                        parts = [p.strip() for p in prompt.split(',')]
                        if len(parts) >= 4:
                            drone_data = {
                                'model': parts[0],
                                'location': parts[1],
                                'flight_hours': parts[2],
                                'last_maintenance': parts[3],
                                'status': 'Available',
                                'current_assignment': '–'
                            }
                            result = agent.add_drone(drone_data, drones_ws)
                            if result.startswith('D'):
                                response = f"✅ **Drone Added Successfully!**\n\nID: {result}\nModel: {parts[0]}\nLocation: {parts[1]}"
                                st.cache_data.clear()
                            else:
                                response = f"❌ Failed to add drone: {result}"
                        else:
                            response = "⚠️ Missing information. Please provide: Model, Location, Flight Hours, Last Maintenance"
                        agent.context.clear()
                    
                    elif current_action == 'awaiting_mission_details':
                        parts = [p.strip() for p in prompt.split(',')]
                        if len(parts) >= 5:
                            mission_data = {
                                'project_name': parts[0],
                                'location': parts[1],
                                'start_date': parts[2],
                                'end_date': parts[3],
                                'required_skills': parts[4],
                                'status': 'Active',
                                'assigned_pilot': '–',
                                'assigned_drone': '–'
                            }
                            result = agent.add_mission(mission_data, missions_ws)
                            if result.startswith('PRJ'):
                                response = f"✅ **Mission Created Successfully!**\n\nID: {result}\nName: {parts[0]}\nLocation: {parts[1]}"
                                st.cache_data.clear()
                            else:
                                response = f"❌ Failed to create mission: {result}"
                        else:
                            response = "⚠️ Missing information. Please provide: Name, Location, Start Date, End Date, Required Skills"
                        agent.context.clear()
                    
                    # ========== DELETE FLOWS ==========
                    elif current_action == 'awaiting_delete_confirmation':
                        entity_id = temp_data['entity_id']
                        entity_type = temp_data['entity_type']
                        
                        if 'yes' in prompt.lower() or 'confirm' in prompt.lower():
                            if entity_type == 'pilot':
                                result = agent.delete_pilot(entity_id, pilots_ws)
                                target = "Pilot"
                            elif entity_type == 'drone':
                                result = agent.delete_drone(entity_id, drones_ws)
                                target = "Drone"
                            elif entity_type == 'mission':
                                result = agent.delete_mission(entity_id, missions_ws)
                                target = "Mission"
                            
                            if result is True:
                                response = f"✅ **{target} {entity_id} has been deleted.**"
                                st.cache_data.clear()
                            else:
                                response = f"❌ Delete failed: {result}"
                        else:
                            response = "❌ Delete cancelled."
                        agent.context.clear()
                    
                    # ========== EDIT FLOWS ==========
                    elif current_action == 'awaiting_field_to_edit':
                        field_name = prompt.strip().lower()
                        temp_data['field_name'] = field_name
                        agent.context.set_awaiting('awaiting_new_value', temp_data)
                        response = f"What should the new value for **{field_name}** be?"
                    
                    elif current_action == 'awaiting_new_value':
                        entity_id = temp_data['entity_id']
                        entity_type = temp_data['entity_type']
                        field_name = temp_data['field_name']
                        new_value = prompt.strip()
                        
                        if entity_type == 'pilot':
                            result = agent.update_pilot_field(entity_id, field_name, new_value, pilots_ws)
                            target = "Pilot"
                        elif entity_type == 'drone':
                            result = agent.update_drone_field(entity_id, field_name, new_value, drones_ws)
                            target = "Drone"
                        elif entity_type == 'mission':
                            result = agent.update_mission_field(entity_id, field_name, new_value, missions_ws)
                            target = "Mission"
                        
                        if result is True:
                            response = f"✅ **Updated {target} {entity_id}**\n{field_name} = {new_value}"
                            st.cache_data.clear()
                        else:
                            response = f"❌ Update failed: {result}"
                        agent.context.clear()
                    
                    # ========== ASSIGNMENT FLOWS ==========
                    elif current_action == 'awaiting_assignment_type':
                        if 'pilot' in prompt.lower():
                            agent.context.set_awaiting('awaiting_mission_for_pilot_assignment')
                            response = "📋 **Which mission should I assign the pilot to?**\n\nActive missions:\n\n"
                            active = missions_df[missions_df['status'] == 'Active']
                            mission_cols = get_safe_columns(active, ['project_id', 'project_name', 'location', 'required_skills'])
                            if mission_cols:
                                response += active[mission_cols].to_markdown(index=False)
                            else:
                                response += active.to_markdown(index=False)
                            response += "\n\nPlease tell me the Project ID (e.g., PRJ001)"
                        
                        elif 'drone' in prompt.lower():
                            agent.context.set_awaiting('awaiting_mission_for_drone_assignment')
                            response = "📋 **Which mission should I assign the drone to?**\n\nActive missions:\n\n"
                            active = missions_df[missions_df['status'] == 'Active']
                            mission_cols = get_safe_columns(active, ['project_id', 'project_name', 'location'])
                            if mission_cols:
                                response += active[mission_cols].to_markdown(index=False)
                            else:
                                response += active.to_markdown(index=False)
                            response += "\n\nPlease tell me the Project ID (e.g., PRJ001)"
                        else:
                            response = "I didn't catch that. Are you assigning a **pilot** or a **drone**?"
                    
                    elif current_action == 'awaiting_mission_for_pilot_assignment':
                        _, mission_id, _, _ = parse_intent(prompt)
                        if mission_id:
                            temp_data['mission_id'] = mission_id
                            agent.context.set_awaiting('awaiting_pilot_selection', temp_data)
                            
                            recs = agent.recommend_replacement(mission_id)
                            if not recs.empty:
                                response = f"✅ **Available pilots for {mission_id}:**\n\n"
                                response += recs.to_markdown(index=False)
                                response += "\n\n👉 **Which pilot would you like to assign?** (Tell me the Pilot ID, e.g., P001)"
                            else:
                                response = f"❌ No available pilots found for mission {mission_id}. Would you like to:\n1. See all available pilots anyway?\n2. Choose a different mission?"
                                agent.context.clear()
                        else:
                            response = "I need a valid Project ID (e.g., PRJ001). Which mission?"
                    
                    elif current_action == 'awaiting_pilot_selection':
                        _, pilot_id, _, _ = parse_intent(prompt)
                        if pilot_id:
                            mission_id = temp_data['mission_id']
                            result = agent.assign_pilot_to_mission(pilot_id, mission_id, pilots_ws, missions_ws)
                            if result is True:
                                response = f"✅ **Assignment Complete!**\n\n"
                                response += f"Pilot {pilot_id} has been assigned to {mission_id}.\n"
                                response += f"Status updated to 'Assigned'."
                                st.cache_data.clear()
                            else:
                                response = f"❌ Assignment failed: {result}"
                            agent.context.clear()
                        else:
                            response = "I need a valid Pilot ID (e.g., P001). Which pilot should I assign?"
                    
                    elif current_action == 'awaiting_mission_for_drone_assignment':
                        _, mission_id, _, _ = parse_intent(prompt)
                        if mission_id:
                            temp_data['mission_id'] = mission_id
                            agent.context.set_awaiting('awaiting_drone_selection', temp_data)
                            
                            available_drones = drones_df[drones_df['status'] == 'Available']
                            if not available_drones.empty:
                                response = f"✅ **Available drones:**\n\n"
                                drone_cols = get_safe_columns(available_drones, ['drone_id', 'model', 'location', 'flight_hours'])
                                if drone_cols:
                                    response += available_drones[drone_cols].to_markdown(index=False)
                                else:
                                    response += available_drones.to_markdown(index=False)
                                response += "\n\n👉 **Which drone would you like to assign?** (Tell me the Drone ID, e.g., D001)"
                            else:
                                response = "❌ No drones are currently available."
                                agent.context.clear()
                        else:
                            response = "I need a valid Project ID (e.g., PRJ001). Which mission?"
                    
                    elif current_action == 'awaiting_drone_selection':
                        _, drone_id, _, _ = parse_intent(prompt)
                        if drone_id:
                            mission_id = temp_data['mission_id']
                            result = agent.assign_drone_to_mission(drone_id, mission_id, drones_ws, missions_ws)
                            if result is True:
                                response = f"✅ **Assignment Complete!**\n\n"
                                response += f"Drone {drone_id} has been assigned to {mission_id}.\n"
                                response += f"Status updated to 'Assigned'."
                                st.cache_data.clear()
                            else:
                                response = f"❌ Assignment failed: {result}"
                            agent.context.clear()
                        else:
                            response = "I need a valid Drone ID (e.g., D001). Which drone should I assign?"
                
                else:
                    # NEW CONVERSATION
                    intent, entity_id, entity_type, clean_text = parse_intent(prompt)
                    
                    # Smart name lookup
                    if not entity_id:
                        for idx, row in pilots_df.iterrows():
                            if row['name'].lower() in clean_text:
                                entity_id = row['pilot_id']
                                entity_type = "pilot"
                                break
                        
                        if not entity_id:
                            for idx, row in drones_df.iterrows():
                                if row['model'].lower() in clean_text:
                                    entity_id = row['drone_id']
                                    entity_type = "drone"
                                    break

                    # ============ INTENT HANDLERS ============
                    
                    # CREATE
                    if intent == "create":
                        agent.context.set_awaiting('awaiting_create_type')
                        response = "➕ **What would you like to add?**\n- Pilot\n- Drone\n- Mission"
                    
                    # DELETE
                    elif intent == "delete":
                        if entity_id:
                            agent.context.set_awaiting('awaiting_delete_confirmation', {
                                'entity_id': entity_id,
                                'entity_type': entity_type
                            })
                            response = f"⚠️ **Are you sure you want to delete {entity_type} {entity_id}?**\n\nType 'yes' to confirm or 'no' to cancel."
                        else:
                            response = "What would you like to delete? Please specify an ID (e.g., 'Delete P001', 'Remove D003')"
                    
                    # EDIT
                    elif intent == "edit":
                        if entity_id:
                            # Show current info
                            if entity_type == 'pilot':
                                info = agent.get_pilot_info(entity_id)
                            elif entity_type == 'drone':
                                info = agent.get_drone_info(entity_id)
                            elif entity_type == 'mission':
                                info = agent.get_mission_info(entity_id)
                            
                            if info:
                                response = f"✏️ **Editing {entity_type.title()} {entity_id}**\n\nCurrent values:\n"
                                for key, value in info.items():
                                    response += f"- {key}: {value}\n"
                                response += "\n**Which field would you like to edit?**"
                                agent.context.set_awaiting('awaiting_field_to_edit', {
                                    'entity_id': entity_id,
                                    'entity_type': entity_type
                                })
                            else:
                                response = f"❌ {entity_type.title()} {entity_id} not found."
                        else:
                            response = "What would you like to edit? Please specify an ID (e.g., 'Edit P001', 'Modify D003')"
                    
                    # ASSIGNMENT
                    elif intent == "assign":
                        agent.context.set_awaiting('awaiting_assignment_type')
                        response = "📝 **Let's set up an assignment.**\n\nWhat would you like to assign?\n- Pilot to mission\n- Drone to mission"
                    
                    # SHOW AVAILABLE
                    elif intent == "show_available":
                        if "drone" in clean_text:
                            available = agent.get_available_resources('drone')['drones']
                            if not available.empty:
                                response = f"✅ **Available Drones ({len(available)} total):**\n\n"
                                drone_cols = get_safe_columns(available, ['drone_id', 'model', 'location', 'flight_hours'])
                                if drone_cols:
                                    response += available[drone_cols].to_markdown(index=False)
                                else:
                                    response += available.to_markdown(index=False)
                            else:
                                response = "❌ No drones are currently available."
                        elif "pilot" in clean_text:
                            available = agent.get_available_resources('pilot')['pilots']
                            if not available.empty:
                                response = f"✅ **Available Pilots ({len(available)} total):**\n\n"
                                pilot_cols = get_safe_columns(available, ['pilot_id', 'name', 'location', 'skills'])
                                if pilot_cols:
                                    response += available[pilot_cols].to_markdown(index=False)
                                else:
                                    response += available.to_markdown(index=False)
                            else:
                                response = "❌ No pilots are currently available."
                        else:
                            resources = agent.get_available_resources('all')
                            response = "✅ **Available Resources:**\n\n"
                            response += f"**Pilots ({len(resources['pilots'])} available):**\n"
                            if not resources['pilots'].empty:
                                pilot_cols = get_safe_columns(resources['pilots'], ['pilot_id', 'name', 'location'])
                                if pilot_cols:
                                    response += resources['pilots'][pilot_cols].to_markdown(index=False)
                                else:
                                    response += resources['pilots'].to_markdown(index=False)
                            response += f"\n\n**Drones ({len(resources['drones'])} available):**\n"
                            if not resources['drones'].empty:
                                drone_cols = get_safe_columns(resources['drones'], ['drone_id', 'model', 'location'])
                                if drone_cols:
                                    response += resources['drones'][drone_cols].to_markdown(index=False)
                                else:
                                    response += resources['drones'].to_markdown(index=False)
                    
                    # SHOW NON-WORKING
                    elif intent == "show_non_working":
                        if "drone" in clean_text:
                            non_working = agent.get_non_working_resources('drone')['drones']
                            if not non_working.empty:
                                response = f"🔴 **Drones Not Working ({len(non_working)} total):**\n\n"
                                drone_cols = get_safe_columns(non_working, ['drone_id', 'model', 'status', 'location'])
                                if drone_cols:
                                    response += non_working[drone_cols].to_markdown(index=False)
                                else:
                                    response += non_working.to_markdown(index=False)
                                if 'status' in non_working.columns:
                                    response += "\n\n**Status Breakdown:**\n"
                                    for status, count in non_working['status'].value_counts().items():
                                        response += f"- {status}: {count}\n"
                            else:
                                response = "✅ All drones are working!"
                        elif "pilot" in clean_text:
                            non_working = agent.get_non_working_resources('pilot')['pilots']
                            if not non_working.empty:
                                response = f"🔴 **Pilots Not Available ({len(non_working)} total):**\n\n"
                                pilot_cols = get_safe_columns(non_working, ['pilot_id', 'name', 'status', 'location'])
                                if pilot_cols:
                                    response += non_working[pilot_cols].to_markdown(index=False)
                                else:
                                    response += non_working.to_markdown(index=False)
                                if 'status' in non_working.columns:
                                    response += "\n\n**Status Breakdown:**\n"
                                    for status, count in non_working['status'].value_counts().items():
                                        response += f"- {status}: {count}\n"
                            else:
                                response = "✅ All pilots are available!"
                        else:
                            resources = agent.get_non_working_resources('all')
                            response = "🔴 **Not Working / Unavailable:**\n\n"
                            if not resources['pilots'].empty:
                                response += f"**Pilots ({len(resources['pilots'])}):**\n"
                                pilot_cols = get_safe_columns(resources['pilots'], ['pilot_id', 'name', 'status', 'location'])
                                if pilot_cols:
                                    response += resources['pilots'][pilot_cols].to_markdown(index=False)
                                else:
                                    response += resources['pilots'].to_markdown(index=False)
                            if not resources['drones'].empty:
                                response += f"\n\n**Drones ({len(resources['drones'])}):**\n"
                                drone_cols = get_safe_columns(resources['drones'], ['drone_id', 'model', 'status', 'location'])
                                if drone_cols:
                                    response += resources['drones'][drone_cols].to_markdown(index=False)
                                else:
                                    response += resources['drones'].to_markdown(index=False)
                    
                    # FIND INFO
                    elif intent == "find_info" or intent == "check_status":
                        if entity_id and entity_type == "pilot":
                            info = agent.get_pilot_info(entity_id)
                            if info:
                                response = f"**👨‍✈️ Pilot: {info.get('name', 'N/A')} ({entity_id})**\n\n"
                                response += f"- **Status:** {info.get('status', 'N/A')}\n"
                                response += f"- **Location:** {info.get('location', 'N/A')}\n"
                                response += f"- **Skills:** {info.get('skills', 'N/A')}\n"
                                response += f"- **Current Assignment:** {info.get('current_assignment', 'None')}\n"
                                response += f"- **Available From:** {info.get('available_from', 'N/A')}"
                            else:
                                response = f"❌ Pilot {entity_id} not found."
                        
                        elif entity_id and entity_type == "drone":
                            info = agent.get_drone_info(entity_id)
                            if info:
                                response = f"**🚁 Drone: {info.get('model', 'N/A')} ({entity_id})**\n\n"
                                response += f"- **Status:** {info.get('status', 'N/A')}\n"
                                response += f"- **Location:** {info.get('location', 'N/A')}\n"
                                response += f"- **Flight Hours:** {info.get('flight_hours', 'N/A')}\n"
                                response += f"- **Current Assignment:** {info.get('current_assignment', 'None')}\n"
                                response += f"- **Last Maintenance:** {info.get('last_maintenance', 'N/A')}"
                            else:
                                response = f"❌ Drone {entity_id} not found."
                        
                        elif entity_id and entity_type == "mission":
                            info = agent.get_mission_info(entity_id)
                            if info:
                                response = f"**📋 Mission: {info.get('project_name', 'N/A')} ({entity_id})**\n\n"
                                response += f"- **Status:** {info.get('status', 'N/A')}\n"
                                response += f"- **Location:** {info.get('location', 'N/A')}\n"
                                response += f"- **Start:** {info.get('start_date', 'N/A')} | **End:** {info.get('end_date', 'N/A')}\n"
                                response += f"- **Required Skills:** {info.get('required_skills', 'N/A')}\n"
                                response += f"- **Assigned Pilot:** {info.get('assigned_pilot', 'None')}\n"
                                response += f"- **Assigned Drone:** {info.get('assigned_drone', 'None')}"
                            else:
                                response = f"❌ Mission {entity_id} not found."
                        else:
                            response = "What would you like to find? (Try: 'Find P001', 'Show D002', 'Status of PRJ001')"
                    
                    # RECOMMEND
                    elif intent == "recommend":
                        if entity_id and entity_type == "mission":
                            recs = agent.recommend_replacement(entity_id)
                            if not recs.empty:
                                response = f"**🎯 Top Recommendations for {entity_id}:**\n\n" + recs.to_markdown(index=False)
                            else:
                                response = f"❌ No available pilots found for {entity_id}."
                        else:
                            response = "Which mission? (e.g., 'Recommend for PRJ001')"
                    
                    # UPDATE STATUS
                    elif intent == "update_status":
                        if entity_id:
                            new_status = "Available"
                            if "leave" in clean_text:
                                new_status = "On Leave"
                            elif "mainten" in clean_text:
                                new_status = "Maintenance"
                            elif "assign" in clean_text:
                                new_status = "Assigned"
                            elif "active" in clean_text:
                                new_status = "Active"
                            elif "complete" in clean_text:
                                new_status = "Completed"
                            
                            if entity_type == "pilot":
                                res = agent.update_pilot_status(entity_id, new_status, pilots_ws)
                                target = "Pilot"
                            elif entity_type == "drone":
                                res = agent.update_drone_status(entity_id, new_status, drones_ws)
                                target = "Drone"
                            elif entity_type == "mission":
                                res = agent.update_mission_status(entity_id, new_status, missions_ws)
                                target = "Mission"
                            else:
                                res = None
                                response = "I need to know what to update."

                            if res is True:
                                response = f"✅ Updated {target} {entity_id} to '{new_status}'."
                                st.cache_data.clear()
                            elif res is not None:
                                response = f"❌ Update failed: {res}"
                        else:
                            response = "What should I update? (e.g., 'Update P001 to Available')"
                    
                    # SHOW MISSIONS
                    elif intent == "show_missions":
                        active = missions_df[missions_df['status'] == 'Active']
                        response = f"**📋 Active Missions ({len(active)} total):**\n\n"
                        mission_cols = get_safe_columns(active, ['project_id', 'project_name', 'location', 'status'])
                        if mission_cols:
                            response += active[mission_cols].to_markdown(index=False)
                        else:
                            response += active.to_markdown(index=False)
                    
                    # SHOW ROSTER
                    elif intent == "show_roster":
                        if "drone" in clean_text:
                            response = f"**🚁 All Drones ({len(drones_df)} total):**\n\n"
                            drone_cols = get_safe_columns(drones_df, ['drone_id', 'model', 'status', 'location', 'flight_hours'])
                            if drone_cols:
                                response += drones_df[drone_cols].to_markdown(index=False)
                            else:
                                response += drones_df.to_markdown(index=False)
                            if 'status' in drones_df.columns:
                                response += f"\n\n**Status Summary:**\n"
                                for status, count in drones_df['status'].value_counts().items():
                                    response += f"- {status}: {count}\n"
                        else:
                            response = f"**👨‍✈️ All Pilots ({len(pilots_df)} total):**\n\n"
                            pilot_cols = get_safe_columns(pilots_df, ['pilot_id', 'name', 'status', 'location', 'skills'])
                            if pilot_cols:
                                response += pilots_df[pilot_cols].to_markdown(index=False)
                            else:
                                response += pilots_df.to_markdown(index=False)
                            if 'status' in pilots_df.columns:
                                response += f"\n\n**Status Summary:**\n"
                                for status, count in pilots_df['status'].value_counts().items():
                                    response += f"- {status}: {count}\n"
                    
                    else:
                        response = "🤖 **I can help you with:**\n\n"
                        response += "**CRUD Operations:**\n"
                        response += "- ➕ **Create**: Add new pilot/drone/mission\n"
                        response += "- 📖 **Read**: Find status, show rosters, list available\n"
                        response += "- ✏️ **Update**: Edit fields, change status, assign resources\n"
                        response += "- 🗑️ **Delete**: Remove pilot/drone/mission\n\n"
                        response += "**Quick Commands:**\n"
                        response += "- 'Add new pilot' | 'Delete D005' | 'Edit P001'\n"
                        response += "- 'Show available drones' | 'Assign pilot to mission'\n"
                        response += "- 'Status of PRJ001' | 'Recommend for PRJ002'\n\n"
                        response += "What would you like to do?"

                # Display response
                with st.chat_message("assistant"):
                    st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})

        with tab2:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader("👨‍✈️ Pilots")
                st.dataframe(pilots_df, use_container_width=True)
            with col2:
                st.subheader("🚁 Drones")
                st.dataframe(drones_df, use_container_width=True)
            with col3:
                st.subheader("📋 Missions")
                st.dataframe(missions_df, use_container_width=True)

        with tab3:
            st.subheader("⚠️ Conflict Detection")
            conflicts = agent.check_conflicts()
            if conflicts:
                for c in conflicts:
                    if c['severity'] == "High":
                        st.error(f"🔴 **{c['type']}**: {c['entity']} - {c['detail']}")
                    else:
                        st.warning(f"🟠 **{c['type']}**: {c['entity']} - {c['detail']}")
            else:
                st.success("✅ No conflicts detected")

if __name__ == "__main__":
    main()
