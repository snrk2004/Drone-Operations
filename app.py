import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time

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

# --- CORE OPS AGENT ---
class OpsAgent:
    def __init__(self, pilots, drones, missions):
        self.pilots = pilots
        self.drones = drones
        self.missions = missions
        self.context = ConversationContext()

    # ============================================================
    # FEATURE 1: ROSTER MANAGEMENT
    # ============================================================
    
    def query_pilots_by_skill(self, skill):
        """Query pilots by skill/certification"""
        skill = skill.strip()
        matching = self.pilots[self.pilots['skills'].str.contains(skill, case=False, na=False)]
        return matching
    
    def query_pilots_by_location(self, location):
        """Query pilots by location"""
        location = location.strip()
        matching = self.pilots[self.pilots['location'].str.contains(location, case=False, na=False)]
        return matching
    
    def query_available_pilots(self, skill=None, location=None):
        """Query available pilots with optional filters"""
        available = self.pilots[self.pilots['status'] == 'Available'].copy()
        
        if skill:
            available = available[available['skills'].str.contains(skill, case=False, na=False)]
        
        if location:
            available = available[available['location'].str.contains(location, case=False, na=False)]
        
        return available
    
    def get_pilot_current_assignments(self):
        """Get all pilots with their current assignments"""
        assigned = self.pilots[
            (self.pilots['current_assignment'].notna()) & 
            (self.pilots['current_assignment'] != '–')
        ].copy()
        return assigned
    
    def update_pilot_status(self, pilot_id, new_status, worksheet):
        """Update pilot status and sync to Google Sheets"""
        try:
            cell = worksheet.find(pilot_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            
            # If setting to Available, clear assignment
            if new_status == 'Available':
                assignment_col = headers.index('current_assignment') + 1
                worksheet.update_cell(cell.row, assignment_col, '–')
            
            return True
        except Exception as e:
            return str(e)

    # ============================================================
    # FEATURE 2: ASSIGNMENT TRACKING
    # ============================================================
    
    def match_pilot_to_project(self, project_id):
        """Match best pilots to a project based on requirements"""
        project_id = str(project_id).strip().upper()
        mission_rows = self.missions[self.missions['project_id'] == project_id]
        
        if mission_rows.empty:
            return pd.DataFrame(), "Project not found"
            
        mission = mission_rows.iloc[0]
        req_skills = [x.strip() for x in mission['required_skills'].split(',')]
        location = mission['location']
        
        # Find available pilots in same location
        candidates = self.pilots[
            (self.pilots['status'] == 'Available') & 
            (self.pilots['location'] == location)
        ].copy()
        
        if candidates.empty:
            return pd.DataFrame(), f"No available pilots in {location}"
            
        # Score based on skill match
        candidates['skill_match_count'] = candidates['skills'].apply(
            lambda x: sum(1 for s in req_skills if s in str(x))
        )
        candidates['missing_skills'] = candidates['skills'].apply(
            lambda x: [s for s in req_skills if s not in str(x)]
        )
        
        # Sort by best match
        best_matches = candidates.sort_values(by='skill_match_count', ascending=False)
        
        cols = ['pilot_id', 'name', 'skills', 'location', 'skill_match_count', 'missing_skills']
        available_cols = [c for c in cols if c in best_matches.columns]
        
        return best_matches[available_cols], "Success"
    
    def track_active_assignments(self):
        """Track all active pilot and drone assignments"""
        result = {
            'pilots': self.pilots[
                (self.pilots['status'] == 'Assigned') |
                ((self.pilots['current_assignment'].notna()) & (self.pilots['current_assignment'] != '–'))
            ].copy(),
            'drones': self.drones[
                (self.drones['status'] == 'Assigned') |
                ((self.drones['current_assignment'].notna()) & (self.drones['current_assignment'] != '–'))
            ].copy()
        }
        return result
    
    def assign_pilot_to_project(self, pilot_id, project_id, pilots_ws, missions_ws):
        """Assign a pilot to a project with validation"""
        try:
            # Check if pilot is available
            pilot_data = self.pilots[self.pilots['pilot_id'] == pilot_id]
            if pilot_data.empty:
                return "Pilot not found"
            
            if pilot_data.iloc[0]['status'] != 'Available':
                return f"Pilot {pilot_id} is not available (Status: {pilot_data.iloc[0]['status']})"
            
            # Update pilot
            pilot_cell = pilots_ws.find(pilot_id)
            headers = pilots_ws.row_values(1)
            
            assignment_col = headers.index('current_assignment') + 1
            status_col = headers.index('status') + 1
            
            pilots_ws.update_cell(pilot_cell.row, assignment_col, project_id)
            pilots_ws.update_cell(pilot_cell.row, status_col, 'Assigned')
            
            # Update mission
            mission_cell = missions_ws.find(project_id)
            mission_headers = missions_ws.row_values(1)
            pilot_assignment_col = mission_headers.index('assigned_pilot') + 1
            
            missions_ws.update_cell(mission_cell.row, pilot_assignment_col, pilot_id)
            
            return True
        except Exception as e:
            return str(e)
    
    def reassign_pilot(self, pilot_id, old_project_id, new_project_id, pilots_ws, missions_ws):
        """Handle pilot reassignment"""
        try:
            # Clear old assignment
            old_mission_cell = missions_ws.find(old_project_id)
            mission_headers = missions_ws.row_values(1)
            pilot_col = mission_headers.index('assigned_pilot') + 1
            missions_ws.update_cell(old_mission_cell.row, pilot_col, '–')
            
            # Assign to new project
            result = self.assign_pilot_to_project(pilot_id, new_project_id, pilots_ws, missions_ws)
            return result
        except Exception as e:
            return str(e)

    # ============================================================
    # FEATURE 3: DRONE INVENTORY
    # ============================================================
    
    def query_drones_by_capability(self, capability):
        """Query drones by model/capability"""
        matching = self.drones[self.drones['model'].str.contains(capability, case=False, na=False)]
        return matching
    
    def query_drones_by_location(self, location):
        """Query drones by location"""
        matching = self.drones[self.drones['location'].str.contains(location, case=False, na=False)]
        return matching
    
    def query_available_drones(self, location=None):
        """Query available drones with optional location filter"""
        available = self.drones[self.drones['status'] == 'Available'].copy()
        
        if location:
            available = available[available['location'].str.contains(location, case=False, na=False)]
        
        return available
    
    def get_drone_deployment_status(self):
        """Get all drones and their deployment status"""
        return self.drones[['drone_id', 'model', 'status', 'location', 'current_assignment', 'flight_hours']].copy()
    
    def flag_maintenance_issues(self):
        """Flag drones that need maintenance"""
        issues = []
        
        for idx, drone in self.drones.iterrows():
            # Check if already in maintenance
            if drone['status'] == 'Maintenance':
                issues.append({
                    'drone_id': drone['drone_id'],
                    'model': drone['model'],
                    'issue': 'Currently in maintenance',
                    'severity': 'High'
                })
            
            # Check flight hours (assuming 500+ hours needs maintenance)
            if 'flight_hours' in drone and pd.notna(drone['flight_hours']):
                try:
                    hours = float(drone['flight_hours'])
                    if hours > 500:
                        issues.append({
                            'drone_id': drone['drone_id'],
                            'model': drone['model'],
                            'issue': f'High flight hours ({hours}hrs) - maintenance recommended',
                            'severity': 'Medium'
                        })
                except:
                    pass
        
        return issues
    
    def update_drone_status(self, drone_id, new_status, worksheet):
        """Update drone status and sync to Google Sheets"""
        try:
            cell = worksheet.find(drone_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            
            # If setting to Available or Maintenance, clear assignment
            if new_status in ['Available', 'Maintenance']:
                assignment_col = headers.index('current_assignment') + 1
                worksheet.update_cell(cell.row, assignment_col, '–')
            
            return True
        except Exception as e:
            return str(e)
    
    def assign_drone_to_project(self, drone_id, project_id, drones_ws, missions_ws):
        """Assign a drone to a project"""
        try:
            # Check if drone is available
            drone_data = self.drones[self.drones['drone_id'] == drone_id]
            if drone_data.empty:
                return "Drone not found"
            
            if drone_data.iloc[0]['status'] != 'Available':
                return f"Drone {drone_id} is not available (Status: {drone_data.iloc[0]['status']})"
            
            # Update drone
            drone_cell = drones_ws.find(drone_id)
            headers = drones_ws.row_values(1)
            
            assignment_col = headers.index('current_assignment') + 1
            status_col = headers.index('status') + 1
            
            drones_ws.update_cell(drone_cell.row, assignment_col, project_id)
            drones_ws.update_cell(drone_cell.row, status_col, 'Assigned')
            
            # Update mission
            mission_cell = missions_ws.find(project_id)
            mission_headers = missions_ws.row_values(1)
            drone_assignment_col = mission_headers.index('assigned_drone') + 1
            
            missions_ws.update_cell(mission_cell.row, drone_assignment_col, drone_id)
            
            return True
        except Exception as e:
            return str(e)

    # ============================================================
    # FEATURE 4: CONFLICT DETECTION
    # ============================================================
    
    def detect_all_conflicts(self):
        """Comprehensive conflict detection"""
        conflicts = []
        
        # 1. DOUBLE-BOOKING DETECTION
        conflicts.extend(self.detect_double_bookings())
        
        # 2. SKILL/CERTIFICATION MISMATCH
        conflicts.extend(self.detect_skill_mismatches())
        
        # 3. LOCATION MISMATCH
        conflicts.extend(self.detect_location_mismatches())
        
        return conflicts
    
    def detect_double_bookings(self):
        """Detect pilots or drones assigned to overlapping projects"""
        conflicts = []
        
        # Check for pilots with assignments but marked as Available
        for idx, pilot in self.pilots.iterrows():
            if pilot['current_assignment'] and pilot['current_assignment'] != '–':
                if pilot['status'] == 'Available':
                    conflicts.append({
                        'type': 'Double-Booking (Pilot)',
                        'entity': f"{pilot['name']} ({pilot['pilot_id']})",
                        'detail': f"Assigned to {pilot['current_assignment']} but marked as Available",
                        'severity': 'Critical'
                    })
        
        # Check for drones with assignments but marked as Available
        for idx, drone in self.drones.iterrows():
            if drone['current_assignment'] and drone['current_assignment'] != '–':
                if drone['status'] == 'Available':
                    conflicts.append({
                        'type': 'Double-Booking (Drone)',
                        'entity': f"{drone['model']} ({drone['drone_id']})",
                        'detail': f"Assigned to {drone['current_assignment']} but marked as Available",
                        'severity': 'Critical'
                    })
        
        # Check for availability date issues
        for idx, pilot in self.pilots.iterrows():
            if pilot['current_assignment'] and pilot['current_assignment'] != '–':
                mission = self.missions[self.missions['project_id'] == pilot['current_assignment']]
                
                if not mission.empty:
                    try:
                        m_data = mission.iloc[0]
                        m_start = pd.to_datetime(m_data['start_date'])
                        p_avail = pd.to_datetime(pilot['available_from'])
                        
                        if p_avail > m_start:
                            conflicts.append({
                                'type': 'Double-Booking (Date Conflict)',
                                'entity': f"{pilot['name']} ({pilot['pilot_id']})",
                                'detail': f"Assigned to {pilot['current_assignment']} starting {m_start.date()} but unavailable until {p_avail.date()}",
                                'severity': 'Critical'
                            })
                    except:
                        pass
        
        return conflicts
    
    def detect_skill_mismatches(self):
        """Detect skill/certification mismatches"""
        conflicts = []
        
        for idx, pilot in self.pilots.iterrows():
            if pilot['current_assignment'] and pilot['current_assignment'] != '–':
                mission = self.missions[self.missions['project_id'] == pilot['current_assignment']]
                
                if not mission.empty:
                    m_data = mission.iloc[0]
                    req_skills = [x.strip() for x in str(m_data['required_skills']).split(',')]
                    pilot_skills = str(pilot['skills'])
                    
                    missing = [s for s in req_skills if s not in pilot_skills]
                    
                    if missing:
                        conflicts.append({
                            'type': 'Skill Mismatch',
                            'entity': f"{pilot['name']} ({pilot['pilot_id']})",
                            'detail': f"Missing skills {missing} for {pilot['current_assignment']}",
                            'severity': 'High'
                        })
        
        return conflicts
    
    def detect_location_mismatches(self):
        """Detect equipment-pilot location mismatches"""
        conflicts = []
        
        # Check pilot location vs project location
        for idx, pilot in self.pilots.iterrows():
            if pilot['current_assignment'] and pilot['current_assignment'] != '–':
                mission = self.missions[self.missions['project_id'] == pilot['current_assignment']]
                
                if not mission.empty:
                    mission_loc = mission.iloc[0]['location']
                    if pilot['location'] != mission_loc:
                        conflicts.append({
                            'type': 'Location Mismatch (Pilot)',
                            'entity': f"{pilot['name']} ({pilot['pilot_id']})",
                            'detail': f"Pilot in {pilot['location']} but project {pilot['current_assignment']} is in {mission_loc}",
                            'severity': 'Medium'
                        })
        
        # Check drone location vs project location
        for idx, drone in self.drones.iterrows():
            if drone['current_assignment'] and drone['current_assignment'] != '–':
                mission = self.missions[self.missions['project_id'] == drone['current_assignment']]
                
                if not mission.empty:
                    mission_loc = mission.iloc[0]['location']
                    if drone['location'] != mission_loc:
                        conflicts.append({
                            'type': 'Location Mismatch (Drone)',
                            'entity': f"{drone['model']} ({drone['drone_id']})",
                            'detail': f"Drone in {drone['location']} but project {drone['current_assignment']} is in {mission_loc}",
                            'severity': 'Medium'
                        })
        
        # Check drone in maintenance but assigned
        for idx, drone in self.drones.iterrows():
            if drone['current_assignment'] and drone['current_assignment'] != '–':
                if drone['status'] == 'Maintenance':
                    conflicts.append({
                        'type': 'Equipment Unavailable',
                        'entity': f"{drone['model']} ({drone['drone_id']})",
                        'detail': f"Assigned to {drone['current_assignment']} but in Maintenance",
                        'severity': 'Critical'
                    })
        
        return conflicts

    # ============================================================
    # HELPER METHODS
    # ============================================================
    
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


def parse_intent(user_input):
    """Intent classifier for natural language commands"""
    user_input_lower = user_input.lower()
    
    # Extract entities first
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
    
    # Detect intent
    intent = None
    params = {}
    
    # Roster Management
    if any(k in user_input_lower for k in ["pilots with", "pilots who have", "find pilots"]):
        intent = "query_pilots_by_skill"
        # Extract skill
        for word in ["mapping", "thermal", "inspection", "surveillance"]:
            if word in user_input_lower:
                params['skill'] = word.capitalize()
                break
    elif any(k in user_input_lower for k in ["pilots in", "pilots at"]):
        intent = "query_pilots_by_location"
        for word in ["mumbai", "delhi", "bangalore", "chennai", "pune"]:
            if word in user_input_lower:
                params['location'] = word.capitalize()
                break
    elif "available pilots" in user_input_lower:
        intent = "query_available_pilots"
    elif "current assignments" in user_input_lower or "who is assigned" in user_input_lower:
        intent = "show_current_assignments"
    elif "update status" in user_input_lower or "set status" in user_input_lower or "change status" in user_input_lower:
        intent = "update_status"
        params['entity_id'] = entity_id
        params['entity_type'] = entity_type
    
    # Assignment Tracking
    elif "match" in user_input_lower and "project" in user_input_lower:
        intent = "match_pilot_to_project"
        params['project_id'] = entity_id
    elif "active assignments" in user_input_lower or "track assignments" in user_input_lower:
        intent = "track_active_assignments"
    elif "assign" in user_input_lower:
        intent = "assign"
    elif "reassign" in user_input_lower:
        intent = "reassign"
        params['entity_id'] = entity_id
    
    # Drone Inventory
    elif "drones with" in user_input_lower or "drones by capability" in user_input_lower:
        intent = "query_drones_by_capability"
        for word in ["phantom", "mavic", "inspire", "matrice"]:
            if word in user_input_lower:
                params['capability'] = word.capitalize()
                break
    elif "drones in" in user_input_lower or "drones at" in user_input_lower:
        intent = "query_drones_by_location"
        for word in ["mumbai", "delhi", "bangalore", "chennai", "pune"]:
            if word in user_input_lower:
                params['location'] = word.capitalize()
                break
    elif "available drones" in user_input_lower:
        intent = "query_available_drones"
    elif "drone deployment" in user_input_lower or "deployment status" in user_input_lower:
        intent = "drone_deployment_status"
    elif "maintenance" in user_input_lower and ("flag" in user_input_lower or "check" in user_input_lower or "issues" in user_input_lower):
        intent = "flag_maintenance"
    
    # Conflict Detection
    elif "conflicts" in user_input_lower or "detect conflicts" in user_input_lower:
        intent = "detect_conflicts"
    elif "double book" in user_input_lower:
        intent = "detect_double_bookings"
    elif "skill mismatch" in user_input_lower:
        intent = "detect_skill_mismatches"
    elif "location mismatch" in user_input_lower:
        intent = "detect_location_mismatches"
    
    # General queries
    elif "info" in user_input_lower or "details" in user_input_lower or "status of" in user_input_lower:
        intent = "get_info"
        params['entity_id'] = entity_id
        params['entity_type'] = entity_type
    
    return intent, params, entity_id, entity_type


def get_safe_columns(df, desired_cols):
    """Helper to get columns that exist in dataframe"""
    return [col for col in desired_cols if col in df.columns]


def main():
    st.title("🚁 Skylark Operations Agent")
    st.caption("🤖 Intelligent Ops Assistant | Roster • Assignments • Inventory • Conflicts")
    
    # Sidebar
    st.sidebar.header("🎯 System Status")
    client = init_connection()
    
    if client:
        st.sidebar.success("✅ Connected to Database")
        if st.sidebar.button("🔄 Refresh Data"):
            st.cache_resource.clear()
            st.rerun()
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📋 Quick Commands")
        st.sidebar.markdown("""
        **Roster Management:**
        - Find pilots with Mapping
        - Available pilots in Mumbai
        - Update P001 status
        
        **Assignment Tracking:**
        - Match pilots for PRJ001
        - Track active assignments
        - Assign pilot to mission
        
        **Drone Inventory:**
        - Available drones
        - Drones in Delhi
        - Flag maintenance issues
        
        **Conflict Detection:**
        - Detect conflicts
        - Check double bookings
        """)
    
    # Load Data
    pilots_df, drones_df, missions_df, pilots_ws, drones_ws, missions_ws = load_data(client)
    
    if pilots_df is not None:
        agent = OpsAgent(pilots_df, drones_df, missions_df)
        
        tab1, tab2, tab3, tab4 = st.tabs([
            "💬 Chat Console", 
            "📊 Data Overview", 
            "⚠️ Conflicts Dashboard",
            "📈 Analytics"
        ])
        
        with tab1:
            st.info("💡 **Ask me anything!** Try: 'Find pilots with Mapping skill' | 'Match pilots for PRJ001' | 'Detect conflicts'")
            
            if "messages" not in st.session_state:
                st.session_state.messages = []

            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

            if prompt := st.chat_input("What can I help you with?"):
                st.session_state.messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.markdown(prompt)

                ctx = agent.context.get_context()
                response = ""
                
                if agent.context.is_awaiting():
                    current_action = ctx['current_action']
                    temp_data = ctx['temp_data']
                    
                    # Handle assignment flow
                    if current_action == 'awaiting_assignment_type':
                        if 'pilot' in prompt.lower():
                            agent.context.set_awaiting('awaiting_mission_for_pilot')
                            response = "📋 Which project? (e.g., PRJ001)"
                        elif 'drone' in prompt.lower():
                            agent.context.set_awaiting('awaiting_mission_for_drone')
                            response = "📋 Which project? (e.g., PRJ001)"
                        else:
                            response = "Assign **pilot** or **drone**?"
                    
                    elif current_action == 'awaiting_mission_for_pilot':
                        _, params, entity_id, _ = parse_intent(prompt)
                        if entity_id:
                            temp_data['mission_id'] = entity_id
                            agent.context.set_awaiting('awaiting_pilot_selection', temp_data)
                            
                            matches, msg = agent.match_pilot_to_project(entity_id)
                            if not matches.empty:
                                response = f"**Best matches for {entity_id}:**\n\n{matches.to_markdown(index=False)}\n\nWhich pilot? (e.g., P001)"
                            else:
                                response = f"❌ {msg}"
                                agent.context.clear()
                        else:
                            response = "Please provide a project ID (e.g., PRJ001)"
                    
                    elif current_action == 'awaiting_pilot_selection':
                        _, params, entity_id, _ = parse_intent(prompt)
                        if entity_id:
                            result = agent.assign_pilot_to_project(
                                entity_id, 
                                temp_data['mission_id'],
                                pilots_ws,
                                missions_ws
                            )
                            if result is True:
                                response = f"✅ Assigned {entity_id} to {temp_data['mission_id']}"
                                st.cache_resource.clear()
                            else:
                                response = f"❌ {result}"
                            agent.context.clear()
                        else:
                            response = "Please provide a pilot ID (e.g., P001)"
                    
                    elif current_action == 'awaiting_mission_for_drone':
                        _, params, entity_id, _ = parse_intent(prompt)
                        if entity_id:
                            temp_data['mission_id'] = entity_id
                            agent.context.set_awaiting('awaiting_drone_selection', temp_data)
                            
                            available = agent.query_available_drones()
                            if not available.empty:
                                cols = get_safe_columns(available, ['drone_id', 'model', 'location'])
                                response = f"**Available drones:**\n\n{available[cols].to_markdown(index=False)}\n\nWhich drone? (e.g., D001)"
                            else:
                                response = "❌ No available drones"
                                agent.context.clear()
                        else:
                            response = "Please provide a project ID (e.g., PRJ001)"
                    
                    elif current_action == 'awaiting_drone_selection':
                        _, params, entity_id, _ = parse_intent(prompt)
                        if entity_id:
                            result = agent.assign_drone_to_project(
                                entity_id,
                                temp_data['mission_id'],
                                drones_ws,
                                missions_ws
                            )
                            if result is True:
                                response = f"✅ Assigned {entity_id} to {temp_data['mission_id']}"
                                st.cache_resource.clear()
                            else:
                                response = f"❌ {result}"
                            agent.context.clear()
                        else:
                            response = "Please provide a drone ID (e.g., D001)"
                    
                    elif current_action == 'awaiting_status_update':
                        new_status = prompt.strip()
                        entity_id = temp_data['entity_id']
                        entity_type = temp_data['entity_type']
                        
                        if entity_type == 'pilot':
                            result = agent.update_pilot_status(entity_id, new_status, pilots_ws)
                        elif entity_type == 'drone':
                            result = agent.update_drone_status(entity_id, new_status, drones_ws)
                        
                        if result is True:
                            response = f"✅ Updated {entity_type} {entity_id} to {new_status}"
                            st.cache_resource.clear()
                        else:
                            response = f"❌ {result}"
                        agent.context.clear()
                
                else:
                    # Parse new query
                    intent, params, entity_id, entity_type = parse_intent(prompt)
                    
                    # ========== ROSTER MANAGEMENT ==========
                    if intent == "query_pilots_by_skill":
                        if 'skill' in params:
                            results = agent.query_pilots_by_skill(params['skill'])
                            if not results.empty:
                                cols = get_safe_columns(results, ['pilot_id', 'name', 'skills', 'location', 'status'])
                                response = f"**Pilots with {params['skill']} skill:**\n\n{results[cols].to_markdown(index=False)}"
                            else:
                                response = f"No pilots found with {params['skill']} skill"
                        else:
                            response = "Which skill? (Mapping, Thermal, Inspection, Surveillance)"
                    
                    elif intent == "query_pilots_by_location":
                        if 'location' in params:
                            results = agent.query_pilots_by_location(params['location'])
                            if not results.empty:
                                cols = get_safe_columns(results, ['pilot_id', 'name', 'location', 'status', 'skills'])
                                response = f"**Pilots in {params['location']}:**\n\n{results[cols].to_markdown(index=False)}"
                            else:
                                response = f"No pilots found in {params['location']}"
                        else:
                            response = "Which location? (Mumbai, Delhi, Bangalore, Chennai, Pune)"
                    
                    elif intent == "query_available_pilots":
                        results = agent.query_available_pilots()
                        if not results.empty:
                            cols = get_safe_columns(results, ['pilot_id', 'name', 'location', 'skills'])
                            response = f"**Available Pilots ({len(results)}):**\n\n{results[cols].to_markdown(index=False)}"
                        else:
                            response = "No pilots currently available"
                    
                    elif intent == "show_current_assignments":
                        assigned = agent.get_pilot_current_assignments()
                        if not assigned.empty:
                            cols = get_safe_columns(assigned, ['pilot_id', 'name', 'current_assignment', 'status'])
                            response = f"**Current Assignments:**\n\n{assigned[cols].to_markdown(index=False)}"
                        else:
                            response = "No active pilot assignments"
                    
                    elif intent == "update_status":
                        if entity_id:
                            agent.context.set_awaiting('awaiting_status_update', {
                                'entity_id': entity_id,
                                'entity_type': entity_type
                            })
                            response = f"What status for {entity_id}? (Available / On Leave / Assigned / Maintenance)"
                        else:
                            response = "Which pilot/drone? (e.g., 'Update P001 status')"
                    
                    # ========== ASSIGNMENT TRACKING ==========
                    elif intent == "match_pilot_to_project":
                        if 'project_id' in params:
                            matches, msg = agent.match_pilot_to_project(params['project_id'])
                            if not matches.empty:
                                response = f"**🎯 Best Matches for {params['project_id']}:**\n\n{matches.to_markdown(index=False)}"
                            else:
                                response = f"❌ {msg}"
                        else:
                            response = "Which project? (e.g., 'Match pilots for PRJ001')"
                    
                    elif intent == "track_active_assignments":
                        active = agent.track_active_assignments()
                        response = "**📋 Active Assignments:**\n\n"
                        if not active['pilots'].empty:
                            cols = get_safe_columns(active['pilots'], ['pilot_id', 'name', 'current_assignment'])
                            response += f"**Pilots:**\n{active['pilots'][cols].to_markdown(index=False)}\n\n"
                        if not active['drones'].empty:
                            cols = get_safe_columns(active['drones'], ['drone_id', 'model', 'current_assignment'])
                            response += f"**Drones:**\n{active['drones'][cols].to_markdown(index=False)}"
                    
                    elif intent == "assign":
                        agent.context.set_awaiting('awaiting_assignment_type')
                        response = "Assign **pilot** or **drone**?"
                    
                    # ========== DRONE INVENTORY ==========
                    elif intent == "query_drones_by_capability":
                        if 'capability' in params:
                            results = agent.query_drones_by_capability(params['capability'])
                            if not results.empty:
                                cols = get_safe_columns(results, ['drone_id', 'model', 'status', 'location'])
                                response = f"**Drones ({params['capability']}):**\n\n{results[cols].to_markdown(index=False)}"
                            else:
                                response = f"No drones found with {params['capability']}"
                        else:
                            response = "Which capability/model? (Phantom, Mavic, Inspire, Matrice)"
                    
                    elif intent == "query_drones_by_location":
                        if 'location' in params:
                            results = agent.query_drones_by_location(params['location'])
                            if not results.empty:
                                cols = get_safe_columns(results, ['drone_id', 'model', 'status', 'location'])
                                response = f"**Drones in {params['location']}:**\n\n{results[cols].to_markdown(index=False)}"
                            else:
                                response = f"No drones in {params['location']}"
                        else:
                            response = "Which location?"
                    
                    elif intent == "query_available_drones":
                        results = agent.query_available_drones()
                        if not results.empty:
                            cols = get_safe_columns(results, ['drone_id', 'model', 'location', 'flight_hours'])
                            response = f"**Available Drones ({len(results)}):**\n\n{results[cols].to_markdown(index=False)}"
                        else:
                            response = "No drones currently available"
                    
                    elif intent == "drone_deployment_status":
                        status = agent.get_drone_deployment_status()
                        response = f"**🚁 Drone Deployment Status:**\n\n{status.to_markdown(index=False)}"
                    
                    elif intent == "flag_maintenance":
                        issues = agent.flag_maintenance_issues()
                        if issues:
                            response = "**⚠️ Maintenance Alerts:**\n\n"
                            for issue in issues:
                                emoji = "🔴" if issue['severity'] == 'High' else "🟡"
                                response += f"{emoji} **{issue['drone_id']}** ({issue['model']}): {issue['issue']}\n"
                        else:
                            response = "✅ No maintenance issues detected"
                    
                    # ========== CONFLICT DETECTION ==========
                    elif intent == "detect_conflicts":
                        conflicts = agent.detect_all_conflicts()
                        if conflicts:
                            response = f"**⚠️ Detected {len(conflicts)} Conflicts:**\n\n"
                            for c in conflicts:
                                emoji = "🔴" if c['severity'] == 'Critical' else "🟡" if c['severity'] == 'High' else "🟠"
                                response += f"{emoji} **{c['type']}**\n"
                                response += f"   {c['entity']}: {c['detail']}\n\n"
                        else:
                            response = "✅ No conflicts detected"
                    
                    elif intent == "detect_double_bookings":
                        conflicts = agent.detect_double_bookings()
                        if conflicts:
                            response = "**🔴 Double-Booking Issues:**\n\n"
                            for c in conflicts:
                                response += f"- {c['entity']}: {c['detail']}\n"
                        else:
                            response = "✅ No double-bookings"
                    
                    elif intent == "detect_skill_mismatches":
                        conflicts = agent.detect_skill_mismatches()
                        if conflicts:
                            response = "**⚠️ Skill Mismatches:**\n\n"
                            for c in conflicts:
                                response += f"- {c['entity']}: {c['detail']}\n"
                        else:
                            response = "✅ No skill mismatches"
                    
                    elif intent == "detect_location_mismatches":
                        conflicts = agent.detect_location_mismatches()
                        if conflicts:
                            response = "**📍 Location Mismatches:**\n\n"
                            for c in conflicts:
                                response += f"- {c['entity']}: {c['detail']}\n"
                        else:
                            response = "✅ No location mismatches"
                    
                    # ========== GENERAL ==========
                    elif intent == "get_info":
                        if entity_id and entity_type == "pilot":
                            info = agent.get_pilot_info(entity_id)
                            if info:
                                response = f"**👨‍✈️ {info.get('name', 'N/A')} ({entity_id})**\n\n"
                                response += f"- Status: {info.get('status', 'N/A')}\n"
                                response += f"- Location: {info.get('location', 'N/A')}\n"
                                response += f"- Skills: {info.get('skills', 'N/A')}\n"
                                response += f"- Assignment: {info.get('current_assignment', 'None')}\n"
                                response += f"- Available: {info.get('available_from', 'N/A')}"
                            else:
                                response = f"❌ Pilot {entity_id} not found"
                        elif entity_id and entity_type == "drone":
                            info = agent.get_drone_info(entity_id)
                            if info:
                                response = f"**🚁 {info.get('model', 'N/A')} ({entity_id})**\n\n"
                                response += f"- Status: {info.get('status', 'N/A')}\n"
                                response += f"- Location: {info.get('location', 'N/A')}\n"
                                response += f"- Flight Hours: {info.get('flight_hours', 'N/A')}\n"
                                response += f"- Assignment: {info.get('current_assignment', 'None')}"
                            else:
                                response = f"❌ Drone {entity_id} not found"
                        else:
                            response = "What info do you need? (Try: 'Info on P001' or 'Status of D003')"
                    
                    else:
                        response = "🤖 **I can help with:**\n\n"
                        response += "**📋 Roster Management:**\n"
                        response += "- Find pilots with [skill]\n"
                        response += "- Available pilots in [location]\n"
                        response += "- Update [ID] status\n\n"
                        response += "**🎯 Assignment Tracking:**\n"
                        response += "- Match pilots for [PROJECT]\n"
                        response += "- Track active assignments\n"
                        response += "- Assign pilot/drone\n\n"
                        response += "**🚁 Drone Inventory:**\n"
                        response += "- Available drones\n"
                        response += "- Drones in [location]\n"
                        response += "- Flag maintenance issues\n\n"
                        response += "**⚠️ Conflict Detection:**\n"
                        response += "- Detect conflicts\n"
                        response += "- Check double bookings"

                with st.chat_message("assistant"):
                    st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})

        with tab2:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader("👨‍✈️ Pilots")
                st.dataframe(pilots_df, use_container_width=True, height=400)
            with col2:
                st.subheader("🚁 Drones")
                st.dataframe(drones_df, use_container_width=True, height=400)
            with col3:
                st.subheader("📋 Missions")
                st.dataframe(missions_df, use_container_width=True, height=400)

        with tab3:
            st.subheader("⚠️ Comprehensive Conflict Detection")
            
            conflicts = agent.detect_all_conflicts()
            
            if conflicts:
                # Group by severity
                critical = [c for c in conflicts if c['severity'] == 'Critical']
                high = [c for c in conflicts if c['severity'] == 'High']
                medium = [c for c in conflicts if c['severity'] == 'Medium']
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("🔴 Critical", len(critical))
                with col2:
                    st.metric("🟡 High", len(high))
                with col3:
                    st.metric("🟠 Medium", len(medium))
                
                st.markdown("---")
                
                for c in conflicts:
                    if c['severity'] == 'Critical':
                        st.error(f"🔴 **{c['type']}**: {c['entity']} - {c['detail']}")
                    elif c['severity'] == 'High':
                        st.warning(f"🟡 **{c['type']}**: {c['entity']} - {c['detail']}")
                    else:
                        st.info(f"🟠 **{c['type']}**: {c['entity']} - {c['detail']}")
            else:
                st.success("✅ No conflicts detected! All systems operational.")
        
        with tab4:
            st.subheader("📈 Operations Analytics")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 👨‍✈️ Pilot Status")
                if 'status' in pilots_df.columns:
                    status_counts = pilots_df['status'].value_counts()
                    st.bar_chart(status_counts)
                
                st.markdown("### 📍 Pilot Locations")
                if 'location' in pilots_df.columns:
                    loc_counts = pilots_df['location'].value_counts()
                    st.bar_chart(loc_counts)
            
            with col2:
                st.markdown("### 🚁 Drone Status")
                if 'status' in drones_df.columns:
                    drone_status = drones_df['status'].value_counts()
                    st.bar_chart(drone_status)
                
                st.markdown("### 📍 Drone Locations")
                if 'location' in drones_df.columns:
                    drone_loc = drones_df['location'].value_counts()
                    st.bar_chart(drone_loc)

if __name__ == "__main__":
    main()
