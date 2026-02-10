import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
from thefuzz import process # Import fuzzy matching

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

# --- AGENT LOGIC CORE ---
class OpsAgent:
    def __init__(self, pilots, drones, missions):
        self.pilots = pilots
        self.drones = drones
        self.missions = missions

    def check_conflicts(self):
        conflicts = []
        
        # --- 1. PILOT CONFLICTS ---
        for idx, pilot in self.pilots.iterrows():
            if pilot['current_assignment'] and pilot['current_assignment'] != "–":
                mission = self.missions[self.missions['project_id'] == pilot['current_assignment']]
                
                if not mission.empty:
                    m_data = mission.iloc[0]
                    
                    # A. Date Overlap
                    m_end = pd.to_datetime(m_data['end_date'])
                    p_avail = pd.to_datetime(pilot['available_from'])
                    if p_avail > m_end:
                         conflicts.append({
                            "type": "Pilot Date Overlap",
                            "entity": pilot['name'],
                            "detail": f"Assigned to {pilot['current_assignment']} but unavailable until {pilot['available_from']}",
                            "severity": "High"
                        })

                    # B. Skill Mismatch
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

        # --- 2. DRONE CONFLICTS ---
        for idx, drone in self.drones.iterrows():
            if drone['current_assignment'] and drone['current_assignment'] != "–":
                # C. Maintenance Check
                if drone['status'] == 'Maintenance':
                    conflicts.append({
                        "type": "Drone in Maintenance",
                        "entity": drone['drone_id'],
                        "detail": f"Assigned to {drone['current_assignment']} but status is Maintenance",
                        "severity": "High"
                    })
                
                # D. Location Mismatch
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
    
    # FIXED: Moved outside check_conflicts() with proper indentation
    def recommend_replacement(self, project_id):
        """Find best pilot replacements for a project"""
        project_id = str(project_id).strip().upper()
        mission_rows = self.missions[self.missions['project_id'] == project_id]
        
        if mission_rows.empty:
            return pd.DataFrame()
            
        mission = mission_rows.iloc[0]
        req_skills = [x.strip() for x in mission['required_skills'].split(',')]
        location = mission['location']
        
        # Filter: Available + Matches Location
        candidates = self.pilots[
            (self.pilots['status'] == 'Available') & 
            (self.pilots['location'] == location)
        ].copy()
        
        if candidates.empty:
            return pd.DataFrame()
            
        # Score candidates
        candidates['score'] = candidates['skills'].apply(
            lambda x: sum(1 for s in req_skills if s in x)
        )
        
        best_matches = candidates.sort_values(by='score', ascending=False)
        return best_matches[['name', 'pilot_id', 'skills', 'location', 'score']]

    def get_pilot_info(self, pilot_id):
        """Get detailed pilot information"""
        pilot_data = self.pilots[self.pilots['pilot_id'] == pilot_id]
        if not pilot_data.empty:
            return pilot_data.iloc[0].to_dict()
        return None
    
    def get_drone_info(self, drone_id):
        """Get detailed drone information"""
        drone_data = self.drones[self.drones['drone_id'] == drone_id]
        if not drone_data.empty:
            return drone_data.iloc[0].to_dict()
        return None
    
    def get_mission_info(self, project_id):
        """Get detailed mission/project information"""
        project_id = str(project_id).strip().upper()
        mission_data = self.missions[self.missions['project_id'] == project_id]
        if not mission_data.empty:
            return mission_data.iloc[0].to_dict()
        return None

    def update_pilot_status(self, pilot_id, new_status, worksheet):
        """Update pilot status in Google Sheets"""
        try:
            cell = worksheet.find(pilot_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            return True
        except Exception as e:
            return str(e)
    
    def update_drone_status(self, drone_id, new_status, worksheet):
        """Update drone status in Google Sheets"""
        try:
            cell = worksheet.find(drone_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            return True
        except Exception as e:
            return str(e)
    
    def update_mission_status(self, project_id, new_status, worksheet):
        """Update mission status in Google Sheets"""
        try:
            cell = worksheet.find(project_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            return True
        except Exception as e:
            return str(e)


def parse_intent(user_input):
    """
    Enhanced intent classifier with support for pilots, drones, and missions
    Returns: (Intent_Type, Entity_ID, Entity_Type, Clean_Text)
    """
    user_input_lower = user_input.lower()
    
    # Define keywords for different intents
    intents = {
        "find_info": ["find", "show", "get", "details", "info", "where is", "who is"],
        "recommend": ["recommend", "replacement", "who can", "suggest"],
        "update_status": ["update", "set status", "mark as", "change status"],
        "check_status": ["status of", "check status", "is available"],
        "show_roster": ["show pilots", "list pilots", "roster", "show drones", "list drones"],
        "show_missions": ["show missions", "list missions", "projects", "show projects"]
    }
    
    # Detect Intent
    detected_intent = None
    for intent, keywords in intents.items():
        if any(k in user_input_lower for k in keywords):
            detected_intent = intent
            break
    
    # Extract Entity ID and Type
    words = user_input.replace("?", "").replace(".", "").split()
    entity_id = None
    entity_type = None
    
    for w in words:
        w_upper = w.upper()
        # Check for Project ID (PRJ001, etc.)
        if w_upper.startswith("PRJ") and any(c.isdigit() for c in w_upper):
            entity_id = w_upper
            entity_type = "mission"
            break
        # Check for Pilot ID (P001, etc.)
        elif w_upper.startswith("P0") and any(c.isdigit() for c in w_upper):
            entity_id = w_upper
            entity_type = "pilot"
            break
        # Check for Drone ID (D001, etc.)
        elif w_upper.startswith("D0") and any(c.isdigit() for c in w_upper):
            entity_id = w_upper
            entity_type = "drone"
            break
            
    return detected_intent, entity_id, entity_type, user_input_lower


def main():
    st.title("🔒 Skylark Private Ops Agent")
    st.caption("Privacy Mode: On. Data processed locally.")
    
    # Sidebar
    st.sidebar.header("System Status")
    client = init_connection()
    
    if client:
        st.sidebar.success("Database Connected")
        if st.sidebar.button("🔄 Sync Data"):
            st.cache_data.clear()
            st.rerun()
    
    # Load Data (now including missions_ws)
    pilots_df, drones_df, missions_df, pilots_ws, drones_ws, missions_ws = load_data(client)
    
    if pilots_df is not None:
        agent = OpsAgent(pilots_df, drones_df, missions_df)
        
        tab1, tab2, tab3 = st.tabs(["💬 Ops Console", "📊 Roster & Fleet", "⚠️ Conflicts"])
        
        with tab1:
            st.info("**Try:** 'Find P001', 'Status of D001', 'Show PRJ001', 'Update D002 to Maintenance', 'Recommend for PRJ001'")
            
            if "messages" not in st.session_state:
                st.session_state.messages = []

            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

            if prompt := st.chat_input("Ex: 'Find drone D001' or 'Show mission PRJ002' or 'Update P003 to Available'"):
                st.session_state.messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.markdown(prompt)

                # Parse Intent
                intent, entity_id, entity_type, clean_text = parse_intent(prompt)
                
                # Smart Name Lookup (for pilots and drones)
                if not entity_id:
                    # Check pilot names
                    for idx, row in pilots_df.iterrows():
                        if row['name'].lower() in clean_text:
                            entity_id = row['pilot_id']
                            entity_type = "pilot"
                            break
                    
                    # Check drone models if not found
                    if not entity_id:
                        for idx, row in drones_df.iterrows():
                            if row['model'].lower() in clean_text:
                                entity_id = row['drone_id']
                                entity_type = "drone"
                                break

                response = "I didn't quite catch that. Try: 'Find P001', 'Show D001', 'Status of PRJ001', or 'Update P002 to Available'."

                # ==================== INTENT HANDLERS ====================
                
                # FIND INFORMATION
                if intent == "find_info" or intent == "check_status":
                    if entity_id and entity_type == "pilot":
                        info = agent.get_pilot_info(entity_id)
                        if info:
                            response = f"**👨‍✈️ Pilot: {info['name']} ({entity_id})**\n\n" \
                                       f"- **Status:** {info['status']}\n" \
                                       f"- **Location:** {info['location']}\n" \
                                       f"- **Skills:** {info['skills']}\n" \
                                       f"- **Current Assignment:** {info.get('current_assignment', 'None')}\n" \
                                       f"- **Available From:** {info.get('available_from', 'N/A')}"
                        else:
                            response = f"❌ Pilot {entity_id} not found."
                    
                    elif entity_id and entity_type == "drone":
                        info = agent.get_drone_info(entity_id)
                        if info:
                            response = f"**🚁 Drone: {info['model']} ({entity_id})**\n\n" \
                                       f"- **Status:** {info['status']}\n" \
                                       f"- **Location:** {info['location']}\n" \
                                       f"- **Flight Hours:** {info.get('flight_hours', 'N/A')}\n" \
                                       f"- **Current Assignment:** {info.get('current_assignment', 'None')}\n" \
                                       f"- **Last Maintenance:** {info.get('last_maintenance', 'N/A')}"
                        else:
                            response = f"❌ Drone {entity_id} not found."
                    
                    elif entity_id and entity_type == "mission":
                        info = agent.get_mission_info(entity_id)
                        if info:
                            response = f"**📋 Mission: {info['project_name']} ({entity_id})**\n\n" \
                                       f"- **Status:** {info['status']}\n" \
                                       f"- **Location:** {info['location']}\n" \
                                       f"- **Start Date:** {info['start_date']}\n" \
                                       f"- **End Date:** {info['end_date']}\n" \
                                       f"- **Required Skills:** {info['required_skills']}\n" \
                                       f"- **Assigned Pilot:** {info.get('assigned_pilot', 'None')}\n" \
                                       f"- **Assigned Drone:** {info.get('assigned_drone', 'None')}"
                        else:
                            response = f"❌ Mission {entity_id} not found."
                    else:
                        response = "Please specify what you'd like to find (e.g., 'Find P001', 'Show D002', 'Status of PRJ001')."

                # RECOMMEND REPLACEMENT
                elif intent == "recommend":
                    if entity_id and entity_type == "mission":
                        recs = agent.recommend_replacement(entity_id)
                        if not recs.empty:
                            response = f"**🎯 Top Pilot Recommendations for {entity_id}:**\n\n" + recs.to_markdown(index=False)
                        else:
                            response = f"❌ No available pilots found for {entity_id} or mission doesn't exist."
                    else:
                        response = "Please specify a project ID (e.g., 'Recommend for PRJ001')."

                # UPDATE STATUS
                elif intent == "update_status":
                    if entity_id:
                        # Determine new status from user input
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
                        elif "pending" in clean_text:
                            new_status = "Pending"
                        
                        # Update based on entity type
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
                            response = "I need to know what to update (pilot, drone, or mission)."
                            res = None

                        if res is True:
                            response = f"✅ Updated {target} {entity_id} to '{new_status}'."
                            st.cache_data.clear()
                        elif res is not None:
                            response = f"❌ Update failed: {res}"
                    else:
                        response = "I need an ID to update (e.g., 'Update P001 to Available', 'Update D002 to Maintenance')."

                # SHOW ROSTER
                elif intent == "show_roster":
                    if "drone" in clean_text:
                        avail = drones_df[drones_df['status'] == 'Available']
                        response = f"**Available Drones:**\n\n{avail[['drone_id', 'model', 'location']].to_markdown(index=False)}"
                    else:
                        avail = pilots_df[pilots_df['status'] == 'Available']
                        response = f"**Available Pilots:**\n\n{avail[['pilot_id', 'name', 'location', 'skills']].to_markdown(index=False)}"
                
                # SHOW MISSIONS
                elif intent == "show_missions":
                    active_missions = missions_df[missions_df['status'] == 'Active']
                    response = f"**Active Missions:**\n\n{active_missions[['project_id', 'project_name', 'location', 'status']].to_markdown(index=False)}"

                # Display response
                with st.chat_message("assistant"):
                    st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})

        with tab2:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader("Pilots")
                st.dataframe(pilots_df)
            with col2:
                st.subheader("Drones")
                st.dataframe(drones_df)
            with col3:
                st.subheader("Missions")
                st.dataframe(missions_df)

        with tab3:
            st.subheader("Conflict Detection Log")
            conflicts = agent.check_conflicts()
            if conflicts:
                for c in conflicts:
                    if c['severity'] == "High":
                        st.error(f"🔴 {c['entity']}: {c['detail']}")
                    else:
                        st.warning(f"🟠 {c['entity']}: {c['detail']}")
                st.divider()
                st.write("💡 *Tip: Go to the Ops Console to find replacements or update statuses.*")
            else:
                st.success("✅ No active conflicts detected.")

if __name__ == "__main__":
    main()
