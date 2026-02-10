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
        
        return pilots_df, drones_df, missions_df, pilots_ws, drones_ws
    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {e}")
        return None, None, None, None, None

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
                    # Date Overlap
                    m_end = pd.to_datetime(m_data['end_date'])
                    p_avail = pd.to_datetime(pilot['available_from'])
                    if p_avail > m_end:
                         conflicts.append({
                            "type": "Pilot Date Overlap",
                            "entity": pilot['name'],
                            "detail": f"Assigned to {pilot['current_assignment']} but unavailable until {pilot['available_from']}",
                            "severity": "High"
                        })
                    # Skill Mismatch
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
                # Maintenance Check
                if drone['status'] == 'Maintenance':
                    conflicts.append({
                        "type": "Drone in Maintenance",
                        "entity": drone['drone_id'],
                        "detail": f"Assigned to {drone['current_assignment']} but status is Maintenance",
                        "severity": "High"
                    })
                # Location Mismatch
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

    # INDENTATION FIXED HERE: This function is now part of the Class, not inside check_conflicts
    def recommend_replacement(self, project_id):
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
        return best_matches[['name', 'pilot_id', 'skills', 'location']]

    def update_pilot_status(self, pilot_id, new_status, worksheet):
        try:
            cell = worksheet.find(pilot_id)
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            return True
        except Exception as e:
            return str(e)

# --- HELPER FUNCTIONS ---
def parse_intent(user_input):
    user_input = user_input.lower()
    intents = {
        "find_pilot": ["find pilot", "recommend", "who can fly", "replacement", "assign"],
        "update_status": ["update", "set status", "mark as", "change"],
        "check_status": ["status of", "where is", "is available", "check"],
        "show_roster": ["show pilots", "list pilots", "who is free", "roster"]
    }
    
    best_match = process.extractOne(user_input, intents.keys())
    
    detected_intent = None
    if best_match[1] > 60: 
        detected_intent = best_match[0]
    else:
        for intent, keywords in intents.items():
            if any(k in user_input for k in keywords):
                detected_intent = intent
                break
    
    words = user_input.replace("?", "").replace(".", "").split()
    entity_id = None
    for w in words:
        if (w.startswith("p0") or w.startswith("d0") or w.startswith("prj")) and any(c.isdigit() for c in w):
            entity_id = w.upper()
            break
            
    return detected_intent, entity_id, user_input

# --- MAIN APP ---
def main():
    st.title("🔒 Skylark Private Ops Agent")
    st.caption("Privacy Mode: On. Data processed locally.")
    
    st.sidebar.header("System Status")
    client = init_connection()
    
    if client:
        st.sidebar.success("Database Connected")
        if st.sidebar.button("🔄 Sync Data"):
            st.cache_data.clear()
            st.rerun()
    
    pilots_df, drones_df, missions_df, pilots_ws, drones_ws = load_data(client)
    
    if pilots_df is not None:
        agent = OpsAgent(pilots_df, drones_df, missions_df)
        
        tab1, tab2, tab3 = st.tabs(["💬 Ops Console", "📊 Roster & Fleet", "⚠️ Conflicts"])
        
        with tab1:
            st.info("System Ready. I can help with Roster, Assignments, and Status updates.")
            
            if "messages" not in st.session_state:
                st.session_state.messages = []

            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

            if prompt := st.chat_input("Ex: 'Find pilot for PRJ001' or 'Set P001 to On Leave'"):
                st.session_state.messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.markdown(prompt)

                intent, entity_id, clean_text = parse_intent(prompt)
                response = "I didn't quite catch that. Try commands like 'Find pilot for PRJ001' or 'Update P001'."

                if intent == "find_pilot":
                    if entity_id:
                        recs = agent.recommend_replacement(entity_id)
                        if not recs.empty:
                            response = f"**Top recommendations for {entity_id}:**\n\n" + recs.to_markdown(index=False)
                        else:
                            response = f"❌ Could not find active project **{entity_id}** or no local pilots are available."
                    else:
                        response = "Which project? Please mention the Project ID (e.g., PRJ001)."

                elif intent == "update_status":
                    if entity_id:
                        new_status = "Available"
                        if "leave" in clean_text: new_status = "On Leave"
                        if "mainten" in clean_text: new_status = "Maintenance"
                        if "assign" in clean_text: new_status = "Assigned"
                        
                        if entity_id.startswith("P"):
                            res = agent.update_pilot_status(entity_id, new_status, pilots_ws)
                            target = "Pilot"
                        else:
                            try:
                                cell = drones_ws.find(entity_id)
                                drones_ws.update_cell(cell.row, 4, new_status)
                                res = True
                                target = "Drone"
                            except Exception as e:
                                res = str(e)

                        if res is True:
                            response = f"✅ Updated {target} {entity_id} to '{new_status}'."
                            st.cache_data.clear()
                        else:
                            response = f"❌ Update failed: {res}"
                    else:
                        response = "I need an ID to update. (e.g., 'Update P001')"

                elif intent == "show_roster" or "available" in clean_text:
                    avail = pilots_df[pilots_df['status'] == 'Available']
                    response = f"**Available Pilots:**\n{avail[['name', 'location', 'skills']].to_markdown(index=False)}"

                with st.chat_message("assistant"):
                    st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})

        with tab2:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Pilots")
                st.dataframe(pilots_df)
            with col2:
                st.subheader("Drones")
                st.dataframe(drones_df)

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
                st.write("💡 *Tip: Go to the AI Assistant tab to find replacements for these conflicts.*")
            else:
                st.success("No active conflicts detected.")

if __name__ == "__main__":
    main()
