import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time

# --- CONFIGURATION ---
st.set_page_config(page_title="Skylark Ops Agent", layout="wide")

# --- AUTHENTICATION & CONNECTION ---
# We use st.cache_resource to keep the connection open
@st.cache_resource
def init_connection():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # FOR LOCAL TESTING: Use credentials.json
    # FOR DEPLOYMENT: Use st.secrets (Recommended)
    try:
        # Checking if st.secrets exists (Deployment mode)
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    except:
        # Fallback to local file (Local mode)
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    
    client = gspread.authorize(creds)
    return client

def load_data(client):
    try:
        sh = client.open("Drone_Operations_DB")
        # specific worksheets
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
                # Get Mission Details
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
                
                # D. Location Mismatch (Drone vs Project)
                # Find the project the drone is assigned to
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
        # Urgent Reassignment Logic
        mission = self.missions[self.missions['project_id'] == project_id].iloc[0]
        req_skills = [x.strip() for x in mission['required_skills'].split(',')]
        location = mission['location']
        
        # Filter: Available + Matches Location + Has Skills
        candidates = self.pilots[
            (self.pilots['status'] == 'Available') & 
            (self.pilots['location'] == location)
        ].copy()
        
        candidates['score'] = candidates['skills'].apply(
            lambda x: sum(1 for s in req_skills if s in x)
        )
        
        # Sort by best skill match
        best_matches = candidates.sort_values(by='score', ascending=False)
        return best_matches[['name', 'pilot_id', 'skills', 'location']]

    def update_pilot_status(self, pilot_id, new_status, worksheet):
        try:
            # Find the row number (1-based index, +1 for header)
            cell = worksheet.find(pilot_id)
            # Assuming 'status' is in column 6 (F) based on sample csv
            # Better way: find column index by header
            headers = worksheet.row_values(1)
            col_idx = headers.index('status') + 1
            worksheet.update_cell(cell.row, col_idx, new_status)
            return True
        except Exception as e:
            return str(e)

# --- UI LAYOUT ---
def main():
    st.title("🚁 Skylark AI Operations Agent")
    
    # Sidebar
    st.sidebar.header("System Status")
    client = init_connection()
    
    if client:
        st.sidebar.success("Database Connected")
        if st.sidebar.button("🔄 Sync Data"):
            st.cache_data.clear()
            st.rerun()
    
    # Load Data
    pilots_df, drones_df, missions_df, pilots_ws, _ = load_data(client)
    
    if pilots_df is not None:
        agent = OpsAgent(pilots_df, drones_df, missions_df)
        
        # Tabs for different Views
        tab1, tab2, tab3 = st.tabs(["💬 AI Assistant", "📊 Roster & Fleet", "⚠️ Conflicts"])
        
        with tab1:
            st.subheader("Operations Chat")
            st.info("Ask me to 'Find a pilot for Project A' or 'Update Pilot X to On Leave'")
            
            # Chat History Setup
            if "messages" not in st.session_state:
                st.session_state.messages = []

            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

            # Chat Input Handler
            if prompt := st.chat_input("How can I help you coordinate?"):
                st.session_state.messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.markdown(prompt)

                # --- SIMPLE NLU PARSER (Replacing complex LLM for prototype speed) ---
                response = "I'm not sure how to handle that yet."
                
                # Intent: Find Replacement / Assignment
                if "find" in prompt.lower() and "project" in prompt.lower():
                    # Extract Project ID (Simple heuristic)
                    words = prompt.split()
                    proj_id = next((w for w in words if w.startswith("PRJ")), None)
                    if proj_id:
                        recs = agent.recommend_replacement(proj_id)
                        if not recs.empty:
                            response = f"**Top recommendations for {proj_id}:**\n\n" + recs.to_markdown(index=False)
                        else:
                            response = f"No direct matches found for {proj_id} in the current location."
                    else:
                        response = "Please specify the Project ID (e.g., PRJ001)."

                # Intent: Update Status
                elif "status" in prompt.lower() or "leave" in prompt.lower():
                    # Heuristic: Look for P00X and Status keyword
                    words = prompt.split()
                    pilot_id = next((w for w in words if w.startswith("P00")), None)
                    if pilot_id and "leave" in prompt.lower():
                        res = agent.update_pilot_status(pilot_id, "On Leave", pilots_ws)
                        if res is True:
                            response = f"✅ Updated {pilot_id} status to 'On Leave'. Syncing to Google Sheets..."
                            st.cache_data.clear() # Force reload on next run
                        else:
                            response = f"❌ Failed to update: {res}"
                
                # Intent: General Status
                elif "available" in prompt.lower():
                    avail = pilots_df[pilots_df['status'] == 'Available']
                    response = f"Here are the currently available pilots:\n\n{avail[['name', 'location', 'skills']].to_markdown(index=False)}"

                else:
                    response = "I can help you:\n1. Find pilots for a project (`Find pilot for PRJ001`)\n2. Update status (`Set P001 to On Leave`)\n3. Check availability (`Who is available?`)"

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
