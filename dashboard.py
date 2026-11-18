import streamlit as st
import psycopg2
import pandas as pd
import time
import plotly.express as px

# ==========================================
# 1. PAGE & CSS CONFIGURATION
# ==========================================
st.set_page_config(page_title="SecureOps Dashboard", page_icon="🛡️", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    .stApp { background-color: #0E1117; }
    div[data-testid="stMetric"] {
        background-color: #1E1E1E;
        padding: 15px 20px;
        border-radius: 8px;
        border: 1px solid #2D2D2D;
        box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.3);
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. DATABASE CONNECTION
# ==========================================
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host="localhost",
        database="iot_security",
        user="sec_admin",
        password="admin123",
        port="5432"
    )

conn = init_connection()

def fetch_data(query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.description:
                cols = [desc[0] for desc in cur.description]
                return pd.DataFrame(cur.fetchall(), columns=cols)
            return pd.DataFrame()
    except Exception as e:
        st.error(f"DB Error: {e}")
        return pd.DataFrame()

# ==========================================
# 3. STATE MANAGEMENT (MULTI-PAGE LOGIC)
# ==========================================
# Initialize the session state to default to the dashboard view
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'dashboard'

# ==========================================
# 4. SIDEBAR (Navigation)
# ==========================================
with st.sidebar:
    st.title("🛡️ SecureOps")
    st.markdown("---")
    
    # Dashboard Button
    if st.button("📊 Dashboard", use_container_width=True, type="primary" if st.session_state.current_page == 'dashboard' else "secondary"):
        st.session_state.current_page = 'dashboard'
        st.rerun()
        
    # Alerts Button
    if st.button("⚠️ Alerts", use_container_width=True, type="primary" if st.session_state.current_page == 'alerts' else "secondary"):
        st.session_state.current_page = 'alerts'
        st.rerun()
        
    st.markdown("---")
    auto_refresh = st.checkbox("Auto-Refresh (5s)", value=True)

# ==========================================
# 5. HEADER BAR (Always Visible)
# ==========================================
header_col1, header_col2, header_col3 = st.columns([2, 2, 1])
with header_col1:
    st.markdown("🟢 **System Health:** OPERATIONAL")
with header_col2:
    st.markdown("🔴 **Threat Level:** ELEVATED")
with header_col3:
    st.markdown("👤 **Admin User**")
st.markdown("<br>", unsafe_allow_html=True)


# ==========================================
# 6. VIEW: DASHBOARD
# ==========================================
if st.session_state.current_page == 'dashboard':
    
    # --- Live Metric Calculations ---
    total_threats = fetch_data("SELECT COUNT(*) as c FROM threat_alerts WHERE alert_type != 'HEARTBEAT';").iloc[0]['c']
    active_alerts = fetch_data("SELECT COUNT(*) as c FROM threat_alerts WHERE alert_type != 'HEARTBEAT' AND time >= NOW() - INTERVAL '10 minutes';").iloc[0]['c']
    systems_protected = fetch_data("SELECT COUNT(DISTINCT device_id) as c FROM threat_alerts WHERE alert_type = 'HEARTBEAT';").iloc[0]['c']
    
    # Calculate REAL average latency over the last 10 minutes
    lat_df = fetch_data("SELECT AVG(latency_ms) as avg_lat FROM threat_alerts WHERE alert_type != 'HEARTBEAT' AND time >= NOW() - INTERVAL '10 minutes';")
    real_latency = round(lat_df.iloc[0]['avg_lat'], 1) if not lat_df.empty and pd.notna(lat_df.iloc[0]['avg_lat']) else 0

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric(label="TOTAL THREATS", value=total_threats, delta="All Time")
    with m2:
        st.metric(label="ACTIVE ALERTS", value=active_alerts, delta="Last 10 Min")
    with m3:
        # Now showing actual real-time latency in milliseconds
        st.metric(label="AVG RESPONSE TIME", value=f"{real_latency} ms", delta="Live Flink Speed", delta_color="inverse")
    with m4:
        st.metric(label="SYSTEMS PROTECTED", value=systems_protected, delta="Online")

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Charts Row ---
    chart1, chart2 = st.columns([2, 1])

    with chart1:
        st.markdown("##### Alert Volume Trend")
        trend_df = fetch_data("""
            SELECT date_trunc('minute', time) as period, COUNT(*) as volume 
            FROM threat_alerts 
            WHERE alert_type != 'HEARTBEAT' 
            GROUP BY period ORDER BY period DESC LIMIT 30;
        """)
        if not trend_df.empty:
            fig_line = px.line(trend_df, x="period", y="volume", template="plotly_dark")
            fig_line.update_traces(line_color="#00F0FF", mode="lines+markers")
            fig_line.update_layout(height=250, margin=dict(l=0, r=0, t=10, b=0), xaxis_title=None, yaxis_title=None, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_line, use_container_width=True)

    with chart2:
        st.markdown("##### Severity Distribution")
        sev_df = fetch_data("SELECT severity, COUNT(*) as c FROM threat_alerts WHERE alert_type != 'HEARTBEAT' GROUP BY severity;")
        if not sev_df.empty:
            color_map = {"HIGH": "#FF4B4B", "MEDIUM": "#FFA500", "LOW": "#00FF00"}
            fig_donut = px.pie(sev_df, values="c", names="severity", hole=0.6, template="plotly_dark", color="severity", color_discrete_map=color_map)
            fig_donut.update_layout(height=250, margin=dict(l=0, r=0, t=10, b=0), showlegend=False, paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_donut, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Recent Threats Preview ---
    st.markdown("##### Recent Threat Activity")
    recent_df = fetch_data("""
        SELECT TO_CHAR(time, 'HH24:MI:SS') as "Time", alert_type as "Threat Type", device_id as "Target Device", severity as "Severity"
        FROM threat_alerts WHERE alert_type != 'HEARTBEAT' ORDER BY time DESC LIMIT 5;
    """)
    if not recent_df.empty:
        st.dataframe(recent_df, use_container_width=True, hide_index=True)


# ==========================================
# 7. VIEW: ALERTS PANEL
# ==========================================
elif st.session_state.current_page == 'alerts':
    st.markdown("### ⚠️ Comprehensive Threat Log")
    st.markdown("Displaying all detected network anomalies and attacks.")
    
    # Fetch a much larger dataset for the dedicated alerts page
    all_alerts_df = fetch_data("""
        SELECT 
            time as "Timestamp", 
            device_id as "Target Device", 
            alert_type as "Attack Signature", 
            severity as "Severity",
            evidence as "Evidence/Payload",
            latency_ms as "Detection Latency (ms)"
        FROM threat_alerts 
        WHERE alert_type != 'HEARTBEAT' 
        ORDER BY time DESC 
        LIMIT 1000;
    """)
    
    if not all_alerts_df.empty:
        # Render a large, interactive dataframe that the user can scroll and sort
        st.dataframe(all_alerts_df, use_container_width=True, hide_index=True, height=600)
    else:
        st.info("No threat data available in the database.")

# ==========================================
# 8. AUTO-REFRESH
# ==========================================
if auto_refresh:
    time.sleep(1)
    st.rerun()