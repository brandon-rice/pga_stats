
# To run this from desktop: 
# - Open terminal 
# - activate your environment: c:\Users\\brand\\nba_env\Scripts\\activate
#- navigate back to git repo:  C:\users\\brand\OneDrive\Documents\git\pga_stats> streamlit run app.py


import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
from query_engine import run_nl_query, generate_sql
 
load_dotenv()
 
# ─── Page Config ─────────────────────────────────────────────────────────────
 
st.set_page_config(
    page_title="PGA Golf Analytics | AI Query",
    page_icon="⛳",
    layout="wide",
    initial_sidebar_state="expanded"
)
 
# ─── Custom CSS ──────────────────────────────────────────────────────────────
 
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap');
 
    :root {
        --green-dark: #1a3a2a;
        --green-mid: #2d5a3d;
        --green-light: #4a8c5c;
        --gold: #c9a84c;
        --gold-light: #e8c97a;
        --cream: #f5f0e8;
        --white: #fafaf8;
        --text-dark: #1a1a1a;
        --text-muted: #6b7280;
    }
 
    .stApp {
        background-color: var(--white);
        font-family: 'DM Sans', sans-serif;
    }
 
    /* Header */
    .main-header {
        background: linear-gradient(135deg, var(--green-dark) 0%, var(--green-mid) 60%, var(--green-light) 100%);
        padding: 2.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        position: relative;
        overflow: hidden;
    }
    .main-header::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -10%;
        width: 300px;
        height: 300px;
        background: radial-gradient(circle, rgba(201,168,76,0.15) 0%, transparent 70%);
        border-radius: 50%;
    }
    .main-header h1 {
        font-family: 'DM Serif Display', serif;
        color: var(--cream);
        font-size: 2.2rem;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .main-header .subtitle {
        color: var(--gold-light);
        font-size: 0.95rem;
        margin-top: 0.4rem;
        font-weight: 300;
    }
    .main-header .flag {
        font-size: 1.1rem;
        color: var(--gold);
        font-family: 'DM Mono', monospace;
        font-size: 0.8rem;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin-bottom: 0.5rem;
    }
 
    /* Query Input Area */
    .query-container {
        background: var(--white);
        border: 1.5px solid #e5e0d8;
        border-radius: 10px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
 
    /* Result Cards */
    .result-card {
        background: var(--cream);
        border-left: 4px solid var(--gold);
        border-radius: 0 8px 8px 0;
        padding: 1.2rem 1.5rem;
        margin-bottom: 1rem;
    }
    .result-card h4 {
        font-family: 'DM Serif Display', serif;
        color: var(--green-dark);
        margin: 0 0 0.5rem 0;
        font-size: 1rem;
    }
    .result-card p {
        color: var(--text-dark);
        margin: 0;
        line-height: 1.6;
        font-size: 0.95rem;
    }
 
    /* SQL Block */
    .sql-block {
        background: var(--green-dark);
        color: #a8d5b5;
        border-radius: 8px;
        padding: 1rem 1.2rem;
        font-family: 'DM Mono', monospace;
        font-size: 0.82rem;
        line-height: 1.6;
        white-space: pre-wrap;
        margin-top: 0.5rem;
    }
 
    /* Example queries */
    .example-chip {
        display: inline-block;
        background: var(--green-dark);
        color: var(--gold-light);
        border-radius: 20px;
        padding: 0.3rem 0.85rem;
        font-size: 0.8rem;
        margin: 0.25rem;
        cursor: pointer;
        font-family: 'DM Sans', sans-serif;
    }
 
    /* Stats bar */
    .stat-badge {
        background: var(--green-mid);
        color: var(--cream);
        border-radius: 6px;
        padding: 0.5rem 1rem;
        text-align: center;
        font-size: 0.85rem;
    }
    .stat-badge .num {
        font-family: 'DM Serif Display', serif;
        font-size: 1.4rem;
        color: var(--gold-light);
        display: block;
    }
 
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: var(--green-dark);
    }
    [data-testid="stSidebar"] * {
        color: var(--cream) !important;
    }
    [data-testid="stSidebar"] .stTextInput input {
        background: rgba(255,255,255,0.1);
        border-color: rgba(255,255,255,0.2);
        color: white !important;
    }
 
    /* Dataframe */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }
 
    /* Button */
    .stButton > button {
        background: var(--green-mid);
        color: var(--cream);
        border: none;
        border-radius: 8px;
        font-family: 'DM Sans', sans-serif;
        font-weight: 500;
        padding: 0.6rem 2rem;
        transition: all 0.2s;
    }
    .stButton > button:hover {
        background: var(--green-light);
        color: white;
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(45,90,61,0.3);
    }
 
    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
</style>
""", unsafe_allow_html=True)
 
 
# ─── DB Config (loaded from .env, never displayed) ───────────────────────────
 
db_schema = os.getenv("DB_SCHEMA", "pga_stats")
 
# Test connection for status indicator
def check_db_connection():
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        conn.close()
        return True
    except:
        return False
 
with st.sidebar:
    st.markdown("### ⛳ PGA Analytics")
    st.markdown("---")
 
    # Connection status
    st.markdown("**Database**")
    if check_db_connection():
        st.success(f"Connected to `{os.getenv('DB_NAME')}`")
    else:
        st.error("Connection failed — check your .env file")
 
    st.markdown("---")
    st.markdown("### 📊 Tables Available")
    st.markdown(f"""
    - `{db_schema}.combined_data`  
    - `{db_schema}.sg_data`  
    - `{db_schema}.leaderboard_data`
    """)
 
    st.markdown("---")
    show_sql = st.toggle("Show generated SQL", value=True)
    st.markdown("---")
    st.markdown("<small style='color: #4a8c5c;'>PGA Analytics Portfolio Project<br>Built with Claude + PostgreSQL</small>", unsafe_allow_html=True)
 
 
# ─── Header ──────────────────────────────────────────────────────────────────
 
st.markdown("""
<div class="main-header">
    <div class="flag">⛳ PGA Tour Analytics</div>
    <h1>Golf Intelligence Interface</h1>
    <div class="subtitle">Ask any question about PGA Tour performance data in plain English</div>
</div>
""", unsafe_allow_html=True)
 
 
# ─── Example Questions ────────────────────────────────────────────────────────
 
EXAMPLE_QUESTIONS = [
    "Who are the top 10 golfers by composite score?",
    "Which players have the best strokes gained tee-to-green in the last 10 rounds?",
    "Who has made the most money in major tournaments?",
    "Show players with a cut rate above 75% last year",
    "Who has the most top-10 finishes in the last 5 events?",
    "Which golfers improved the most between their last 10 and last 5 SG averages?",
    "Who finished in the top 5 most often last year?",
    "Show me the best putting stats from signature events",
]
 
st.markdown("**Try an example:**")
cols = st.columns(4)
for i, example in enumerate(EXAMPLE_QUESTIONS):
    with cols[i % 4]:
        if st.button(example, key=f"ex_{i}", use_container_width=True):
            st.session_state["prefill_question"] = example
 
 
# ─── Query Input ─────────────────────────────────────────────────────────────
 
prefill = st.session_state.get("prefill_question", "")
 
with st.container():
    question = st.text_area(
        "Ask a question about PGA Tour data",
        value=prefill,
        height=80,
        placeholder="e.g. Who are the top 5 golfers by strokes gained over the last 20 rounds?",
        label_visibility="collapsed"
    )
 
    col1, col2 = st.columns([1, 5])
    with col1:
        run_btn = st.button("⛳ Run Query", use_container_width=True)
    with col2:
        if question:
            st.caption(f"*{len(question.split())} words*")
 
 
# ─── Clear prefill after use ─────────────────────────────────────────────────
 
if "prefill_question" in st.session_state and run_btn:
    del st.session_state["prefill_question"]
 
 
# ─── Golf Ball Animation Placeholder (sits right below the text box) ──────────
 
animation_slot = st.empty()
 
 
# ─── Run Query ────────────────────────────────────────────────────────────────
 
if run_btn and question.strip():
 
    if not os.getenv("DB_NAME") or not os.getenv("DB_USER"):
        st.warning("⚠️ Database credentials missing — check your .env file.")
    else:
        # Show golf ball animation in the slot directly below the text box
        animation_slot.markdown("""
        <div style="
            background: linear-gradient(135deg, #1a3a2a, #2d5a3d);
            border-radius: 12px;
            padding: 1.2rem 1.5rem;
            margin: 0.5rem 0 1rem 0;
            overflow: hidden;
            position: relative;
        ">
            <div style="color: #e8c97a; font-size: 0.78rem; letter-spacing: 2px; text-transform: uppercase; margin-bottom: 0.6rem; font-family: 'DM Mono', monospace;">
                ⛳ Analyzing your question...
            </div>
            <!-- Fairway track -->
            <div style="
                background: rgba(255,255,255,0.08);
                border-radius: 20px;
                height: 28px;
                width: 100%;
                position: relative;
                overflow: hidden;
                border: 1px solid rgba(255,255,255,0.1);
            ">
                <!-- Grass texture lines -->
                <div style="
                    position: absolute; top: 0; left: 0; right: 0; bottom: 0;
                    background: repeating-linear-gradient(
                        90deg,
                        transparent,
                        transparent 40px,
                        rgba(255,255,255,0.03) 40px,
                        rgba(255,255,255,0.03) 41px
                    );
                "></div>
                <!-- Rolling golf ball -->
                <div style="
                    position: absolute;
                    top: 50%;
                    transform: translateY(-50%);
                    width: 22px;
                    height: 22px;
                    background: radial-gradient(circle at 35% 35%, #ffffff, #d0d0d0);
                    border-radius: 50%;
                    box-shadow: 2px 2px 4px rgba(0,0,0,0.4), inset -2px -2px 4px rgba(0,0,0,0.1);
                    animation: rollBall 2.2s cubic-bezier(0.45, 0, 0.55, 1) infinite;
                ">
                    <!-- Dimple pattern -->
                    <div style="
                        position: absolute; top: 4px; left: 5px;
                        width: 4px; height: 4px;
                        background: rgba(0,0,0,0.12);
                        border-radius: 50%;
                    "></div>
                    <div style="
                        position: absolute; top: 8px; left: 11px;
                        width: 3px; height: 3px;
                        background: rgba(0,0,0,0.1);
                        border-radius: 50%;
                    "></div>
                    <div style="
                        position: absolute; top: 13px; left: 6px;
                        width: 3px; height: 3px;
                        background: rgba(0,0,0,0.1);
                        border-radius: 50%;
                    "></div>
                </div>
                <!-- Flag at the end -->
                <div style="
                    position: absolute;
                    right: 12px;
                    top: 50%;
                    transform: translateY(-50%);
                    font-size: 16px;
                    line-height: 1;
                ">⛳</div>
            </div>
            <style>
                @keyframes rollBall {
                    0%   { left: 2px;   }
                    80%  { left: calc(100% - 42px); }
                    100% { left: calc(100% - 42px); }
                }
            </style>
        </div>
        """, unsafe_allow_html=True)
 
        result = run_nl_query(question.strip())
 
        # Clear the animation once done
        animation_slot.empty()
 
        # ── Error State ──
        if result["error"]:
            st.error(f"**Error:** {result['error']}")
 
        else:
            # ── AI Explanation ──
            st.markdown(f"""
            <div class="result-card">
                <h4>📋 Analysis</h4>
                <p>{result['explanation']}</p>
            </div>
            """, unsafe_allow_html=True)
 
            # ── SQL Block ──
            if show_sql and result["sql"]:
                with st.expander("🔍 View Generated SQL", expanded=False):
                    st.markdown(f'<div class="sql-block">{result["sql"]}</div>', unsafe_allow_html=True)
 
            # ── Results Table ──
            if result["rows"]:
                st.markdown(f"**Results** — {len(result['rows'])} rows returned")
                df = pd.DataFrame(result["rows"])
 
                # Round float columns to 2 decimal places for display
                float_cols = df.select_dtypes(include=['float64', 'float32']).columns
                df[float_cols] = df[float_cols].round(2)
 
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    height=min(400, 50 + len(df) * 35)
                )
 
                # Download button
                csv = df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download CSV",
                    data=csv,
                    file_name="pga_query_results.csv",
                    mime="text/csv"
                )
            else:
                st.info("Query ran successfully but returned no results.")
 
elif run_btn and not question.strip():
    st.warning("Please enter a question first.")
 
 
# ─── Query History ────────────────────────────────────────────────────────────
 
if "query_history" not in st.session_state:
    st.session_state.query_history = []
 
if run_btn and question.strip() and "result" in dir() and not result.get("error"):
    st.session_state.query_history.insert(0, {
        "question": question,
        "rows": len(result.get("rows", []))
    })
    st.session_state.query_history = st.session_state.query_history[:10]
 
if st.session_state.query_history:
    st.markdown("---")
    with st.expander("🕓 Recent Queries", expanded=False):
        for item in st.session_state.query_history:
            st.markdown(f"- **{item['question']}** — *{item['rows']} rows*")