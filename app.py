import streamlit as st
import pandas as pd
import numpy as np
from etl_pipeline import get_crypto_data
from dotenv import load_dotenv
import os
from streamlit.errors import StreamlitSecretNotFoundError

st.set_page_config(
    page_title="DEX Whale Watcher",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed"
)

def load_secrets():
    """loads secrets from streamlit or .env file."""
    try:
        # check for api key in streamlit secrets
        if st.secrets.get('GOOGLE_API_KEY'):
            os.environ['GOOGLE_API_KEY'] = st.secrets['GOOGLE_API_KEY']
            os.environ['GCP_PROJECT'] = st.secrets['GCP_PROJECT']
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = st.secrets['GOOGLE_APPLICATION_CREDENTIALS']
        else:
            # fallback to .env if not in secrets
            load_dotenv()
    except StreamlitSecretNotFoundError:
        # fallback to .env if secrets.toml is missing
        load_dotenv()
    except Exception as e:
        print(f"warning: could not load secrets from streamlit ({e}). falling back to .env.")
        load_dotenv()

load_secrets()

st.markdown("""
<style>
    .stApp {
        background-color: #0d1117;
        color: #c9d1d9;
    }
    .stMetric {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 10px;
    }
    .stExpander {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

st.title("🤖 DEX Live Trade Feed")
st.caption("ai-powered analysis of live uniswap v3 swaps")

try:
    with st.spinner('fetching latest on-chain data from bigquery...'):
        df = get_crypto_data()

    if not df.empty:
        active_wallets = df['sender'].nunique()
        tx_count = len(df)
        active_bots = df[df['is_bot']]['sender'].nunique()

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(label="active wallets in feed", value=f"{active_wallets}")
        with col2:
            st.metric(label="transactions analyzed", value=f"{tx_count}")
        with col3:
            st.metric(label="active bots detected", value=f"{active_bots}")

        st.markdown("---")
        st.subheader("live transaction feed")

        for index, row in df.iterrows():
            badge = "[bot]" if row['is_bot'] else "[user]"
            trade_time = row['block_timestamp'].strftime('%H:%M:%S')
            header = f"{badge} trade by {row['sender']} at {trade_time}"
            
            with st.expander(header):
                narrative = row['AI_Narrative']
                
                if "[bullish]" in narrative.lower():
                    st.success(narrative)
                elif "[bearish]" in narrative.lower():
                    st.error(narrative)
                else:
                    st.info(narrative)

                st.write("transaction details:")
                st.json(row.to_json())

    else:
        st.warning("no significant uniswap v3 swap data found in the last 24 hours.")

except Exception as e:
    st.error(f"an application error occurred: {e}")
    st.error("please ensure your google_api_key and other credentials are correctly set in st.secrets or your .env file.")