import os
import pandas as pd
import time
from google.cloud import bigquery
import google.generativeai as genai
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound, TooManyRequests

def decode_swap_log(log_data: str) -> tuple[int, int]:
    """decodes uniswap v3 swap log data."""
    if log_data.startswith('0x'):
        log_data = log_data[2:]
    
    # amount0 (signed 256-bit integer)
    amount0_hex = log_data[0:64]
    # amount1 (signed 256-bit integer)
    amount1_hex = log_data[64:128]

    # convert hex to signed int (two's complement)
    amount0 = int.from_bytes(bytes.fromhex(amount0_hex), byteorder='big', signed=True)
    amount1 = int.from_bytes(bytes.fromhex(amount1_hex), byteorder='big', signed=True)

    return amount0, amount1

def get_crypto_data():
    """extracts, decodes, analyzes, and narrates uniswap v3 swaps."""
    # load env vars from .env file for local development
    load_dotenv()
    
    project_id = os.getenv("GCP_PROJECT")
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        # this check helps local setup
        raise Exception("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
    
    try:
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY not found.")
        genai.configure(api_key=api_key) # type: ignore
        model = genai.GenerativeModel('gemini-2.5-flash') # type: ignore
    except Exception as e:
        # fallback for streamlit cloud secrets
        try:
            import streamlit as st
            genai.configure(api_key=st.secrets["GOOGLE_API_KEY"]) # type: ignore
            model = genai.GenerativeModel('gemini-2.5-flash') # type: ignore
        except Exception as st_e:
            print(f"failed to configure gemini. error: {st_e}")
            raise

    bq_client = bigquery.Client(project=project_id)

    sql = """
    SELECT
        tx.from_address AS sender,
        logs.address AS pool_address,
        logs.data,
        tx.value,
        logs.block_timestamp
    FROM
        `bigquery-public-data.crypto_ethereum.logs` AS logs
    JOIN
        `bigquery-public-data.crypto_ethereum.transactions` AS tx
        ON tx.hash = logs.transaction_hash AND DATE(tx.block_timestamp) = DATE(logs.block_timestamp)
    WHERE
        ARRAY_LENGTH(logs.topics) > 0 
        AND logs.topics[OFFSET(0)] = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        AND DATE(logs.block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND DATE(tx.block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    ORDER BY
        logs.block_timestamp DESC
    LIMIT 100
    """

    try:
        query_job = bq_client.query(sql)
        df = query_job.to_dataframe()
    except Exception as e:
        print(f"bigquery query failed: {e}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df['value_eth'] = pd.to_numeric(df['value'], errors='coerce').astype('float64') / 1e18
    df[['amount0', 'amount1']] = df['data'].apply(lambda x: pd.Series(decode_swap_log(x)))
    
    # bot detection
    df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])
    df = df.sort_values(by=['sender', 'block_timestamp'])
    df['time_diff'] = df.groupby('sender')['block_timestamp'].diff()
    threshold = pd.Timedelta(minutes=5)
    bot_senders = df[df['time_diff'] <= threshold]['sender'].unique()
    df['is_bot'] = df['sender'].isin(bot_senders)
    
    # top 20 for feed
    df = df.sort_values(by='block_timestamp', ascending=False).head(20)

    ai_narratives = []
    # analyze top 10 rows with rate limiting
    for index, row in df.head(10).iterrows():
        amount0_decoded = row['amount0']
        amount1_decoded = row['amount1']

        if amount0_decoded < 0:
            action = f"user bought {-amount0_decoded:,} of token0 and sold {amount1_decoded:,} of token1"
        else:
            action = f"user sold {amount0_decoded:,} of token0 and bought {-amount1_decoded:,} of token1"
        bot_note = "this wallet is flagged as a bot due to high-frequency trading." if row['is_bot'] else ""
        
        prompt = f"""you are a financial analyst. summarize this trade in 1 sentence.
        focus on the decoded action: "{action}". 
        instruction: large numbers (billions+) usually imply meme coins. small numbers (0-100) often imply major assets like eth/wbtc.
        {bot_note}
        end with exactly one tag: [bullish], [bearish], or [neutral].
        """
        
        try:
            response = model.generate_content(prompt)
            ai_narratives.append(response.text.strip())
        except (NotFound, TooManyRequests):
            ai_narratives.append("analysis unavailable (api limit). [neutral]")
        except Exception as e:
            print(f"ai generation failed: {e}")
            ai_narratives.append("analysis failed. [neutral]")

    # skip remaining items
    for _ in range(len(df) - 10):
        ai_narratives.append("analysis skipped to save api credits. [neutral]")

    df['AI_Narrative'] = ai_narratives
    return df

if __name__ == '__main__':
    print("running etl pipeline for testing...")
    df_result = get_crypto_data()
    print("etl process complete. result:")
    print(df_result[['sender', 'is_bot', 'amount0', 'amount1', 'AI_Narrative']].head(10))