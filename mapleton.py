import os
import json
import re
from google.oauth2.service_account import Credentials
import gspread
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, Column, Integer, Text
import pandas as pd
import numpy as np
import subprocess
from sqlalchemy import create_engine, text

# Load variables from .env into environment
load_dotenv()

def container_exists(name):
    result = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        capture_output=True, text=True
    )
    return name in result.stdout.splitlines()

def clean_header(header):
    if not header:  # skip None or empty
        return None
    header = header.replace(' ', '_')  # replace spaces
    header = re.sub(r'[^0-9a-zA-Z_]', '', header)  # remove invalid chars
    return header


# === CONFIG ===
# Path to your Google service account key
pg_user = os.getenv("POSTGRES_USER")
pg_pw = os.getenv("POSTGRES_PASSWORD")
pg_db = os.getenv("POSTGRES_DB")
pg_host = os.getenv("POSTGRES_HOST")
pg_port = os.getenv("POSTGRES_PORT")
pg_table_name = os.getenv("TABLE_NAME")
service_account_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])


google_sheet_id = os.getenv("SPREADSHEET_ID")
google_sheet_name = os.getenv("SHEET_NAME")


# === STEP 1: Load Google Sheet ===
scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(google_sheet_id).worksheet(google_sheet_name)

# === STEP 1: Clean headers and data (your existing code) ===
all_values = sheet.get_all_values()
headers = all_values[1]
data_rows = all_values[2:]
df = pd.DataFrame(data_rows, columns=headers)

# Apply cleaning to headers
df.columns = [clean_header(h) for h in df.columns]
df = df.loc[:, df.columns.notna() & (df.columns != '')]
df.columns = [c.lower() for c in df.columns]

# Replace empty strings with NaN
df.replace('', np.nan, inplace=True)
df = df.where(pd.notnull(df), None)

# === STEP 2: Start Postgres Docker container ===
# only do this is creating a local db and not using supabase
# if not container_exists(POSTGRES_CONTAINER_NAME):
#     subprocess.run([
#         "docker", "run", "-d",
#         "--name", POSTGRES_CONTAINER_NAME,
#         "-e", f"POSTGRES_USER={POSTGRES_USER}",
#         "-e", f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
#         "-e", f"POSTGRES_DB={POSTGRES_DB}",
#         "-e", f"TZ=America/Denver",
#         "-p", f"{POSTGRES_PORT}:5432",
#         "postgres"
#     ], check=True)
# else:
#     print(f"Container '{POSTGRES_CONTAINER_NAME}' already exists. Skipping docker run.")

# === STEP 3: Wait for Postgres to start ===
import time
time.sleep(5)

# === STEP 4: Connect to Postgres ===
engine = create_engine(f"postgresql://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}")

# === STEP 5: Create table with auto-increment primary key ===
metadata = MetaData()

# First column: id primary key
columns = [Column('id', Integer, primary_key=True, autoincrement=True)]

# Add columns based on cleaned DataFrame headers, use Text for simplicity
for col in df.columns:
    columns.append(Column(col, Text))

table = Table(pg_table_name, metadata, *columns)
metadata.drop_all(engine, [table])  # Drop if exists
metadata.create_all(engine)          # Create table

# === STEP 6: Insert data into the table ===
df.to_sql(pg_table_name, engine, if_exists='append', index=False)

print(f"Successfully created table '{pg_table_name}' with primary key 'id' and inserted {len(df)} rows.")

# --- Call the normalize function ---
with engine.begin() as conn:
    conn.execute(text("SELECT normalize_data();"))

print("normalize_data() function executed successfully.")
