import snowflake.connector
import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilename
import os
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

# Connectionparameter
account = os.getenv('SNOWFLAKE_ACCOUNT')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
username = os.getenv('SNOWFLAKE_USERNAME')
password = os.getenv('SNOWFLAKE_PASSWORD')

# Path to your CSV
Tk().withdraw()
csv_file_path = askopenfilename(
    title='Wähle CSV-Datei aus', filetypes=[('CSV-Dateien', '*.csv')])

# Tablename
table_name = input('Type in the name of your tabel: ')

# Create Connection
conn = snowflake.connector.connect(
    user=username,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# Read CSV
df = pd.read_csv(csv_file_path)

# Create Table in Snowflake
create_table_sql = f"CREATE TABLE {table_name} (" \
    f"{', '.join(f'{column} STRING' for column in df.columns)}" \
    ")"
cursor = conn.cursor()
cursor.execute(create_table_sql)

# Create temporary stage
stage_name = 'temp_stage'
create_stage_sql = f"CREATE TEMPORARY STAGE {stage_name}"
cursor.execute(create_stage_sql)

# Upload CSV to temporary stage
csv_file_name = csv_file_path.split('/')[-1]
csv_file_stage_path = f"@{stage_name}/{csv_file_name}"
put_file_sql = f"PUT file://{csv_file_path} @{stage_name}"
cursor.execute(put_file_sql)

# Copy CSV into table
copy_into_sql = f"COPY INTO {table_name} FROM {
    csv_file_stage_path} FILE_FORMAT = (TYPE=CSV,SKIP_HEADER=1,FIELD_DELIMITER=',',TRIM_SPACE=FALSE,FIELD_OPTIONALLY_ENCLOSED_BY=NONE,DATE_FORMAT=AUTO,TIME_FORMAT=AUTO,TIMESTAMP_FORMAT=AUTO)"
cursor.execute(copy_into_sql)

# Delete temporary stage
drop_stage_sql = f"DROP STAGE {stage_name}"
cursor.execute(drop_stage_sql)

# Close connection
conn.commit()
conn.close()
